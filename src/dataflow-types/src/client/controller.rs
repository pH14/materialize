// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A representative of STORAGE and COMPUTE that maintains summaries of the involved objects.
//!
//! The `Controller` provides the ability to create and manipulate storage and compute instances.
//! Each of Storage and Compute provide their own controllers, accessed through the `storage()`
//! and `compute(instance_id)` methods. It is an error to access a compute instance before it has
//! been created; a single storage instance is always available.
//!
//! The controller also provides a `recv()` method that returns responses from the storage and
//! compute layers, which may remain of value to the interested user. With time, these responses
//! may be thinned down in an effort to make the controller more self contained.
//!
//! Consult the `StorageController` and `ComputeController` documentation for more information
//! about each of these interfaces.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::stream::{BoxStream, StreamExt};
use maplit::hashmap;
use mz_persist_client::cache::PersistClientCache;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use timely::order::TotalOrder;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use tokio_stream::StreamMap;

use mz_orchestrator::{
    CpuLimit, MemoryLimit, NamespacedOrchestrator, Orchestrator, ServiceConfig, ServiceEvent,
    ServicePort,
};
use mz_persist_client::PersistLocation;
use mz_persist_types::Codec64;
use mz_proto::RustType;

use crate::client::{
    ComputeClient, ComputeCommand, ComputeInstanceId, ComputeResponse,
    ConcreteComputeInstanceReplicaConfig, ControllerResponse, ProcessId, ReplicaId, StorageCommand,
    StorageResponse,
};
use crate::client::{ComputedRemoteClient, GenericClient};
use crate::logging::LoggingConfig;
use crate::{TailBatch, TailResponse};

pub use mz_orchestrator::ServiceStatus as ComputeInstanceStatus;

pub use crate::client::controller::compute::{ComputeController, ComputeControllerMut};
pub use crate::client::controller::storage::{StorageController, StorageControllerState};

use super::ActiveReplicationResponse;

mod compute;
pub mod storage;

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The orchestrator implementation to use.
    pub orchestrator: Arc<dyn Orchestrator>,
    /// Whether or not storage and compute processes should die when connection
    /// with their controller is lost.
    pub linger: bool,
    /// The persist location where all storage collections will be written to.
    pub persist_location: PersistLocation,
    /// A process-global cache of (blob_uri, consensus_uri) ->
    /// PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
    /// The stash URL for the storage controller.
    pub storage_stash_url: String,
    /// The storaged image to use when starting new storage processes.
    pub storaged_image: String,
    /// The computed image to use when starting new compute processes.
    pub computed_image: String,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterReplicaSizeConfig {
    pub memory_limit: Option<MemoryLimit>,
    pub cpu_limit: Option<CpuLimit>,
    pub scale: NonZeroUsize,
    pub workers: NonZeroUsize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterReplicaSizeMap(pub HashMap<String, ClusterReplicaSizeConfig>);

impl Default for ClusterReplicaSizeMap {
    fn default() -> Self {
        // {
        //     "1": {"scale": 1, "workers": 1},
        //     "2": {"scale": 1, "workers": 2},
        //     "4": {"scale": 1, "workers": 4},
        //     /// ...
        //     "32": {"scale": 1, "workers": 32}
        //     /// Testing with multiple processes on a single machine is a novelty, so
        //     /// we don't bother providing many options.
        //     "2-1": {"scale": 2, "workers": 1},
        //     "2-2": {"scale": 2, "workers": 2},
        //     "2-4": {"scale": 2, "workers": 4},
        // }
        let mut inner = (0..=5)
            .map(|i| {
                let workers = 1 << i;
                (
                    workers.to_string(),
                    ClusterReplicaSizeConfig {
                        memory_limit: None,
                        cpu_limit: None,
                        scale: NonZeroUsize::new(1).unwrap(),
                        workers: NonZeroUsize::new(workers).unwrap(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        inner.insert(
            "2-1".to_string(),
            ClusterReplicaSizeConfig {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(1).unwrap(),
            },
        );
        inner.insert(
            "2-2".to_string(),
            ClusterReplicaSizeConfig {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(2).unwrap(),
            },
        );
        inner.insert(
            "2-4".to_string(),
            ClusterReplicaSizeConfig {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(4).unwrap(),
            },
        );
        Self(inner)
    }
}

/// Deterministically generates replica names based on inputs.
fn generate_replica_service_name(instance_id: ComputeInstanceId, replica_id: ReplicaId) -> String {
    format!("cluster-{instance_id}-replica-{replica_id}")
}

/// Parse a name generated by `generate_replica_service_name`, to extract the
/// replica's compute instance ID and replica ID values.
fn parse_replica_service_name(
    service_name: &str,
) -> Result<(ComputeInstanceId, ReplicaId), anyhow::Error> {
    static SERVICE_NAME_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?-u)^cluster-(\d+)-replica-(\d+)$").unwrap());

    let caps = SERVICE_NAME_RE
        .captures(service_name)
        .ok_or_else(|| anyhow!("invalid service name: {service_name}"))?;

    let instance_id = caps.get(1).unwrap().as_str().parse().unwrap();
    let replica_id = caps.get(2).unwrap().as_str().parse().unwrap();
    Ok((instance_id, replica_id))
}

/// An event describing a change in status of a compute process.
#[derive(Debug, Clone, Serialize)]
pub struct ComputeInstanceEvent {
    pub instance_id: ComputeInstanceId,
    pub replica_id: ReplicaId,
    pub process_id: ProcessId,
    pub status: ComputeInstanceStatus,
    pub time: DateTime<Utc>,
}

enum UnderlyingControllerResponse<T> {
    Storage(StorageResponse<T>),
    Compute(ComputeInstanceId, ActiveReplicationResponse<T>),
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
///
/// NOTE(benesch): I find the fact that this type is called `Controller` but is
/// referred to as the `dataflow_client` in the coordinator to be very
/// confusing. We should find the one correct name, and use it everywhere!
pub struct Controller<T = mz_repr::Timestamp> {
    linger: bool,
    storage_controller: Box<dyn StorageController<Timestamp = T>>,
    compute_orchestrator: Arc<dyn NamespacedOrchestrator>,
    computed_image: String,
    compute: BTreeMap<ComputeInstanceId, compute::ComputeControllerState<T>>,
    stashed_response: Option<UnderlyingControllerResponse<T>>,
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + Codec64 + Copy + Unpin,
    ComputeCommand<T>: RustType<super::ProtoComputeCommand>,
    ComputeResponse<T>: RustType<super::ProtoComputeResponse>,
{
    pub async fn create_instance(
        &mut self,
        instance: ComputeInstanceId,
        logging: Option<LoggingConfig>,
    ) -> Result<(), anyhow::Error> {
        // Insert a new compute instance controller.
        self.compute.insert(
            instance,
            compute::ComputeControllerState::new(&logging).await?,
        );

        Ok(())
    }

    /// Adds replicas of an instance.
    ///
    /// # Panics
    /// - If the identified `instance` has not yet been created via
    ///   [`Self::create_instance`].
    pub async fn add_replica_to_instance(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        assert!(
            self.compute.contains_key(&instance_id),
            "call Controller::create_instance before calling add_replica_to_instance"
        );

        // Add replicas backing that instance.
        match config {
            ConcreteComputeInstanceReplicaConfig::Remote { replicas } => {
                let mut compute_instance = self.compute_mut(instance_id).unwrap();
                let client = ComputedRemoteClient::new(&replicas.into_iter().collect::<Vec<_>>());
                let client: Box<dyn ComputeClient<T>> = Box::new(client);
                compute_instance.add_replica(replica_id, client);
            }
            ConcreteComputeInstanceReplicaConfig::Managed {
                size_config,
                size_name: _,
                availability_zone,
            } => {
                let service_name = generate_replica_service_name(instance_id, replica_id);

                let service = self
                    .compute_orchestrator
                    .ensure_service(
                        &service_name,
                        ServiceConfig {
                            image: self.computed_image.clone(),
                            args: &|assigned| {
                                let mut compute_opts = vec![
                                    format!(
                                        "--listen-addr={}:{}",
                                        assigned.listen_host, assigned.ports["controller"]
                                    ),
                                    format!(
                                        "--internal-http-listen-addr={}:{}",
                                        assigned.listen_host, assigned.ports["internal-http"]
                                    ),
                                    format!("--workers={}", size_config.workers),
                                    format!("--opentelemetry-resource=instance_id={}", instance_id),
                                    format!("--opentelemetry-resource=replica_id={}", replica_id),
                                ];
                                compute_opts.extend(
                                    assigned.peers.iter().map(|(host, ports)| {
                                        format!("{host}:{}", ports["compute"])
                                    }),
                                );
                                if let Some(index) = assigned.index {
                                    compute_opts.push(format!("--process={index}"));
                                    compute_opts.push(format!(
                                        "--opentelemetry-resource=replica_index={}",
                                        index
                                    ));
                                }
                                if self.linger {
                                    compute_opts.push(format!("--linger"));
                                }
                                compute_opts
                            },
                            ports: vec![
                                ServicePort {
                                    name: "controller".into(),
                                    port_hint: 2100,
                                },
                                ServicePort {
                                    name: "compute".into(),
                                    port_hint: 2102,
                                },
                                ServicePort {
                                    name: "internal-http".into(),
                                    port_hint: 6875,
                                },
                            ],
                            cpu_limit: size_config.cpu_limit,
                            memory_limit: size_config.memory_limit,
                            scale: size_config.scale,
                            labels: hashmap! {
                                "cluster-id".into() => instance_id.to_string(),
                                "type".into() => "cluster".into(),
                            },
                            availability_zone,
                        },
                    )
                    .await?;
                let client = ComputedRemoteClient::new(&service.addresses("controller"));
                let client: Box<dyn ComputeClient<T>> = Box::new(client);
                self.compute_mut(instance_id)
                    .unwrap()
                    .add_replica(replica_id, client);
            }
        }

        Ok(())
    }

    /// Removes a replica from an instance, including its service in the
    /// orchestrator.
    pub async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        if let ConcreteComputeInstanceReplicaConfig::Managed {
            size_config: _,
            size_name: _,
            availability_zone: _,
        } = config
        {
            let service_name = generate_replica_service_name(instance_id, replica_id);
            self.compute_orchestrator
                .drop_service(&service_name)
                .await?;
        }
        let mut compute = self.compute_mut(instance_id).unwrap();
        compute.remove_replica(replica_id);
        Ok(())
    }

    /// Removes an instance from the orchestrator.
    ///
    /// # Panics
    /// - If the identified `instance` still has active replicas.
    pub async fn drop_instance(
        &mut self,
        instance: ComputeInstanceId,
    ) -> Result<(), anyhow::Error> {
        if let Some(mut compute) = self.compute.remove(&instance) {
            assert!(
                compute.client.get_replica_ids().next().is_none(),
                "cannot drop instances with provisioned replicas; call `drop_replica` first"
            );
            self.compute_orchestrator
                .drop_service(&format!("cluster-{instance}"))
                .await?;
            compute.client.send(ComputeCommand::DropInstance).await?;
        }
        Ok(())
    }

    /// Listen for changes to compute services reported by the orchestrator.
    pub fn watch_compute_services(&self) -> BoxStream<'static, ComputeInstanceEvent> {
        fn translate_event(event: ServiceEvent) -> Result<ComputeInstanceEvent, anyhow::Error> {
            let (instance_id, replica_id) = parse_replica_service_name(&event.service_id)?;
            Ok(ComputeInstanceEvent {
                instance_id,
                replica_id,
                process_id: event.process_id,
                status: event.status,
                time: event.time,
            })
        }

        let stream = self
            .compute_orchestrator
            .watch_services()
            .map(|event| event.and_then(translate_event))
            .filter_map(|event| async {
                match event {
                    Ok(event) => Some(event),
                    Err(error) => {
                        tracing::error!("service watch error: {error}");
                        None
                    }
                }
            });

        Box::pin(stream)
    }
}

impl<T> Controller<T> {
    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> &dyn StorageController<Timestamp = T> {
        &*self.storage_controller
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(&mut self) -> &mut dyn StorageController<Timestamp = T> {
        &mut *self.storage_controller
    }

    /// Acquires an immutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute(&self, instance: ComputeInstanceId) -> Option<ComputeController<T>> {
        let compute = self.compute.get(&instance)?;
        Some(ComputeController {
            instance,
            compute,
            storage_controller: self.storage(),
        })
    }

    /// Acquires a mutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute_mut(&mut self, instance: ComputeInstanceId) -> Option<ComputeControllerMut<T>> {
        let compute = self.compute.get_mut(&instance)?;
        Some(ComputeControllerMut {
            instance,
            compute,
            storage_controller: &mut *self.storage_controller,
        })
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + Codec64,
{
    /// Wait until the controller is ready to process a response.
    ///
    /// This method may await indefinitely.
    ///
    /// This method is intended to be cancel-safe: dropping the returned
    /// future at any await point and restarting from the beginning is fine.
    // This method's correctness relies on the assumption that the _underlying_
    // clients are _also_ cancel-safe, since it introduces its own `select` call.
    pub async fn ready(&mut self) -> Result<(), anyhow::Error> {
        if self.stashed_response.is_some() {
            Ok(())
        } else {
            let mut compute_stream: StreamMap<_, _> = self
                .compute
                .iter_mut()
                .map(|(id, compute)| (*id, compute.client.as_stream()))
                .collect();
            loop {
                tokio::select! {
                    Some((instance, response)) = compute_stream.next() => {
                        let response = response?;
                        assert!(self.stashed_response.is_none());
                        self.stashed_response = Some(UnderlyingControllerResponse::Compute(instance, response));
                        return Ok(());
                    }
                    response = self.storage_controller.recv() => {
                        if let Some(response) = response? {
                            assert!(self.stashed_response.is_none());
                            self.stashed_response = Some(UnderlyingControllerResponse::Storage(response));
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Process any queued messages.
    ///
    /// This method is _not_ known to be cancel-safe, and should _not_ be called
    /// in the receiver of a `tokio::select` branch. Calling it in the handler of
    /// such a branch is fine.
    ///
    /// This method is _not_ intended to await indefinitely for reconnections and such;
    /// the coordinator relies on this property in order to not hang.
    pub async fn process(&mut self) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        let stashed = self.stashed_response.take().expect("`process` is not allowed to block indefinitely -- `ready` should be polled to completion first.");
        match stashed {
            UnderlyingControllerResponse::Storage(response) => match response {
                StorageResponse::FrontierUppers(updates) => {
                    self.storage_controller
                        .update_write_frontiers(&updates)
                        .await?;
                    Ok(None)
                }
                StorageResponse::LinearizedTimestamps(res) => {
                    Ok(Some(ControllerResponse::LinearizedTimestamps(res)))
                }
            },
            UnderlyingControllerResponse::Compute(
                instance,
                ActiveReplicationResponse::ComputeResponse(response),
            ) => {
                match response {
                    ComputeResponse::FrontierUppers(updates) => {
                        self.compute_mut(instance)
                            // TODO: determine if this is an error, or perhaps just a late
                            // response about a terminated instance.
                            .expect("Reference to absent instance")
                            .update_write_frontiers(&updates)
                            .await?;
                        Ok(None)
                    }
                    ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                        self.compute_mut(instance)
                            .expect("Reference to absent instance")
                            .remove_peeks(std::iter::once(uuid))
                            .await?;
                        Ok(Some(ControllerResponse::PeekResponse(
                            uuid,
                            peek_response,
                            otel_ctx,
                        )))
                    }
                    ComputeResponse::TailResponse(global_id, response) => {
                        let mut changes = timely::progress::ChangeBatch::new();
                        match &response {
                            TailResponse::Batch(TailBatch { lower, upper, .. }) => {
                                changes.extend(upper.iter().map(|time| (time.clone(), 1)));
                                changes.extend(lower.iter().map(|time| (time.clone(), -1)));
                            }
                            TailResponse::DroppedAt(frontier) => {
                                // The tail will not be written to again, but we should not confuse that
                                // with the source of the TAIL being complete through this time.
                                changes.extend(frontier.iter().map(|time| (time.clone(), -1)));
                            }
                        }
                        self.compute_mut(instance)
                            .expect("Reference to absent instance")
                            .update_write_frontiers(&[(global_id, changes)])
                            .await?;
                        Ok(Some(ControllerResponse::TailResponse(global_id, response)))
                    }
                }
            }
            UnderlyingControllerResponse::Compute(
                _instance,
                ActiveReplicationResponse::ReplicaHeartbeat(replica_id, when),
            ) => Ok(Some(ControllerResponse::ComputeReplicaHeartbeat(
                replica_id, when,
            ))),
        }
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64 + Unpin,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    StorageCommand<T>: RustType<super::ProtoStorageCommand>,
    StorageResponse<T>: RustType<super::ProtoStorageResponse>,
{
    /// Creates a new controller.
    pub async fn new(config: ControllerConfig) -> Self {
        let storage_controller = crate::client::controller::storage::Controller::new(
            config.storage_stash_url,
            config.persist_location,
            config.persist_clients,
            config.orchestrator.namespace("storage"),
            config.storaged_image,
        )
        .await;
        Self {
            linger: config.linger,
            storage_controller: Box::new(storage_controller),
            compute_orchestrator: config.orchestrator.namespace("compute"),
            computed_image: config.computed_image,
            compute: BTreeMap::default(),
            stashed_response: None,
        }
    }
}

/// Compaction policies for collections maintained by `Controller`.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum ReadPolicy<T> {
    /// Maintain the collection as valid from this frontier onward.
    ValidFrom(Antichain<T>),
    /// Maintain the collection as valid from a function of the write frontier.
    ///
    /// This function will only be re-evaluated when the write frontier changes.
    /// If the intended behavior is to change in response to external signals,
    /// consider using the `ValidFrom` variant to manually pilot compaction.
    ///
    /// The `Arc` makes the function cloneable.
    LagWriteFrontier(
        #[derivative(Debug = "ignore")] Arc<dyn Fn(AntichainRef<T>) -> Antichain<T> + Send + Sync>,
    ),
    /// Allows one to express multiple read policies, taking the least of
    /// the resulting frontiers.
    Multiple(Vec<ReadPolicy<T>>),
}

impl ReadPolicy<mz_repr::Timestamp> {
    /// Creates a read policy that lags the write frontier by the indicated amount, rounded down to a multiple of that amount.
    ///
    /// The rounding down is done to reduce the number of changes the capability undergoes, with the thinking
    /// being that if you are ok with `lag`, then getting something between `lag` and `2 x lag` should be ok.
    pub fn lag_writes_by(lag: mz_repr::Timestamp) -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                // Subtract the lag from the time, and then round down to a multiple thereof to cut chatter.
                let mut time = upper[0];
                if lag != 0 {
                    time = time.saturating_sub(lag);
                    time = time.saturating_sub(time % lag);
                }
                Antichain::from_elem(time)
            }
        }))
    }
}

impl<T: Timestamp> ReadPolicy<T> {
    pub fn frontier(&self, write_frontier: AntichainRef<T>) -> Antichain<T> {
        match self {
            ReadPolicy::ValidFrom(frontier) => frontier.clone(),
            ReadPolicy::LagWriteFrontier(logic) => logic(write_frontier),
            ReadPolicy::Multiple(policies) => {
                let mut frontier = Antichain::new();
                for policy in policies.iter() {
                    for time in policy.frontier(write_frontier).iter() {
                        frontier.insert(time.clone());
                    }
                }
                frontier
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_writes_by_zero() {
        let policy = ReadPolicy::lag_writes_by(0);
        let write_frontier = Antichain::from_elem(5);
        assert_eq!(policy.frontier(write_frontier.borrow()), write_frontier);
    }
}
