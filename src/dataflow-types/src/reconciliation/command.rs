// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Functionality to reconcile commands between a COMPUTE controller and a COMPUTE instance.
//!
//! The [ComputeCommandReconcile] struct implements [GenericClient],
//! which allow the controller to reconnect after restarts. It maintains enough state to
//! get a newly connected instance up-to-date and matches existing installed objects with
//! what the controller wants to provide.
//!
//! [ComputeCommandReconcile] is designed to live in a COMPUTE instance and liberally uses `assert` to
//! validate the correctness of commands. It is not intended to be part of a COMPUTE controller as
//! it might have correctness issues.
//!
//! The contract between this and the COMPUTE controller is that identifiers are not re-used and
//! describe the same object after restarts. Failure to do so will result in undefined behavior.
//!
//! The reconciliation presents to a restarted COMPUTE controller as if the COMPUTE instance was
//! restarted as well. It responds with the expected replies after a `CreateInstance` command and
//! brings the controller  up-to-date by notifying it about the current upper frontiers.
//!
//! Controllers should ignore all responses received before `CreateInstance` as those were intended
//! for the previous instance. The implementation currently does not distinguish between buffering
//! messages for a disconnected controller and talking to a live controller.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use timely::progress::frontier::MutableAntichain;
use timely::progress::ChangeBatch;
use tracing::warn;

use mz_repr::GlobalId;

use crate::client::controller::storage::CollectionMetadata;
use crate::client::{ComputeClient, ComputeCommand, ComputeResponse, GenericClient};
use crate::{DataflowDescription, Plan};

/// Reconcile commands targeted at a COMPUTE instance.
///
/// See the module-level documentation for details.
#[derive(Debug)]
pub struct ComputeCommandReconcile<T, C> {
    /// The client wrapped by this struct.
    client: C,
    /// Whether we've seen a `CreateInstance` command without a corresponding
    /// `DropInstance` command.
    created: bool,
    /// Dataflows by ID.
    dataflows: HashMap<GlobalId, DataflowDescription<Plan<T>, CollectionMetadata, T>>,
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashSet<uuid::Uuid>,
    /// Stash of responses to send back to the controller.
    responses: VecDeque<ComputeResponse<T>>,
    /// Upper frontiers for indexes, sources, and sinks.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
}

#[async_trait]
impl<T, C> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for ComputeCommandReconcile<T, C>
where
    C: ComputeClient<T>,
    T: timely::progress::Timestamp + Copy,
{
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        if let Some(response) = self.responses.pop_front() {
            Ok(Some(response))
        } else {
            let response = self.client.recv().await;
            if let Ok(Some(response)) = response {
                self.absorb_response(response)
            }
            Ok(self.responses.pop_front())
        }
    }
}

impl<T, C> ComputeCommandReconcile<T, C>
where
    C: ComputeClient<T>,
    T: timely::progress::Timestamp + Copy,
{
    /// Construct a new [ComputeCommandReconcile].
    ///
    /// * `client`: The client wrapped by this struct.
    pub fn new(client: C) -> Self {
        Self {
            client,
            created: Default::default(),
            dataflows: Default::default(),
            peeks: Default::default(),
            responses: Default::default(),
            uppers: Default::default(),
        }
    }

    /// Start tracking of a id within an instance.
    ///
    /// If we're already tracking this ID, it means that the controller lost connection and
    /// reconnected (or has a bug). We're updating the controller's upper to match the local state.
    fn start_tracking(&mut self, id: GlobalId) {
        let frontier = timely::progress::frontier::MutableAntichain::new_bottom(T::minimum());
        match self.uppers.entry(id) {
            Entry::Occupied(entry) => {
                // We're about to start tracking an already-bound ID. This means that the controller
                // needs to be informed about the `upper`.
                let mut change_batch = ChangeBatch::new_from(T::minimum(), -1);
                change_batch.extend(entry.get().frontier().iter().copied().map(|t| (t, 1)));
                self.responses
                    .push_back(ComputeResponse::FrontierUppers(vec![(id, change_batch)]));
            }
            Entry::Vacant(entry) => {
                entry.insert(frontier);
            }
        }
    }

    /// Stop tracking the id within an instance.
    fn stop_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id);
        if previous.is_none() {
            warn!("Protocol error: ceasing frontier tracking for absent identifier {id:?}");
        }
        // Remove dataflow export information.
        self.dataflows.remove(&id);
    }

    /// Absorbs a response, and produces response that should be emitted.
    pub fn absorb_response(&mut self, message: ComputeResponse<T>) {
        match message {
            ComputeResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }

                self.responses
                    .push_back(ComputeResponse::FrontierUppers(list));
            }
            ComputeResponse::PeekResponse(uuid, response, otel_ctx) => {
                if self.peeks.remove(&uuid) {
                    self.responses
                        .push_back(ComputeResponse::PeekResponse(uuid, response, otel_ctx));
                }
            }
            ComputeResponse::TailResponse(id, response) => {
                self.responses
                    .push_back(ComputeResponse::TailResponse(id, response));
            }
        }
    }

    async fn absorb_command(&mut self, command: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        use ComputeCommand::*;
        match command {
            CreateInstance(config) => {
                // TODO: Handle `config.logging` correctly when reconnecting. We currently assume
                // that the logging config stays the same.
                if !self.created {
                    if let Some(logging) = &config.logging {
                        for id in logging.log_identifiers() {
                            if !self.uppers.contains_key(&id) {
                                self.start_tracking(id);
                            }
                        }
                    }
                    self.client.send(CreateInstance(config)).await?;
                    self.created = true;
                }
                Ok(())
            }
            cmd @ DropInstance => {
                if self.created {
                    self.created = false;
                    self.uppers.clear();
                    self.client.send(cmd).await
                } else {
                    Ok(())
                }
            }
            CreateDataflows(dataflows) => {
                let mut create = Vec::new();
                for dataflow in dataflows {
                    for id in dataflow.export_ids() {
                        self.start_tracking(id);
                    }
                    match self.dataflows.entry(dataflow.global_id().unwrap()) {
                        Entry::Vacant(entry) => {
                            entry.insert(dataflow.clone());
                            create.push(dataflow);
                        }
                        Entry::Occupied(entry) => {
                            assert!(
                                entry.get().compatible_with(&dataflow),
                                "New dataflow with same ID {:?}",
                                dataflow.id
                            );
                        }
                    }
                }
                if !create.is_empty() {
                    self.client.send(CreateDataflows(create)).await?
                }
                Ok(())
            }
            AllowCompaction(frontiers) => {
                for (id, frontier) in &frontiers {
                    if frontier.is_empty() {
                        self.stop_tracking(*id);
                    }
                }
                self.client.send(AllowCompaction(frontiers)).await
            }
            Peek(peek) => {
                self.peeks.insert(peek.uuid);
                self.client.send(ComputeCommand::Peek(peek)).await
            }
            CancelPeeks { uuids } => {
                for uuid in &uuids {
                    self.peeks.remove(uuid);
                }
                self.client.send(CancelPeeks { uuids }).await
            }
        }
    }
}
