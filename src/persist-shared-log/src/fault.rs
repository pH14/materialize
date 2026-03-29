// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! BUGGIFY-style cooperative fault injection for deterministic simulation.
//!
//! Each injection point has a name. In test mode, a [`FaultConfig`] controls
//! which points are active and with what probability. The configuration is
//! seeded from the simulation seed, so fault injection is deterministic and
//! reproducible.
//!
//! In production (non-test) builds, all injection points compile to no-ops.
//!
//! ## Usage in production code
//!
//! ```rust,ignore
//! use crate::fault;
//!
//! async fn do_reconfigure(&mut self) -> Result<(), Error> {
//!     self.persist_intent().await;
//!     fault::maybe_fail("after_intent_persist")?;
//!     self.seal_shards().await;
//!     fault::maybe_fail("after_seal")?;
//!     // ...
//! }
//! ```
//!
//! ## Usage in tests
//!
//! ```rust,ignore
//! fault::configure(FaultConfig::seeded(42));
//! // Run test — injection points fire deterministically based on seed.
//! fault::clear();
//! ```

use std::cell::RefCell;
use std::collections::BTreeSet;

/// Configuration for fault injection. Controls which injection points are
/// active and with what probability.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Fields read only in cfg(test) `maybe_fail`; non-test version is a no-op.
pub struct FaultConfig {
    /// Injection points that are enabled for this test run.
    enabled_points: BTreeSet<&'static str>,
    /// Probability numerator / 256 that an enabled point fires.
    fire_threshold: u8,
    /// Simple counter-based PRNG state for deterministic decisions.
    state: u64,
}

/// All known injection point names.
pub const INJECTION_POINTS: &[&str] = &[
    "after_intent_persist",
    "after_seal",
    "after_actor_spawn",
    "after_replay_complete",
    "after_routing_swap",
    "after_commit_persist",
    "before_hold_release",
    "during_predecessor_replay",
];

/// Simple xorshift64 for deterministic pseudo-random decisions without
/// pulling in the `rand` crate.
fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[allow(clippy::as_conversions)]
fn probability_to_threshold(fire_probability: f64) -> u8 {
    (fire_probability * 256.0).min(255.0) as u8
}

impl FaultConfig {
    /// Create a fault configuration from a seed. Each injection point is
    /// independently enabled/disabled (50% chance), and enabled points fire
    /// with approximately `fire_probability` (0.0 to 1.0).
    pub fn seeded(seed: u64, fire_probability: f64) -> Self {
        let mut state = seed.wrapping_add(0xB099_1F10);
        let mut enabled = BTreeSet::new();
        for &point in INJECTION_POINTS {
            state = xorshift64(state);
            if state % 2 == 0 {
                enabled.insert(point);
            }
        }
        let fire_threshold = probability_to_threshold(fire_probability);
        FaultConfig {
            enabled_points: enabled,
            fire_threshold,
            state,
        }
    }

    /// Create a configuration with a specific set of enabled points.
    pub fn with_points(points: &[&'static str], fire_probability: f64, seed: u64) -> Self {
        let fire_threshold = probability_to_threshold(fire_probability);
        FaultConfig {
            enabled_points: points.iter().copied().collect(),
            fire_threshold,
            state: seed,
        }
    }

    /// Check whether a specific injection point should fire.
    #[allow(dead_code)] // Used only in cfg(test) `maybe_fail`.
    fn should_fire(&mut self, point: &str) -> bool {
        if !self.enabled_points.contains(point) {
            return false;
        }
        self.state = xorshift64(self.state);
        (self.state % 256) < u64::from(self.fire_threshold)
    }
}

thread_local! {
    static FAULT_CONFIG: RefCell<Option<FaultConfig>> = const { RefCell::new(None) };
}

/// Install a fault configuration for the current thread.
pub fn configure(config: FaultConfig) {
    FAULT_CONFIG.with(|c| {
        *c.borrow_mut() = Some(config);
    });
}

/// Clear the fault configuration for the current thread.
pub fn clear() {
    FAULT_CONFIG.with(|c| {
        *c.borrow_mut() = None;
    });
}

/// Check an injection point. Returns `Err` if the point fires (simulating
/// a crash or error at this location).
///
/// In non-test builds, this is a no-op that always returns `Ok(())`.
///
/// The error message includes the injection point name for debugging.
#[cfg(test)]
pub fn maybe_fail(point: &'static str) -> Result<(), String> {
    FAULT_CONFIG.with(|c| {
        let mut borrow = c.borrow_mut();
        if let Some(config) = borrow.as_mut() {
            if config.should_fire(point) {
                tracing::info!(point, "fault injection fired");
                return Err(format!("fault injection: {}", point));
            }
        }
        Ok(())
    })
}

/// Non-test version: always succeeds.
#[cfg(not(test))]
pub fn maybe_fail(_point: &'static str) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_config_deterministic() {
        let c1 = FaultConfig::seeded(42, 0.25);
        let c2 = FaultConfig::seeded(42, 0.25);
        assert_eq!(c1.enabled_points, c2.enabled_points);
        assert_eq!(c1.state, c2.state);
    }

    #[test]
    fn fault_config_different_seeds_differ() {
        let c1 = FaultConfig::seeded(1, 0.25);
        let c2 = FaultConfig::seeded(99, 0.25);
        // Not guaranteed but extremely likely with 8 points.
        assert_ne!(
            c1.enabled_points, c2.enabled_points,
            "different seeds should usually produce different enabled points"
        );
    }

    #[test]
    fn maybe_fail_no_config() {
        clear();
        assert!(maybe_fail("after_intent_persist").is_ok());
    }

    #[test]
    fn maybe_fail_with_config() {
        // With 100% fire probability, all enabled points should fire.
        configure(FaultConfig::with_points(
            &["after_intent_persist"],
            1.0,
            42,
        ));
        assert!(maybe_fail("after_intent_persist").is_err());
        assert!(maybe_fail("after_seal").is_ok()); // not enabled
        clear();
    }

    #[test]
    fn maybe_fail_zero_probability() {
        configure(FaultConfig::with_points(
            &["after_intent_persist"],
            0.0,
            42,
        ));
        assert!(maybe_fail("after_intent_persist").is_ok());
        clear();
    }
}
