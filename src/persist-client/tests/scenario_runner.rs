mod scenario;

use anyhow::Result;

use scenario::{
    ScenarioHistoryOp, ScenarioOp, ScenarioRunner, ShardOracle, SnapshotError, Update,
    end_to_end_smoke_ops,
};

#[test]
fn history_ops_are_marked() {
    assert!(!ScenarioOp::open_writer("w0").enters_linearizability_history());
    assert_eq!(ScenarioOp::open_writer("w0").history_op(), None);
    assert!(!ScenarioOp::open_reader("r0").enters_linearizability_history());
    assert_eq!(ScenarioOp::open_reader("r0").history_op(), None);
    assert!(
        ScenarioOp::compare_and_append("w0", vec![Update::new("a", 0, 1)], 0, 1)
            .enters_linearizability_history()
    );
    assert_eq!(
        ScenarioOp::compare_and_append("w0", vec![Update::new("a", 0, 1)], 0, 1).history_op(),
        Some(ScenarioHistoryOp::CompareAndAppend {
            writer: "w0".into(),
            updates: vec![Update::new("a", 0, 1)],
            expected_upper: 0,
            new_upper: 1,
        })
    );
    assert!(ScenarioOp::downgrade_since("r0", 1).enters_linearizability_history());
    assert_eq!(
        ScenarioOp::downgrade_since("r0", 1).history_op(),
        Some(ScenarioHistoryOp::DowngradeSince {
            reader: "r0".into(),
            new_since: 1,
        })
    );
    assert!(ScenarioOp::snapshot("r0", 0).enters_linearizability_history());
    assert_eq!(
        ScenarioOp::snapshot("r0", 0).history_op(),
        Some(ScenarioHistoryOp::Snapshot {
            reader: "r0".into(),
            as_of: 0,
        })
    );
}

#[test]
fn oracle_tracks_since_and_snapshots() -> Result<()> {
    let mut oracle = ShardOracle::default();

    oracle.apply(&ScenarioOp::open_writer("w0"))?;
    oracle.apply(&ScenarioOp::open_reader("r0"))?;
    oracle.apply(&ScenarioOp::compare_and_append(
        "w0",
        vec![Update::new("a", 0, 1), Update::new("b", 1, 1)],
        0,
        2,
    ))?;
    oracle.apply(&ScenarioOp::downgrade_since("r0", 1))?;

    let snapshot = oracle.apply(&ScenarioOp::snapshot("r0", 1))?;
    assert_eq!(
        snapshot,
        scenario::ScenarioObservation::Snapshot(Ok(vec![
            Update::new("a", 1, 1),
            Update::new("b", 1, 1),
        ]))
    );

    let too_old = oracle.apply(&ScenarioOp::snapshot("r0", 0))?;
    assert_eq!(
        too_old,
        scenario::ScenarioObservation::Snapshot(Err(SnapshotError::Since(1)))
    );

    Ok(())
}

#[tokio::test]
async fn in_mem_runner_matches_oracle() -> Result<()> {
    let mut runner = ScenarioRunner::new_in_mem().await?;
    let _ = runner.run_and_assert(end_to_end_smoke_ops()).await?;
    Ok(())
}
