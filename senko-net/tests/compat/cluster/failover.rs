use senko_cluster::{NodeRole, NodeState};

use super::harness::ClusterHarness;

#[test]
fn automatic_failover_promotes_replica_after_primary_failure() {
    let mut harness = ClusterHarness::start(1, 1);
    let primary_idx = 0;
    let replica_idx = 1;
    let failed_primary = harness.node_id(primary_idx);

    harness.kill(primary_idx);
    for step in 0..6 {
        harness.run_failover_round(20_000 + step * 800);
    }

    let replica_meta = harness.nodes[replica_idx]
        .gossip
        .cluster()
        .get_node(&harness.node_id(replica_idx))
        .unwrap();
    assert!(matches!(replica_meta.role, NodeRole::Primary));
    assert!(replica_meta.slots.contains(0));
    let old_primary_meta = harness.nodes[replica_idx]
        .gossip
        .cluster()
        .get_node(&failed_primary)
        .unwrap();
    assert!(matches!(old_primary_meta.role, NodeRole::Replica { .. }));
}

#[test]
fn failover_data_consistency_preserves_confirmed_keys() {
    let mut harness = ClusterHarness::start(1, 1);
    let primary_idx = 0;
    let replica_idx = 1;
    for index in 0..1_000 {
        harness.set(primary_idx, &format!("k:{index}"), &format!("v:{index}"));
        harness.set(replica_idx, &format!("k:{index}"), &format!("v:{index}"));
    }

    harness.kill(primary_idx);
    for step in 0..6 {
        harness.run_failover_round(15_000 + step * 800);
    }

    for index in 0..1_000 {
        assert_eq!(
            harness.get(replica_idx, &format!("k:{index}")),
            Some(format!("v:{index}").into_bytes())
        );
    }
}

#[test]
fn minority_partition_cannot_promote_new_primary() {
    let mut harness = ClusterHarness::start(3, 1);
    let minority_replica_idx = 5;
    let failed_primary_idx = 4;
    let failed_primary_id = harness.node_id(failed_primary_idx);
    let replica_id = harness.node_id(minority_replica_idx);

    harness.kill(failed_primary_idx);
    {
        let replica = &mut harness.nodes[minority_replica_idx].gossip;
        if let Some(primary) = replica.cluster_mut().get_node_mut(&failed_primary_id) {
            primary.state = NodeState::Failed;
        }
        replica.set_primary_progress(replica_id, [7; 16], 500);
    }
    for step in 0..6 {
        harness.run_failover_round(20_000 + step * 800);
    }
    let role = harness.nodes[minority_replica_idx]
        .gossip
        .cluster()
        .get_node(&replica_id)
        .unwrap()
        .role
        .clone();
    assert!(matches!(role, NodeRole::Replica { .. }));
}
