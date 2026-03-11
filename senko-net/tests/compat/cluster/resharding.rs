use senko_cluster::RouteDecision;

use super::harness::ClusterHarness;

fn begin_migration(harness: &mut ClusterHarness, source_idx: usize, target_idx: usize, slot: u16) {
    let target_id = harness.node_id(target_idx).to_string();
    let source_id = harness.node_id(source_idx).to_string();
    let slot_text = slot.to_string();
    let _ = harness
        .execute_cluster(
            source_idx,
            &["SETSLOT", &slot_text, "MIGRATING", &target_id],
        )
        .unwrap();
    let _ = harness
        .execute_cluster(
            target_idx,
            &["SETSLOT", &slot_text, "IMPORTING", &source_id],
        )
        .unwrap();

    {
        let mut source_cluster = harness.nodes[source_idx].cluster.borrow_mut();
        let mut source_store = harness.nodes[source_idx].store.borrow_mut();
        source_cluster
            .slot_index_mut()
            .rebuild_from_store(&mut source_store);
    }
    {
        let mut target_cluster = harness.nodes[target_idx].cluster.borrow_mut();
        let mut target_store = harness.nodes[target_idx].store.borrow_mut();
        target_cluster
            .slot_index_mut()
            .rebuild_from_store(&mut target_store);
    }
}

fn migrate_one_chunk(
    harness: &mut ClusterHarness,
    source_idx: usize,
    target_idx: usize,
    slot: u16,
) -> bool {
    let mut source_cluster = harness.nodes[source_idx].cluster.borrow_mut();
    let mut target_cluster = harness.nodes[target_idx].cluster.borrow_mut();
    let mut source_store = harness.nodes[source_idx].store.borrow_mut();
    let mut target_store = harness.nodes[target_idx].store.borrow_mut();
    let mut migration = std::mem::take(source_cluster.migration_mut());
    let mut source_index = std::mem::take(source_cluster.slot_index_mut());
    let mut target_index = std::mem::take(target_cluster.slot_index_mut());
    let step = migration
        .migrate_slot_chunk(
            slot,
            &mut source_store,
            &mut target_store,
            &mut source_index,
            &mut target_index,
            source_cluster.slot_table_mut(),
        )
        .unwrap();
    *source_cluster.migration_mut() = migration;
    *source_cluster.slot_index_mut() = source_index;
    *target_cluster.slot_index_mut() = target_index;
    step.complete
}

fn finalize_migration(
    harness: &mut ClusterHarness,
    source_idx: usize,
    target_idx: usize,
    slot: u16,
) {
    let target_id = harness.node_id(target_idx).to_string();
    let slot_text = slot.to_string();
    let _ = harness
        .execute_cluster(source_idx, &["SETSLOT", &slot_text, "NODE", &target_id])
        .unwrap();
    let _ = harness
        .execute_cluster(target_idx, &["SETSLOT", &slot_text, "NODE", &target_id])
        .unwrap();
    harness.sync_views();
}

#[test]
fn ask_redirect_during_and_after_migration() {
    let mut harness = ClusterHarness::start(2, 0);
    let key = "{0}:migrating";
    let slot = senko_cluster::crc16_slot(key.as_bytes());
    let source_idx = 0;
    let target_idx = 1;

    harness.set(source_idx, key, "value");
    begin_migration(&mut harness, source_idx, target_idx, slot);

    assert!(matches!(
        harness.route_from(source_idx, key.as_bytes(), true),
        RouteDecision::LocalShard(_)
    ));

    let _ = migrate_one_chunk(&mut harness, source_idx, target_idx, slot);
    assert!(matches!(
        harness.route_from(source_idx, key.as_bytes(), true),
        RouteDecision::Ask(_, _)
    ));

    harness.arm_asking(target_idx);
    assert!(matches!(
        harness.route_asking(target_idx, key.as_bytes(), true),
        RouteDecision::LocalShard(_)
    ));

    finalize_migration(&mut harness, source_idx, target_idx, slot);
    assert!(matches!(
        harness.route_from(source_idx, key.as_bytes(), true),
        RouteDecision::Moved(_, _)
    ));
}

#[test]
fn add_node_rebalances_slot_distribution() {
    let mut harness = ClusterHarness::start(4, 0);
    let new_primary_idx = 3;
    let drained = harness.nodes[new_primary_idx]
        .cluster
        .borrow()
        .local_meta()
        .unwrap()
        .slots
        .iter()
        .map(|slot| slot as u16)
        .collect::<Vec<_>>();
    {
        let mut state = harness.nodes[new_primary_idx].cluster.borrow_mut();
        state.local_meta_mut().unwrap().slots.clear();
    }
    {
        let mut state = harness.nodes[0].cluster.borrow_mut();
        for slot in &drained {
            state.local_meta_mut().unwrap().slots.insert(*slot as u32);
        }
    }
    harness.sync_views();
    let rebalanced = (0..4_096).map(|slot| slot as u16).collect::<Vec<_>>();
    harness.rebalance_add_node(new_primary_idx, &rebalanced);

    let counts = harness
        .nodes
        .iter()
        .take(4)
        .map(|node| node.cluster.borrow().local_meta().unwrap().slots.len())
        .collect::<Vec<_>>();
    assert_eq!(counts.iter().sum::<u64>(), 16_384);
    assert!(counts.iter().all(|count| *count >= 4_000));
}

#[test]
fn remove_node_after_draining_preserves_remaining_slot_coverage() {
    let mut harness = ClusterHarness::start(4, 0);
    let drained_idx = 3;
    let slots = harness.nodes[drained_idx]
        .cluster
        .borrow()
        .local_meta()
        .unwrap()
        .slots
        .iter()
        .map(|slot| slot as u16)
        .collect::<Vec<_>>();
    harness.rebalance_add_node(0, &slots);
    {
        let mut state = harness.nodes[drained_idx].cluster.borrow_mut();
        state.local_meta_mut().unwrap().slots.clear();
    }
    harness.sync_views();

    let total = harness
        .nodes
        .iter()
        .take(3)
        .map(|node| node.cluster.borrow().local_meta().unwrap().slots.len())
        .sum::<u64>();
    assert_eq!(total, 16_384);
}
