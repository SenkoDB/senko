use senko_cluster::{RouteDecision, crc16_slot};

use super::harness::{ClusterHarness, same_slot};

#[test]
fn harness_helpers_are_usable() {
    let mut harness = ClusterHarness::start(1, 0);
    assert_eq!(harness.primary(0).index, 0);
    let _client = harness.client(0);
    harness.restart(0);
    harness.wait_healthy(std::time::Duration::from_millis(10));
}

#[test]
fn basic_routing_returns_moved_from_wrong_node_and_serves_from_owner() {
    let mut harness = ClusterHarness::start(3, 0);
    let key = b"foo";
    let owner = match harness.route_from(0, key, true) {
        RouteDecision::LocalShard(_) => 0,
        RouteDecision::Moved(node_id, _) => harness
            .nodes
            .iter()
            .position(|node| node.cluster.borrow().local_node_id() == Some(node_id))
            .unwrap(),
        decision => panic!("unexpected route decision: {decision:?}"),
    };

    harness.set(owner, "foo", "bar");
    assert_eq!(harness.get(owner, "foo"), Some(b"bar".to_vec()));

    let wrong = (owner + 1) % 3;
    assert!(matches!(
        harness.route_from(wrong, key, true),
        RouteDecision::Moved(_, _)
    ));
}

#[test]
fn hash_tag_keys_route_to_same_owner() {
    let harness = ClusterHarness::start(3, 0);
    let name = b"{user:1}.name";
    let age = b"{user:1}.age";

    assert_eq!(crc16_slot(name), crc16_slot(age));
    assert!(same_slot(&[name, age]));
    assert_eq!(
        harness.route_from(0, name, true),
        harness.route_from(0, age, true)
    );
}

#[test]
fn cross_slot_detection_matches_hash_tag_behavior() {
    assert!(!same_slot(&[b"foo", b"baz"]));
    assert!(same_slot(&[b"{tag}foo", b"{tag}baz"]));
}
