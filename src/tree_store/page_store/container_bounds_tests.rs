//! Proof candidates only; not registered or executed.
// req: STO-020, PRJ-004, REC-001
use super::{
    ContainerBoundError, btree_insert_growth, btree_storage, hash_reserve_growth,
    hash_storage_from_historical_fresh_capacity, vector_amortized_growth, vector_storage,
};
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default)]
struct CollidingHasher;
impl Hasher for CollidingHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

#[test]
fn native_hash_tombstones_require_historical_allocation_provenance() {
    let mut map =
        HashMap::<u64, u64, BuildHasherDefault<CollidingHasher>>::with_capacity_and_hasher(
            128,
            BuildHasherDefault::default(),
        );
    let fresh = map.capacity();
    for key in 0..fresh {
        map.insert(u64::try_from(key).unwrap(), 1);
    }
    assert_eq!(map.capacity(), fresh);
    let retained = hash_storage_from_historical_fresh_capacity::<(u64, u64)>(fresh).unwrap();
    for key in 0..fresh {
        assert_eq!(map.remove(&u64::try_from(key).unwrap()), Some(1));
    }
    assert!(map.is_empty());
    // Pinned erase only updates controls/items, never frees the backing table.
    assert!(map.capacity() < fresh);
    let incorrectly_inverted_current =
        hash_storage_from_historical_fresh_capacity::<(u64, u64)>(map.capacity()).unwrap();
    assert!(incorrectly_inverted_current.requested_bytes < retained.requested_bytes);
    let grow = hash_reserve_growth::<(u64, u64)>(fresh, map.len(), 1).unwrap();
    assert!(grow.simultaneous_peak_bytes > retained.allocator_bytes);
    map.insert(1, 2);
    assert_eq!(map.get(&1), Some(&2));
}

#[test]
fn native_deque_bound_preserves_empty_retained_buffer_wrapping_and_zst() {
    let mut deque = VecDeque::<u64>::with_capacity(17);
    let old_capacity = deque.capacity();
    for value in 0..old_capacity {
        deque.push_back(u64::try_from(value).unwrap());
    }
    for _ in 0..old_capacity / 2 {
        deque.pop_front();
    }
    for value in 0..old_capacity / 2 {
        deque.push_back(u64::try_from(value).unwrap());
    }
    let growth = vector_amortized_growth::<u64>(old_capacity, deque.len(), 1).unwrap();
    deque.push_back(99);
    let after = vector_storage::<u64>(deque.capacity()).unwrap();
    assert!(growth.retained_after_bytes >= after.allocator_bytes);
    assert!(
        growth.simultaneous_peak_bytes
            >= after.allocator_bytes + vector_storage::<u64>(old_capacity).unwrap().allocator_bytes
    );
    deque.clear();
    assert!(
        vector_storage::<u64>(deque.capacity())
            .unwrap()
            .allocator_bytes
            > 0
    );
    let mut zst = VecDeque::<()>::with_capacity(17);
    zst.push_back(());
    assert_eq!(
        vector_storage::<()>(zst.capacity())
            .unwrap()
            .allocator_bytes,
        0
    );
    assert_eq!(
        vector_amortized_growth::<()>(zst.capacity(), zst.len(), 1)
            .unwrap()
            .simultaneous_peak_bytes,
        0
    );
}

#[test]
fn native_container_overflow_refuses_without_changing_real_containers() {
    let mut deque = VecDeque::<u64>::with_capacity(4);
    deque.push_back(7);
    let capacity = deque.capacity();
    assert!(matches!(
        vector_amortized_growth::<u64>(capacity, usize::MAX, 1),
        Err(ContainerBoundError::Overflow)
    ));
    assert!(hash_storage_from_historical_fresh_capacity::<(u64, u64)>(usize::MAX).is_err());
    assert!(btree_storage::<u64, u64>(usize::MAX).is_err());
    assert!(btree_insert_growth::<u64, u64>(usize::MAX, 1).is_err());
    assert_eq!(deque.capacity(), capacity);
    assert_eq!(deque.pop_front(), Some(7));
    // Empty B-trees may retain a real root allocation; zero entries is not zero.
    assert!(btree_storage::<u64, u64>(0).unwrap() > 0);
}

#[test]
fn native_btree_resource_quote_refuses_overaligned_elements() {
    #[repr(align(4096))]
    struct Overaligned;
    assert!(matches!(
        btree_storage::<Overaligned, u64>(1),
        Err(ContainerBoundError::UnsupportedElementLayout)
    ));
    assert!(matches!(
        btree_insert_growth::<u64, Overaligned>(0, 1),
        Err(ContainerBoundError::UnsupportedElementLayout)
    ));
    // This is a closed B-tree quote profile, not a generic Vec/ZST restriction.
    assert_eq!(
        vector_storage::<Overaligned>(usize::MAX)
            .unwrap()
            .allocator_bytes,
        0
    );
}
