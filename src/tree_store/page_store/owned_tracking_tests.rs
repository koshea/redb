//! Buffer-custody controls, no fabricated page is read/freed or published.
// req: STO-020, PRJ-004, REC-001
use super::super::page_resources::{NativeNumericalRetainer, NativePageResources};
use super::*;

#[test]
fn native_owned_tracking_empty_cleanup_and_two_transfers_keep_prepared_capacity() {
    let requests = 3;
    let quote = SharedUncommittedPages::capacity_bytes(requests).unwrap();
    let resources = NativePageResources::for_fixed_write(quote, NativeNumericalRetainer::new(()));
    let tracked = SharedUncommittedPages::source(&resources, requests).unwrap();
    assert_eq!(resources.current(), quote);
    let initial_high = resources.admitted_high_water_for_test();
    for _ in 0..4 {
        assert!(tracked.take_all().is_empty());
    }
    assert_eq!(resources.current(), quote);
    assert_eq!(resources.admitted_high_water_for_test(), initial_high);
    // These are scalar buffer members only, never database allocation proofs.
    let first = PageNumber::new(0, 1, 0);
    tracked.prepare_requests(1).unwrap();
    tracked.insert(first);
    let batch = tracked.take_all();
    assert_eq!(batch.len(), 1);
    assert!(!tracked.contains(first));
    let second = PageNumber::new(0, 2, 0);
    tracked.prepare_requests(1).unwrap();
    tracked.insert(second);
    let tail = tracked.take_all();
    assert_eq!(tail.len(), 1);
    for _ in 0..4 {
        assert!(tracked.take_all().is_empty());
    }
    // A third nonempty epoch has no prepared active storage. Refusal happens
    // before the caller can mutate a native allocator or construct page bytes.
    assert!(tracked.prepare_requests(1).is_err());
    assert_eq!(resources.admitted_high_water_for_test(), initial_high);
    drop(tracked);
    assert!(
        resources.current() > 0,
        "moved batches lost their actual guards"
    );
    drop(batch);
    assert!(
        resources.current() > 0,
        "post-only batch must still retain custody"
    );
    drop(tail);
    assert_eq!(resources.current(), 0);
}

#[test]
fn native_owned_page_set_vector_and_iterators_keep_actual_buffer_guards() {
    let requests = 2;
    let set_quote = super::super::container_bounds::hash_storage_from_historical_fresh_capacity::<
        PageNumber,
    >(requests)
    .unwrap()
    .allocator_bytes as usize;
    let vec_quote = super::super::container_bounds::vector_storage::<PageNumber>(requests)
        .unwrap()
        .allocator_bytes as usize;
    let resources = NativePageResources::for_fixed_write(
        set_quote + vec_quote,
        NativeNumericalRetainer::new(()),
    );
    let mut batch = NativeOwnedPageBatch::prepare_source(&resources, requests).unwrap();
    batch.insert(PageNumber::new(0, 1, 0));
    batch.insert(PageNumber::new(0, 2, 0));
    assert_eq!(resources.current(), set_quote + vec_quote);
    let vector = batch.into_prepared_vec();
    assert_eq!(vector.len(), 2);
    assert_eq!(
        resources.current(),
        vec_quote,
        "the real hash allocation has retired"
    );
    let mut values = vector.into_iter();
    assert!(values.next().is_some());
    assert!(values.next().is_some());
    assert!(values.next().is_none());
    assert_eq!(
        resources.current(),
        vec_quote,
        "iterator storage lives until its owner drops"
    );
    drop(values);
    assert_eq!(resources.current(), 0);

    let mut batch = NativeOwnedPageBatch::prepare_source(&resources, requests).unwrap();
    batch.insert(PageNumber::new(0, 3, 0));
    let mut values = batch.into_iter();
    assert!(values.next().is_some());
    assert!(values.next().is_none());
    assert_eq!(resources.current(), set_quote + vec_quote);
    drop(values);
    assert_eq!(resources.current(), 0);
}

#[test]
fn native_owned_post_only_batch_keeps_membership_reuse_and_claim_exact() {
    let quote = SharedUncommittedPages::capacity_bytes(2).unwrap();
    let resources = NativePageResources::for_fixed_write(quote, NativeNumericalRetainer::new(()));
    let mut batch = NativeOwnedPageBatch::prepare_source(&resources, 2).unwrap();
    let first = PageNumber::new(0, 1, 0);
    let second = PageNumber::new(0, 2, 0);
    batch.insert(first);
    batch.insert(second);
    let mut state = UnpersistedState::default();
    state.extend(batch, true).unwrap();
    assert!(state.contains(first) && state.contains(second));
    assert!(state.claim(first));
    assert!(
        !state.claim(first),
        "reused number must not match an old allocation"
    );
    state.pages.insert(first); // Distinct later ordinary membership.
    assert!(state.claim(first));
    assert!(!state.contains(first));
    let claimed = state.take_post_commit_allocations();
    assert!(claimed.already_claimed);
    assert_eq!(claimed.pages.len(), 1);
    assert!(claimed.pages.contains(&second));
    assert!(!state.contains(second));
    assert!(resources.current() > 0);
    drop(claimed);
    assert_eq!(resources.current(), 0);
    // Empty takes/clear may be repeated without allocation or false claims.
    assert!(state.take_post_commit_allocations().pages.is_empty());
    state.clear();
    state.clear();
    assert_eq!(resources.current(), 0);
}

#[test]
fn native_owned_tracking_concurrent_final_release_retires_preallocated_storage() {
    use std::sync::Barrier;
    let quote = SharedUncommittedPages::capacity_bytes(3).unwrap();
    let resources = NativePageResources::for_fixed_write(quote, NativeNumericalRetainer::new(()));
    let tracked = SharedUncommittedPages::source(&resources, 3).unwrap();
    let last_left = tracked.clone();
    let last_right = tracked; // No root reference survives outside the workers.
    let arrived = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let left_arrived = arrived.clone();
    let left_release = release.clone();
    let left = std::thread::spawn(move || {
        left_arrived.wait();
        left_release.wait();
        drop(last_left);
    });
    let right_arrived = arrived.clone();
    let right_release = release.clone();
    let right = std::thread::spawn(move || {
        right_arrived.wait();
        right_release.wait();
        drop(last_right);
    });
    arrived.wait();
    assert_eq!(
        resources.current(),
        quote,
        "both last owners retain real preallocated arrays"
    );
    release.wait();
    left.join().unwrap();
    right.join().unwrap();
    assert_eq!(
        resources.current(),
        0,
        "final Arc shell and all buffers must retire before guards"
    );
}
