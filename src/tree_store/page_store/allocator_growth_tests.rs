//! Actual allocator Vec capacities around the existing native resize operation.
// req: STO-020, PRJ-004
use super::super::container_bounds;
use super::*;

fn layout(pages: u64, capacity: u32) -> DatabaseLayout {
    DatabaseLayout::calculate(pages * 4096, capacity, 1, 4096)
}
fn observed_bytes(allocators: &Allocators) -> u64 {
    // All allocator buffers are Vec<BuddyAllocator>, Vec<BtreeBitmap>,
    // Vec<U64GroupedBitmap> or Vec<u64>; assert the actual common alignment.
    assert!(core::mem::align_of::<BuddyAllocator>() <= 8);
    assert!(core::mem::align_of::<BtreeBitmap>() <= 8);
    assert!(core::mem::align_of::<super::super::bitmap::U64GroupedBitmap>() <= 8);
    let actual = allocators.reopen_capacity_facts().unwrap();
    2 * actual.capacity_bytes() as u64 + 272 * actual.buffer_count() as u64
}

#[test]
fn native_allocator_growth_quote_covers_actual_lazy_bitmap_extension() {
    let initial = layout(64, 8192);
    let next = layout(4097, 8192);
    let mut allocators = Allocators::new(initial);
    let before = observed_bytes(&allocators);
    let quote = allocators.resize_quote(initial, next).unwrap();
    assert_eq!(
        observed_bytes(&allocators),
        before,
        "quoting cannot allocate or resize native owners"
    );
    allocators.resize_to(next);
    let after = observed_bytes(&allocators);
    assert!(
        after > before,
        "padded tree height did not preallocate all bitmap words"
    );
    assert!(quote.retained_after_bytes >= after);
    assert!(quote.simultaneous_peak_bytes >= quote.retained_after_bytes);
    assert!(quote.simultaneous_peak_bytes >= before);
    assert_eq!(allocators.region_allocators[0].len(), 4097);
    assert_eq!(allocators.region_allocators[0].count_free_pages(), 4097);
}

#[test]
fn native_allocator_growth_quote_covers_actual_new_regions_and_repeated_push_capacity() {
    let initial = layout(1, 64);
    let next = layout(1025 * 64, 64);
    let mut allocators = Allocators::new(initial);
    let old_capacity = allocators.region_allocators.capacity();
    let bulk =
        container_bounds::vector_amortized_growth::<BuddyAllocator>(old_capacity, 1, 1024).unwrap();
    let pushes = container_bounds::vector_pushes::<BuddyAllocator>(old_capacity, 1, 1024).unwrap();
    let quote = allocators.resize_quote(initial, next).unwrap();
    allocators.resize_to(next);
    let actual_outer =
        container_bounds::vector_storage::<BuddyAllocator>(allocators.region_allocators.capacity())
            .unwrap()
            .allocator_bytes;
    assert!(
        actual_outer > bulk.retained_after_bytes,
        "one bulk reserve is insufficient for the actual repeated-push owner"
    );
    assert_eq!(pushes.retained_after_bytes, actual_outer);
    assert!(quote.retained_after_bytes >= observed_bytes(&allocators));
    assert_eq!(allocators.region_allocators.len(), 1025);
    assert!(allocators.region_tracker.len() >= 1025);
    assert_eq!(
        allocators
            .region_allocators
            .last()
            .unwrap()
            .count_free_pages(),
        64
    );
}

#[test]
fn native_allocator_shrink_quote_preserves_actual_retained_bitmap_and_outer_capacity() {
    let initial = layout(4097, 8192);
    let smaller = layout(32, 8192);
    let mut allocators = Allocators::new(initial);
    let before = observed_bytes(&allocators);
    let quote = allocators.resize_quote(initial, smaller).unwrap();
    allocators.resize_to(smaller);
    assert_eq!(
        observed_bytes(&allocators),
        before,
        "logical page shrink does not free allocator Vec buffers"
    );
    assert_eq!(quote.retained_after_bytes, before);
    assert_eq!(allocators.region_allocators[0].len(), 32);
    assert_eq!(allocators.region_allocators[0].count_free_pages(), 32);
}

#[test]
fn native_allocator_growth_quote_refuses_stale_or_foreign_geometry_before_mutation() {
    let initial = layout(64, 8192);
    let allocators = Allocators::new(initial);
    let before = observed_bytes(&allocators);
    assert!(
        allocators
            .resize_quote(layout(63, 8192), layout(65, 8192))
            .is_err()
    );
    assert!(allocators.resize_quote(initial, layout(65, 4096)).is_err());
    assert!(
        allocators
            .resize_quote(initial, layout((u64::from(MAX_REGIONS) + 1) * 8192, 8192))
            .is_err()
    );
    assert_eq!(observed_bytes(&allocators), before);
    assert_eq!(allocators.region_allocators[0].len(), 64);
    assert_eq!(allocators.region_allocators[0].count_free_pages(), 64);
}

// Private cohesive successor controls only; no complete Source writer/factory
// or actual SourceReservation T is activated by this numerical native fixture.
// req: STO-020, PRJ-004
#[test]
fn native_allocator_prepared_backing_survives_native_resize_and_ordinary_handoff() {
    use super::super::page_resources::NativePageResources;
    let initial = layout(64, 8192);
    let next = layout(8193, 8192);
    let mut actual = Allocators::new(initial);
    let before_hash = actual.xxh3_hash();
    let quote = actual.source_resize_storage_quote(initial, next).unwrap();
    let limit = usize::try_from(quote.simultaneous_peak_bytes).unwrap();
    let resources = NativePageResources::from_retained_pool_data(limit, ()).unwrap();
    let prepared = actual
        .prepare_source_resize(initial, next, &resources)
        .unwrap();
    assert_eq!(actual.region_allocators.len(), 1);
    assert_eq!(actual.region_allocators[0].len(), 64);
    assert_eq!(
        actual.xxh3_hash(),
        before_hash,
        "preparation cannot change native allocator bits"
    );
    assert!(
        resources.current() > 0,
        "actual changed/new buffer owners must retain admission"
    );
    // No file I/O in this low-level algorithm control. Future real native
    // caller must retain this checked borrow through actual resize/sync.
    let claimed = prepared.claim(&mut actual, next).unwrap();
    claimed.apply();
    let expected = Allocators::new(next);
    assert_eq!(actual.xxh3_hash(), expected.xxh3_hash());
    assert_eq!(actual.region_allocators.len(), 2);
    assert_eq!(actual.region_allocators[0].count_free_pages(), 8192);
    assert_eq!(actual.region_allocators[1].count_free_pages(), 1);
    assert!(
        resources.current() > 0,
        "moving prepared child buffers cannot refund their live backing"
    );
    actual.handoff_ordinary().unwrap();
    assert_eq!(
        resources.current(),
        0,
        "all real Source arrays retire before ordinary ownership/refund"
    );
    assert_eq!(actual.xxh3_hash(), expected.xxh3_hash());
    let pressure = resources.admit_control(limit).unwrap();
    // The exact existing allocator algorithm now grows ordinary-owned arrays
    // with no Source capacity, not an injected synthetic ordinary replacement.
    actual.resize_to(layout(8194, 8192));
    assert_eq!(actual.region_allocators[1].count_free_pages(), 2);
    assert_eq!(resources.current(), limit);
    drop(actual);
    drop(pressure);
    assert_eq!(resources.current(), 0);
}

// req: STO-020, PRJ-004
#[test]
fn native_allocator_backing_refusal_preserves_native_bits_and_needs_no_source_cleanup_quota() {
    use super::super::page_resources::NativePageResources;
    let initial = layout(64, 8192);
    let next = layout(4097, 8192);
    let mut actual = Allocators::new(initial);
    let hash = actual.xxh3_hash();
    let bytes = observed_bytes(&actual);
    let resources = NativePageResources::from_retained_pool_data(0, ()).unwrap();
    assert!(
        actual
            .prepare_source_resize(initial, next, &resources)
            .is_err()
    );
    assert_eq!(actual.xxh3_hash(), hash);
    assert_eq!(actual.region_allocators[0].len(), 64);
    assert_eq!(observed_bytes(&actual), bytes);
    assert_eq!(resources.current(), 0);
    actual.handoff_ordinary().unwrap();
    actual.resize_to(next);
    assert_eq!(actual.region_allocators[0].count_free_pages(), 4097);
    assert_eq!(resources.current(), 0);
}

// req: STO-020, PRJ-004
#[test]
fn native_allocator_prepared_resize_refuses_different_actual_owner_before_apply() {
    use super::super::page_resources::NativePageResources;
    let initial = layout(64, 8192);
    let next = layout(4097, 8192);
    let mut actual = Allocators::new(initial);
    let mut other = Allocators::new(initial);
    let quote = actual.source_resize_storage_quote(initial, next).unwrap();
    let resources = NativePageResources::from_retained_pool_data(
        usize::try_from(quote.simultaneous_peak_bytes).unwrap(),
        (),
    )
    .unwrap();
    let prepared = actual
        .prepare_source_resize(initial, next, &resources)
        .unwrap();
    assert!(prepared.claim(&mut other, next).is_err());
    assert_eq!(actual.region_allocators[0].len(), 64);
    assert_eq!(other.region_allocators[0].len(), 64);
    assert!(
        resources.current() > 0,
        "failed claim cannot refund already installed physical backing"
    );
    actual.handoff_ordinary().unwrap();
    assert_eq!(resources.current(), 0);
}
