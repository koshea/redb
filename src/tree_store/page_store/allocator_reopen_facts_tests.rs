use super::{AllocatorCapacityFacts, AllocatorParseRecorder, AllocatorResourceError};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::layout::{DatabaseLayout, RegionLayout};
use crate::tree_store::page_store::region::{Allocators, RegionTracker};
use alloc::vec;

// req: STO-020, REC-003
#[test]
fn allocator_reopen_preserves_pre_resize_capacity_and_parser_scratch() {
    let bytes = BuddyAllocator::new(4096, 4096).to_vec();
    let tracker_bytes = RegionTracker::new(1000, 13).to_vec();
    let mut recorder = AllocatorParseRecorder::new();
    let first = BuddyAllocator::from_bytes_recording(&bytes, Some(&mut recorder));
    let second = BuddyAllocator::from_bytes_recording(&bytes, Some(&mut recorder));
    let region_tracker = RegionTracker::from_bytes_recording(&tracker_bytes, Some(&mut recorder));
    let region_allocators = vec![first, second];
    recorder.replace_vec::<BuddyAllocator>(0, region_allocators.capacity());
    let mut graph = Allocators::from_parts(region_tracker, region_allocators);
    let before = graph.reopen_capacity_facts().unwrap();
    let parse = recorder.finish().unwrap();
    assert_eq!(parse.retained(), before);
    assert!(parse.peak_capacity_bytes() > before.capacity_bytes());
    assert_eq!(parse.region_records(), 0); // Only the sole loader records row inventory.
    graph.resize_to(DatabaseLayout::new(
        1,
        RegionLayout::new(4096, 0, 4096),
        None,
    ));
    let after = graph.reopen_capacity_facts().unwrap();
    assert!(before.capacity_bytes() > after.capacity_bytes());
    assert_eq!(graph.region_allocators.len(), 1);
    let combined = parse.with_resize_after(after).unwrap();
    assert_eq!(combined.parsed(), before);
    assert_eq!(combined.final_retained(), after);
    assert!(combined.peak_capacity_bytes() >= before.capacity_bytes() + after.capacity_bytes());
}

// req: STO-020
#[test]
fn allocator_reopen_facts_refuse_capacity_arithmetic_overflow() {
    let mut recorder = AllocatorParseRecorder::new();
    recorder.replace_vec::<u64>(0, usize::MAX);
    assert_eq!(
        recorder.finish().unwrap_err(),
        AllocatorResourceError::Overflow
    );
    let left = AllocatorCapacityFacts::for_test(usize::MAX, 1);
    assert_eq!(
        left.checked_sum(AllocatorCapacityFacts::for_test(1, 1)),
        Err(AllocatorResourceError::Overflow)
    );
}

// req: STO-020
#[test]
fn allocator_reopen_tracks_old_and_new_buffers_without_retaining_scratch() {
    let mut recorder = AllocatorParseRecorder::new();
    recorder.replace_vec::<u64>(0, 4);
    recorder.replace_vec::<u64>(4, 8);
    recorder.replace_vec::<usize>(0, 16);
    recorder.replace_vec::<usize>(16, 0);
    let facts = recorder.finish().unwrap();
    assert_eq!(
        facts.retained().capacity_bytes(),
        8 * core::mem::size_of::<u64>()
    );
    assert_eq!(facts.retained().buffer_count(), 1);
    assert!(facts.peak_capacity_bytes() >= 12 * core::mem::size_of::<u64>());
    assert!(facts.peak_buffer_count() >= 2);
    assert!(facts.maximum_allocated_buffer_bytes() >= 16 * core::mem::size_of::<usize>());
}
