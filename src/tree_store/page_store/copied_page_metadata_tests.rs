//! Private numerical allocation fixtures; no database/source authority is minted.
// req: STO-020, PRJ-004, REC-001
use super::super::base::{PageImpl, PageReadTracking};
use super::super::page_bytes::SharedPageBytes;
use super::*;
use crate::StorageError;
use core::sync::atomic::{AtomicUsize, Ordering};
const BASE: usize = 4096;
fn minimum() -> usize {
    NativePageResources::page_allocation_bound(BASE).unwrap()
}
fn page(
    metadata: &SharedCopiedPageMetadata,
    resources: &NativePageResources,
    index: u32,
) -> Result<PageImpl> {
    metadata.require_available()?;
    let number = PageNumber::new(0, index, 0);
    let mem = SharedPageBytes::zeroed(BASE, Some(resources))?;
    metadata.register(number)?;
    Ok(PageImpl {
        mem,
        page_number: number,
        read_tracking: PageReadTracking::Copied(metadata.clone()),
    })
}
fn oom<T>(result: &Result<T>) -> bool {
    matches!(result,Err(StorageError::Io(error)) if error.kind()==std::io::ErrorKind::OutOfMemory)
}
#[test]
fn native_copied_metadata_reserves_controls_before_creation_and_preserves_original_page_pool() {
    let physical = minimum().checked_mul(3).unwrap();
    let quote = SharedCopiedPageMetadata::capacity_bytes(physical, BASE).unwrap();
    let short = NativePageResources::for_test(quote - 1, Arc::new(()));
    assert!(oom(&SharedCopiedPageMetadata::new(&short, physical, BASE)));
    assert_eq!(
        short.current(),
        0,
        "failed pre-admission leaked metadata reservation"
    );
    let resources = NativePageResources::for_test(quote + physical, Arc::new(()));
    let metadata = SharedCopiedPageMetadata::new(&resources, physical, BASE).unwrap();
    assert_eq!(resources.current(), quote);
    assert_eq!(metadata.capacity_for_test(), (3, 3));
    let mut pages = Vec::new();
    for index in 0..3 {
        pages.push(page(&metadata, &resources, index).unwrap());
    }
    assert_eq!(
        resources.current(),
        quote + physical,
        "metadata took bytes from original physical pool"
    );
    assert!(oom(&page(&metadata, &resources, 3)));
    assert_eq!(resources.current(), quote + physical);
    drop(pages.pop());
    assert_eq!(resources.current(), quote + physical - minimum());
    pages.push(page(&metadata, &resources, 3).unwrap());
    assert_eq!(
        metadata.capacity_for_test(),
        (3, 3),
        "recycled slots grew metadata capacity"
    );
    drop(pages);
    assert_eq!(resources.current(), quote);
    drop(metadata);
    assert_eq!(
        resources.current(),
        0,
        "final metadata owner did not retire its whole control charge"
    );
}
struct Retainer(Arc<AtomicUsize>);
impl Drop for Retainer {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn native_copied_metadata_and_bytes_retain_one_counter_through_the_real_final_alias() {
    let retired = Arc::new(AtomicUsize::new(0));
    let physical = minimum() * 2;
    let quote = SharedCopiedPageMetadata::capacity_bytes(physical, BASE).unwrap();
    let resources =
        NativePageResources::for_test(quote + physical, Arc::new(Retainer(retired.clone())));
    let metadata = SharedCopiedPageMetadata::new(&resources, physical, BASE).unwrap();
    let first = page(&metadata, &resources, 0).unwrap();
    let last = first.clone();
    let bytes = first.to_arc();
    assert_eq!(metadata.references_for_test(first.page_number), 2);
    drop(first);
    assert_eq!(metadata.references_for_test(last.page_number), 1);
    drop(metadata);
    drop(resources);
    assert_eq!(retired.load(Ordering::SeqCst), 0);
    match &last.read_tracking {
        PageReadTracking::Copied(metadata) => {
            assert_eq!(metadata.references_for_test(last.page_number), 1)
        }
        PageReadTracking::Primary(_) => panic!("fresh copied page selected primary tracking"),
    }
    drop(last); // LAST metadata Arc retires before slots/control guard destruction.
    assert_eq!(
        retired.load(Ordering::SeqCst),
        0,
        "bare admitted bytes lost their counter owner"
    );
    drop(bytes); // Only this actual final payload/counter alias may release retainer.
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}
#[test]
fn native_copied_metadata_independent_context_owners_never_share_counts_or_grow_slots() {
    // Numerical suballocation owners only. Production must pre-admit BOTH from
    // its same originating Source ledger; these are not new Source budgets.
    let physical = minimum() * 2;
    let quote = SharedCopiedPageMetadata::capacity_bytes(physical, BASE).unwrap();
    let left = NativePageResources::for_test(quote + physical, Arc::new(()));
    let right = NativePageResources::for_test(quote + physical, Arc::new(()));
    let left_meta = SharedCopiedPageMetadata::new(&left, physical, BASE).unwrap();
    let right_meta = SharedCopiedPageMetadata::new(&right, physical, BASE).unwrap();
    let left_page = page(&left_meta, &left, 0).unwrap();
    let right_page = page(&right_meta, &right, 0).unwrap();
    let clone = left_page.clone();
    assert_eq!(left_meta.references_for_test(left_page.page_number), 2);
    assert_eq!(right_meta.references_for_test(right_page.page_number), 1);
    drop(left_page);
    assert_eq!(left_meta.references_for_test(clone.page_number), 1);
    let second = page(&left_meta, &left, 1).unwrap();
    assert!(oom(&page(&left_meta, &left, 2)));
    let right_second = page(&right_meta, &right, 1).unwrap();
    assert_eq!(left_meta.capacity_for_test(), (2, 2));
    assert_eq!(right_meta.capacity_for_test(), (2, 2));
    drop((clone, second, right_page, right_second));
    assert_eq!(left.current(), quote);
    assert_eq!(right.current(), quote);
    drop((left_meta, right_meta));
    assert_eq!(left.current(), 0);
    assert_eq!(right.current(), 0);
}
#[test]
fn native_copied_metadata_poison_refuses_admission_but_releases_existing_aliases() {
    let physical = minimum() * 2;
    let quote = SharedCopiedPageMetadata::capacity_bytes(physical, BASE).unwrap();
    let resources = NativePageResources::for_test(quote + physical, Arc::new(()));
    let metadata = SharedCopiedPageMetadata::new(&resources, physical, BASE).unwrap();
    let first = page(&metadata, &resources, 0).unwrap();
    let before = resources.current();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = metadata.get().slots.lock().unwrap();
            panic!("deterministic copied-metadata poison fixture");
        }))
        .is_err()
    );
    assert!(oom(&metadata.require_available()));
    assert!(oom(&page(&metadata, &resources, 1)));
    assert_eq!(resources.current(), before);
    let clone = first.clone(); // Existing immutable alias bookkeeping only.
    drop(first);
    assert!(
        oom(&metadata.require_available()),
        "cleanup must not clear poison"
    );
    drop(clone);
    assert_eq!(resources.current(), quote);
    drop(metadata);
    assert_eq!(resources.current(), 0);
}

#[test]
fn native_copied_metadata_concurrent_final_page_owners_release_one_actual_retainer() {
    let retired = Arc::new(AtomicUsize::new(0));
    let physical = minimum() * 2;
    let quote = SharedCopiedPageMetadata::capacity_bytes(physical, BASE).unwrap();
    let resources =
        NativePageResources::for_test(quote + physical, Arc::new(Retainer(retired.clone())));
    let metadata = SharedCopiedPageMetadata::new(&resources, physical, BASE).unwrap();
    let first = page(&metadata, &resources, 0).unwrap();
    let second = first.clone();
    drop(metadata);
    drop(resources);
    let arrived = Arc::new(std::sync::Barrier::new(3));
    let release = Arc::new(std::sync::Barrier::new(3));
    let spawn = |page: PageImpl| {
        let arrived = arrived.clone();
        let release = release.clone();
        std::thread::spawn(move || {
            arrived.wait();
            release.wait();
            drop(page);
        })
    };
    let left = spawn(first);
    let right = spawn(second);
    arrived.wait();
    let retained = retired.load(Ordering::SeqCst) == 0;
    release.wait(); // Always release the deterministic schedule BEFORE asserting.
    left.join().unwrap();
    right.join().unwrap();
    assert!(
        retained,
        "final metadata/page owners released their retainer while still live"
    );
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}
