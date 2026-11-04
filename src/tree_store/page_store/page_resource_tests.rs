//! Component tests: actual native page aliases, not complete reopen/write plans.
use super::super::page_resources::NativePageResources;
use super::*;
use crate::io;
use core::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CountingBackend(Arc<AtomicUsize>);
impl StorageBackend for CountingBackend {
    fn len(&self) -> core::result::Result<u64, io::Error> {
        Ok(8192)
    }
    fn read(&self, _: u64, bytes: &mut [u8]) -> core::result::Result<(), io::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        bytes.fill(7);
        Ok(())
    }
    fn set_len(&self, _: u64) -> core::result::Result<(), io::Error> {
        Err(io::invalid_input("read-only test"))
    }
    fn write(&self, _: u64, _: &[u8]) -> core::result::Result<(), io::Error> {
        Err(io::invalid_input("read-only test"))
    }
    fn sync_data(&self) -> core::result::Result<(), io::Error> {
        Err(io::invalid_input("read-only test"))
    }
}

// req: STO-020, PRJ-004
#[test]
fn native_page_charge_survives_cache_eviction_and_last_physical_alias() {
    let retainer = Arc::new(17_u64);
    let weak = Arc::downgrade(&retainer);
    let bytes = NativePageResources::page_allocation_bound(4096).unwrap();
    let resources = NativePageResources::for_test(bytes, retainer);
    let reads = Arc::new(AtomicUsize::new(0));
    let cache = PagedCachedFile::with_page_resources_for_test(
        Box::new(CountingBackend(reads.clone())),
        4096,
        4096,
        resources.clone(),
    );
    let page = cache.read(0, 4096, PageHint::Clean).unwrap();
    let alias = page.clone();
    assert_eq!(&*page, &[7; 4096]);
    assert_eq!(resources.current_for_test(), bytes);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    cache.invalidate_cache_all();
    assert_eq!(resources.current_for_test(), bytes);
    drop(cache);
    drop(resources);
    drop(page);
    assert!(weak.upgrade().is_some());
    assert_eq!(&*alias, &[7; 4096]);
    drop(alias);
    assert!(weak.upgrade().is_none());
}

// req: STO-020, PRJ-004
#[test]
fn native_page_refuses_before_payload_read_and_releases_after_last_alias() {
    let bytes = NativePageResources::page_allocation_bound(4096).unwrap();
    let resources = NativePageResources::for_test(bytes, Arc::new(()));
    let reads = Arc::new(AtomicUsize::new(0));
    let cache = PagedCachedFile::with_page_resources_for_test(
        Box::new(CountingBackend(reads.clone())),
        4096,
        4096,
        resources.clone(),
    );
    let page = cache.read(0, 4096, PageHint::Clean).unwrap();
    cache.invalidate_cache_all();
    assert!(cache.read(4096, 4096, PageHint::Clean).is_err());
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(resources.current_for_test(), bytes);
    drop(page);
    assert_eq!(resources.current_for_test(), 0);
    assert!(cache.read(4096, 4096, PageHint::Clean).is_ok());
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    cache.invalidate_cache_all();
    assert_eq!(resources.current_for_test(), 0);
    assert!(NativePageResources::page_allocation_bound(usize::MAX).is_err());
}

// req: STO-020, PRJ-004
#[test]
fn native_page_last_capacity_competition_has_one_actual_owner() {
    use std::sync::Barrier;
    let bytes = NativePageResources::page_allocation_bound(4096).unwrap();
    let resources = NativePageResources::for_test(bytes, Arc::new(()));
    let reads = Arc::new(AtomicUsize::new(0));
    let cache: Arc<PagedCachedFile> = Arc::new(PagedCachedFile::with_page_resources_for_test(
        Box::new(CountingBackend(reads.clone())),
        4096,
        4096,
        resources.clone(),
    ));
    let begin = Arc::new(Barrier::new(3));
    let hold = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for offset in [0, 4096] {
        let cache = cache.clone();
        let begin = begin.clone();
        let hold = hold.clone();
        threads.push(std::thread::spawn(move || {
            begin.wait();
            let result = cache.read(offset, 4096, PageHint::Clean);
            let acquired = result.is_ok();
            hold.wait();
            drop(result);
            acquired
        }));
    }
    begin.wait();
    hold.wait();
    let successes = threads
        .into_iter()
        .map(|thread| usize::from(thread.join().unwrap()))
        .sum::<usize>();
    assert_eq!(successes, 1);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    cache.invalidate_cache_all();
    assert_eq!(resources.current_for_test(), 0);
}

// req: STO-020, PRJ-004
#[test]
fn native_page_concurrent_final_aliases_release_one_charge_after_payload_retirement() {
    use std::sync::Barrier;
    let bytes = NativePageResources::page_allocation_bound(4096).unwrap();
    let resources = NativePageResources::for_test(bytes, Arc::new(()));
    let reads = Arc::new(AtomicUsize::new(0));
    let cache = PagedCachedFile::with_page_resources_for_test(
        Box::new(CountingBackend(reads.clone())),
        4096,
        4096,
        resources.clone(),
    );
    let page = cache.read(0, 4096, PageHint::Clean).unwrap();
    let first = page.clone();
    let second = page.clone();
    cache.invalidate_cache_all();
    drop(cache);
    drop(page);
    let ready = Barrier::new(3);
    let release = Barrier::new(3);
    std::thread::scope(|scope| {
        for alias in [first, second] {
            let ready = &ready;
            let release = &release;
            scope.spawn(move || {
                assert_eq!(&*alias, &[7; 4096]);
                ready.wait();
                release.wait();
                drop(alias);
            });
        }
        ready.wait();
        assert_eq!(resources.current_for_test(), bytes);
        assert!(resources.admit_page(4096).is_err());
        release.wait();
    });
    assert_eq!(resources.current_for_test(), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    let next = resources.admit_page(4096).unwrap();
    assert_eq!(resources.current_for_test(), bytes);
    drop(next);
    assert_eq!(resources.current_for_test(), 0);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_write_refusal_releases_pending_buffer_bytes_and_preserves_reads() {
    let bound = NativePageResources::page_allocation_bound(4096).unwrap();
    let resources = NativePageResources::for_test(bound, Arc::new(()));
    let reads = Arc::new(AtomicUsize::new(0));
    let cache = PagedCachedFile::with_page_resources_for_test(
        Box::new(CountingBackend(reads.clone())),
        4096,
        8192,
        resources.clone(),
    );
    let original = cache.read(0, 4096, PageHint::Clean).unwrap();
    assert!(cache.write(4096, 4096, true).is_err());
    assert_eq!(resources.current_for_test(), bound);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.write_buffer_bytes.load(Ordering::Acquire),
        0,
        "refused payload must not leave a phantom buffered write"
    );
    assert_eq!(cache.read_cache_bytes.load(Ordering::Acquire), 4096);
    assert!(
        cache
            .write_buffer
            .iter()
            .all(|stripe| stripe.lock().unwrap().cache.len() == 0)
    );
    assert_eq!(&*cache.read(0, 4096, PageHint::Clean).unwrap(), &[7; 4096]);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    cache.discard_write_buffer();
    assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 0);
    assert!(cache.write(4096, 4096, true).is_err());
    assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 0);
    drop(original);
    cache.invalidate_cache_all();
    assert_eq!(resources.current_for_test(), 0);
    let mut write = cache.write(4096, 4096, true).unwrap();
    assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 4096);
    write.mem_mut()[0] = 42;
    drop(write);
    assert_eq!(cache.read(4096, 4096, PageHint::None).unwrap()[0], 42);
    cache.discard_write_buffer();
    assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 0);
    assert_eq!(resources.current_for_test(), 0);
}

// req: STO-020, PRJ-004
#[test]
fn native_counter_nonunique_drain_preserves_retainer_until_real_final_owner() {
    struct FinalRetainer(Arc<AtomicUsize>);
    impl Drop for FinalRetainer {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let releases = Arc::new(AtomicUsize::new(0));
    let resource = NativePageResources::for_test(4096, Arc::new(FinalRetainer(releases.clone())));
    let alias = resource.clone();
    assert!(resource.try_into_drained().is_err());
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    alias.try_into_drained().unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

// req: STO-020, PRJ-004
#[test]
fn native_counter_concurrent_final_owners_destroy_one_retainer_after_page_release() {
    use std::sync::Barrier;
    struct FinalRetainer(Arc<AtomicUsize>);
    impl Drop for FinalRetainer {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let releases = Arc::new(AtomicUsize::new(0));
    let bound = NativePageResources::page_allocation_bound(4096).unwrap();
    let resource = NativePageResources::for_test(bound, Arc::new(FinalRetainer(releases.clone())));
    let page_owner = resource.admit_page(4096).unwrap();
    let first = resource.clone();
    let second = resource.clone();
    drop(resource);
    let ready = Barrier::new(3);
    let release = Barrier::new(3);
    std::thread::scope(|scope| {
        for owner in [first, second] {
            let ready = &ready;
            let release = &release;
            scope.spawn(move || {
                ready.wait();
                release.wait();
                drop(owner);
            });
        }
        ready.wait();
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        release.wait();
    });
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    drop(page_owner);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

// req: STO-020, PRJ-004
#[test]
fn native_source_write_flush_retires_real_stripe_capacity_without_read_cache_promotion() {
    use crate::backends::InMemoryBackend;
    let backend = InMemoryBackend::new();
    backend.set_len(131 * 4096).unwrap();
    let cache = PagedCachedFile::new(Box::new(backend), 4096, 12 * 1024 * 1024);
    let bound = NativePageResources::page_allocation_bound(4096).unwrap();
    // This unchanged fixture has one resident write entry and flushes before
    // its next Source insertion. Price its actual fresh map/queue separately
    // from the original payload; no production demand theorem is implied.
    let metadata = usize::try_from(
        super::super::lru_cache::LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(1, 1)
            .unwrap(),
    )
    .unwrap();
    let resources = NativePageResources::for_fixed_write(
        bound + metadata,
        super::super::page_resources::NativeNumericalRetainer::new(()),
    );
    // The payload guard carries its original byte length, independent of the
    // numerical bound formula and supported native power-of-two page profile.
    let mismatched = resources.admit_page(4096).unwrap();
    assert!(SharedPageBytes::zeroed_admitted(2048, mismatched).is_err());
    assert_eq!(resources.current_for_test(), 0);
    for index in 0_u64..131 {
        let offset = index * 4096;
        let mut page = cache
            .write_with_admission(
                offset,
                4096,
                true,
                Some(resources.admit_page(4096).unwrap()),
            )
            .unwrap();
        page.mem_mut().fill(index as u8);
        drop(page);
        assert!(
            cache
                .write_buffer_stripe(offset)
                .lock()
                .unwrap()
                .cache
                .resource_capacity_bytes()
                .unwrap()
                > 0
        );
        cache.flush().unwrap();
        assert_eq!(
            resources.current_for_test(),
            0,
            "source_write_no_promotion_after_real_flush"
        );
        assert_eq!(cache.read_cache_bytes.load(Ordering::Acquire), 0);
        assert!(cache.write_buffer.iter().all(|stripe| {
            stripe
                .lock()
                .unwrap()
                .cache
                .resource_capacity_bytes()
                .unwrap()
                == 0
        }));
    }
    let ordinary = cache.read(0, 4096, PageHint::Clean).unwrap();
    assert_eq!(ordinary[0], 0);
    assert_eq!(resources.current_for_test(), 0);
    assert!(cache.read_cache_bytes.load(Ordering::Acquire) > 0);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_source_committed_buffer_alias_remains_charged_without_promotion_or_ordinary_quota() {
    use crate::backends::InMemoryBackend;
    let backend = InMemoryBackend::new();
    backend.set_len(8192).unwrap();
    let cache = PagedCachedFile::new(Box::new(backend), 4096, 12 * 1024 * 1024);
    let bound = NativePageResources::page_allocation_bound(4096).unwrap();
    // This unchanged fixture has one resident write entry and flushes before
    // its next Source insertion. Price its actual fresh map/queue separately
    // from the original payload; no production demand theorem is implied.
    let metadata = usize::try_from(
        super::super::lru_cache::LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(1, 1)
            .unwrap(),
    )
    .unwrap();
    let resources = NativePageResources::for_fixed_write(
        bound + metadata,
        super::super::page_resources::NativeNumericalRetainer::new(()),
    );
    let mut page = cache
        .write_with_admission(0, 4096, true, Some(resources.admit_page(4096).unwrap()))
        .unwrap();
    page.mem_mut()[0] = 81;
    drop(page);
    let foreign = NativePageResources::for_fixed_write(
        bound + metadata,
        super::super::page_resources::NativeNumericalRetainer::new(()),
    );
    assert!(
        cache
            .write_with_admission(0, 4096, false, Some(foreign.admit_page(4096).unwrap()))
            .is_err()
    );
    assert_eq!(foreign.current_for_test(), 0);
    assert_eq!(resources.current_for_test(), bound + metadata);
    cache.write_barrier();
    let alias = cache.read(0, 4096, PageHint::Clean).unwrap();
    assert_eq!(alias[0], 81);
    assert_eq!(cache.read_cache_bytes.load(Ordering::Acquire), 0);
    assert_eq!(resources.current_for_test(), bound + metadata);
    // Saturated Source page counter cannot prevent an ordinary native write.
    let mut ordinary = cache.write(4096, 4096, true).unwrap();
    ordinary.mem_mut()[0] = 17;
    drop(ordinary);
    cache.flush().unwrap();
    assert_eq!(resources.current_for_test(), bound);
    assert_eq!(
        cache
            .write_buffer_stripe(0)
            .lock()
            .unwrap()
            .cache
            .resource_capacity_bytes()
            .unwrap(),
        0
    );
    // Ordinary stripe policy is unchanged; its empty native capacity may remain.
    assert!(
        cache
            .write_buffer_stripe(4096)
            .lock()
            .unwrap()
            .cache
            .resource_capacity_bytes()
            .unwrap()
            > 0
    );
    assert_eq!(cache.read(0, 4096, PageHint::Clean).unwrap()[0], 81);
    drop(alias);
    assert_eq!(resources.current_for_test(), 0);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_source_write_rollback_retires_actual_empty_source_touched_containers() {
    use crate::backends::InMemoryBackend;
    let backend = InMemoryBackend::new();
    backend.set_len(4096).unwrap();
    let cache = PagedCachedFile::new(Box::new(backend), 4096, 12 * 1024 * 1024);
    let bound = NativePageResources::page_allocation_bound(4096).unwrap();
    // This unchanged fixture has one resident write entry and flushes before
    // its next Source insertion. Price its actual fresh map/queue separately
    // from the original payload; no production demand theorem is implied.
    let metadata = usize::try_from(
        super::super::lru_cache::LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(1, 1)
            .unwrap(),
    )
    .unwrap();
    let resources = NativePageResources::for_fixed_write(
        bound + metadata,
        super::super::page_resources::NativeNumericalRetainer::new(()),
    );
    let page = cache
        .write_with_admission(0, 4096, true, Some(resources.admit_page(4096).unwrap()))
        .unwrap();
    drop(page);
    assert_eq!(resources.current_for_test(), bound + metadata);
    cache.discard_write_buffer();
    assert_eq!(resources.current_for_test(), 0);
    assert!(cache.write_buffer.iter().all(|stripe| {
        stripe
            .lock()
            .unwrap()
            .cache
            .resource_capacity_bytes()
            .unwrap()
            == 0
    }));
    let page = cache
        .write_with_admission(0, 4096, true, Some(resources.admit_page(4096).unwrap()))
        .unwrap();
    drop(page);
    cache.cancel_pending_write(0, 4096);
    assert_eq!(resources.current_for_test(), 0);
    assert_eq!(
        cache
            .write_buffer_stripe(0)
            .lock()
            .unwrap()
            .cache
            .resource_capacity_bytes()
            .unwrap(),
        0
    );
}

// req: STO-020, PRJ-004
#[test]
fn native_numeric_carrier_preserves_data_only_owner_until_last_physical_alias() {
    use super::super::page_resources::NativeNumericalRetainer;
    struct NumericalMarker(Arc<AtomicUsize>);
    impl Drop for NumericalMarker {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let releases = Arc::new(AtomicUsize::new(0));
    let bytes = NativePageResources::page_allocation_bound(4096).unwrap();
    let resource = NativePageResources::for_fixed_write(
        bytes,
        NativeNumericalRetainer::new(NumericalMarker(releases.clone())),
    );
    let page = resource.admit_page(4096).unwrap();
    let alias = resource.clone();
    drop(resource);
    assert!(
        alias.try_into_drained().is_err(),
        "physical page is still admitted"
    );
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    drop(page);
    assert_eq!(
        releases.load(Ordering::SeqCst),
        1,
        "final counter retires one numerical owner"
    );
}

// req: STO-020, PRJ-004
#[test]
fn native_numeric_carrier_quote_prices_actual_incoming_layout_before_construction() {
    use super::super::page_resources::NativeNumericalRetainer;
    #[repr(align(64))]
    struct LargeNumericalValue([u8; 8192]);
    let unit = NativePageResources::fixed_write_control_capacity_bytes::<()>().unwrap();
    let actual =
        NativePageResources::fixed_write_control_capacity_bytes::<LargeNumericalValue>().unwrap();
    let incoming = super::super::container_bounds::allocation(
        core::mem::size_of::<Option<LargeNumericalValue>>(),
        core::mem::align_of::<Option<LargeNumericalValue>>(),
    )
    .unwrap()
    .allocator_bytes;
    assert_eq!(
        actual - unit,
        incoming
            - super::super::container_bounds::allocation(
                core::mem::size_of::<Option<()>>(),
                core::mem::align_of::<Option<()>>(),
            )
            .unwrap()
            .allocator_bytes
    );
    assert!(
        actual > unit + 16 * 1024,
        "unit quote cannot stand in for concrete owner"
    );
    let value = LargeNumericalValue([7; 8192]);
    assert_eq!(value.0[8191], 7);
    let owner = NativeNumericalRetainer::new(value);
    drop(owner);
}
