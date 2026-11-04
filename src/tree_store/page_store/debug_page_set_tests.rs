//! Genuine fresh native fixtures with explicit fixture-only demand. Numerical
//! unit marker only, not SourceReservation attachment or a complete write plan.
// req: STO-020, PRJ-004, REC-001
use super::super::{
    SharedPageBytes, container_bounds, lru_cache::LRUCache, page_resources::NativePageResources,
};
use super::*;
use crate::tree_store::InMemoryBackend;
use crate::{Database, StorageBackend, StorageError};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Barrier;

const ATTEMPTS: usize = 4;
const MUTABLE: usize = 2;
fn pool_limit() -> usize {
    // Four calls, each for one512 page, at most four current entries/queue
    // slots in any selected stripe. Eight concrete cache-owner quotes cover
    // old+replacement coexistence across those four fixture calls. These are
    // finite fixture demands, never the missing production recipe theorem.
    let allocated = container_bounds::vector_storage::<PageNumber>(ATTEMPTS)
        .unwrap()
        .allocator_bytes;
    let dirty = container_bounds::vector_storage::<PageNumber>(MUTABLE)
        .unwrap()
        .allocator_bytes;
    let cache =
        LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(ATTEMPTS + 1, ATTEMPTS + 1)
            .unwrap();
    let pages = NativePageResources::page_allocation_bound(512)
        .unwrap()
        .checked_mul(ATTEMPTS)
        .unwrap();
    let total = usize::try_from(allocated + dirty)
        .unwrap()
        .checked_add(
            usize::try_from(cache)
                .unwrap()
                .checked_mul(2 * ATTEMPTS)
                .unwrap(),
        )
        .unwrap()
        .checked_add(pages)
        .unwrap();
    total
}
fn pool() -> NativePageResources {
    NativePageResources::from_retained_pool_data(pool_limit(), ()).unwrap()
}

fn database() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let mut builder = Database::builder();
    builder.set_page_size(512).set_cache_size(1024);
    let db = builder.create(file.path()).unwrap();
    (file, db)
}
fn prepare(
    db: &Database,
    write: &crate::WriteTransaction,
    resources: &NativePageResources,
    new_pages: usize,
    mutable: usize,
) -> NativePreparedDebugFamilies {
    let mem = db.get_memory();
    let guard = write.transaction_guard();
    mem.prepare_source_debug_families(guard.tracker(), &guard, resources, new_pages, mutable)
        .unwrap()
}
fn ordinary(error: &StorageError) -> bool {
    matches!(error,StorageError::Io(x) if x.kind()==std::io::ErrorKind::OutOfMemory)
}

#[test]
fn native_prepared_debug_union_tracks_real_page_drop_free_and_reuse_without_refund() {
    let (_file, db) = database();
    let write = db.begin_write().unwrap();
    let mem = db.get_memory();
    let resources = pool();
    let proof = prepare(&db, &write, &resources, ATTEMPTS, MUTABLE);
    let guard = write.transaction_guard();
    proof
        .require_for(&mem, guard.tracker(), &guard, &resources)
        .unwrap();
    drop(guard);
    let base = mem.count_allocated_pages().unwrap();
    let held = resources.current();
    assert!(held > 0);
    let page = mem
        .allocate_helper_with_resources(512, true, Some(&resources))
        .unwrap();
    let first = page.get_page_number();
    assert!(Arc::ptr_eq(&page.open_pages, &mem.open_dirty_pages));
    assert!(mem.allocated_pages.lock().unwrap().contains(&first));
    assert!(mem.open_dirty_pages.lock().unwrap().contains(&first));
    drop(page);
    assert!(!mem.open_dirty_pages.lock().unwrap().contains(&first));
    assert!(resources.current() >= held);
    mem.free(first, &PageTracker::ignore());
    assert_eq!(
        resources.current(),
        held,
        "logical empty sets retain guarded physical capacity"
    );
    assert_eq!(mem.count_allocated_pages().unwrap(), base);
    let reused = mem
        .allocate_helper_with_resources(512, true, Some(&resources))
        .unwrap();
    let number = reused.get_page_number();
    drop(reused);
    // Use genuine lowest allocation; freed lowest fixture page must be reusable
    // without stale duplicate membership in either debug component.
    assert_eq!(number, first);
    assert!(mem.allocated_pages.lock().unwrap().contains(&number));
    mem.free(number, &PageTracker::ignore());
    drop(proof);
    write.abort().unwrap();
    let pressure = resources
        .admit_control(pool_limit().checked_sub(resources.current()).unwrap())
        .unwrap();
    let ordinary = db.begin_write().unwrap();
    ordinary.abort().unwrap();
    assert_eq!(resources.current(), pressure.reserved_bytes());
    drop(pressure);
    assert_eq!(resources.current(), 0);
}

#[test]
fn native_prepared_debug_both_capacity_refusals_precede_real_page_reservation() {
    for (new_pages, mutable) in [(0, 1), (1, 0)] {
        let (_file, db) = database();
        let write = db.begin_write().unwrap();
        let mem = db.get_memory();
        let resources = pool();
        let proof = prepare(&db, &write, &resources, new_pages, mutable);
        let before = mem.count_allocated_pages().unwrap();
        let held = resources.current();
        let result = mem.allocate_helper_with_resources(512, false, Some(&resources));
        assert!(matches!(result,Err(ref e) if ordinary(e)));
        drop(result);
        assert_eq!(mem.count_allocated_pages().unwrap(), before);
        assert_eq!(resources.current(), held);
        assert!(mem.open_dirty_pages.lock().unwrap().is_empty());
        drop(proof);
        write.abort().unwrap();
        let retry = db.begin_write().unwrap();
        retry.abort().unwrap();
        assert_eq!(resources.current(), 0);
    }
}

#[test]
fn native_prepared_debug_foreign_pool_refuses_before_native_page_mutation() {
    let (_file, db) = database();
    let write = db.begin_write().unwrap();
    let mem = db.get_memory();
    let resources = pool();
    let foreign = pool();
    let proof = prepare(&db, &write, &resources, ATTEMPTS, MUTABLE);
    let before = mem.count_allocated_pages().unwrap();
    let held = resources.current();
    let result = mem.allocate_helper_with_resources(512, false, Some(&foreign));
    assert!(matches!(result,Err(ref e) if ordinary(e)));
    drop(result);
    assert_eq!(mem.count_allocated_pages().unwrap(), before);
    assert_eq!(resources.current(), held);
    assert_eq!(foreign.current(), 0);
    let guard = write.transaction_guard();
    assert!(
        proof
            .require_for(&mem, guard.tracker(), &guard, &foreign)
            .is_err()
    );
    drop(guard);
    drop(proof);
    write.abort().unwrap();
    let retry = db.begin_write().unwrap();
    retry.abort().unwrap();
    assert_eq!(resources.current(), 0);
}

#[test]
fn native_prepared_debug_ordinary_retry_keeps_actual_live_membership_until_handoff() {
    let (_file, db) = database();
    let write = db.begin_write().unwrap();
    let mem = db.get_memory();
    let resources = pool();
    let proof = prepare(&db, &write, &resources, ATTEMPTS, MUTABLE);
    let page = mem
        .allocate_helper_with_resources(512, false, Some(&resources))
        .unwrap();
    let number = page.get_page_number();
    drop(page);
    drop(proof);
    // Direct primitive reservation remains this fixture's responsibility. It
    // is deliberately not published or added to the transaction's write set.
    write.abort().unwrap();
    let before = resources.current();
    mem.allocated_pages
        .lock()
        .unwrap()
        .fail_next_ordinary_reserve_for_test();
    let failed = db.begin_write();
    assert!(matches!(failed,Err(crate::TransactionError::Storage(ref e)) if ordinary(e)));
    drop(failed);
    assert!(!mem.storage.has_source_write_metadata());
    assert!(mem.allocated_pages.lock().unwrap().has_source_storage());
    assert!(mem.allocated_pages.lock().unwrap().contains(&number));
    assert!(
        resources.current() <= before && resources.current() > 0,
        "retired cache metadata may decrease; actual debug/payload backing remains charged"
    );
    // Retry is genuine Live writer acquisition, never manual slot release.
    let retry = db.begin_write().unwrap();
    assert!(mem.allocated_pages.lock().unwrap().contains(&number));
    assert!(!mem.allocated_pages.lock().unwrap().has_source_storage());
    assert!(!mem.open_dirty_pages.lock().unwrap().has_source_storage());
    assert_eq!(
        resources.current(),
        NativePageResources::page_allocation_bound(512).unwrap(),
        "ordinary metadata handoff cannot refund still-live Source payload"
    );
    mem.free(number, &PageTracker::ignore());
    assert_eq!(resources.current(), 0);
    retry.abort().unwrap();
}

#[derive(Debug)]
struct FailingBackend {
    inner: InMemoryBackend,
    fail: Arc<AtomicBool>,
    failed: Arc<AtomicUsize>,
}
impl StorageBackend for FailingBackend {
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, at: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.inner.read(at, out)
    }
    fn set_len(&self, n: u64) -> std::io::Result<()> {
        self.inner.set_len(n)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn write(&self, at: u64, data: &[u8]) -> std::io::Result<()> {
        if self.fail.load(Ordering::Acquire) {
            self.failed.fetch_add(1, Ordering::AcqRel);
            return Err(std::io::Error::other(
                "injected debug-owner writeback failure",
            ));
        }
        self.inner.write(at, data)
    }
}
#[test]
fn native_prepared_debug_writeback_error_preserves_old_union_and_actual_owners() {
    let fail = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicUsize::new(0));
    let mut builder = Database::builder();
    builder.set_page_size(512).set_cache_size(1024);
    let db = builder
        .create_with_backend(FailingBackend {
            inner: InMemoryBackend::new(),
            fail: fail.clone(),
            failed: failed.clone(),
        })
        .unwrap();
    let write = db.begin_write().unwrap();
    let mem = db.get_memory();
    let resources = pool();
    let proof = prepare(&db, &write, &resources, ATTEMPTS, MUTABLE);
    let seed = mem
        .allocate_helper_with_resources(512, false, Some(&resources))
        .unwrap();
    let number = seed.get_page_number();
    drop(seed);
    let before = mem.count_allocated_pages().unwrap();
    let held = resources.current();
    let members = mem.allocated_pages.lock().unwrap().len();
    fail.store(true, Ordering::Release);
    let result = mem.allocate_helper_with_resources(512, false, Some(&resources));
    assert!(matches!(result, Err(StorageError::Io(_))));
    drop(result);
    assert_eq!(failed.load(Ordering::Acquire), 1);
    assert!(mem.storage_failure());
    assert_eq!(
        mem.count_allocated_pages().unwrap(),
        before,
        "qualified never-returned-PN cleanup remains exact"
    );
    assert_eq!(mem.allocated_pages.lock().unwrap().len(), members);
    assert!(mem.allocated_pages.lock().unwrap().contains(&number));
    assert!(mem.open_dirty_pages.lock().unwrap().is_empty());
    assert_eq!(
        resources.current(),
        held,
        "failed unused owners retire, existing backing/payload remains charged"
    );
    mem.free(number, &PageTracker::ignore());
    drop(proof);
    drop(write);
    drop(mem);
    drop(db);
    assert_eq!(
        resources.current(),
        0,
        "all actual debug backings retire with native owner despite I/O latch"
    );
}

#[test]
fn native_debug_logical_clear_retains_backing_through_concurrent_final_arc_owners() {
    let resources = pool();
    let mut value = NativeDebugPageSet::default();
    value.prepare_source(ATTEMPTS, &resources).unwrap();
    let number = PageNumber {
        region: 0,
        page_index: 7,
        page_order: 0,
    };
    assert!(value.insert(number));
    let held = resources.current();
    value.clear();
    assert!(value.is_empty());
    assert_eq!(resources.current(), held);
    // Actual same debug-owner Arc representation. This separately establishes
    // Source Vec lifetime, not pricing the already-baseline Arc shell/Source T.
    let owner = Arc::new(Mutex::new(value));
    let left = owner.clone();
    let right = owner.clone();
    drop(owner);
    let ready = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for owner in [left, right] {
        let ready = ready.clone();
        let release = release.clone();
        workers.push(std::thread::spawn(move || {
            ready.wait();
            release.wait();
            drop(owner);
        }));
    }
    ready.wait();
    assert_eq!(resources.current(), held);
    release.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        resources.current(),
        0,
        "source Vec/control must physically retire with final real owner"
    );
}

#[test]
fn native_prepared_debug_partial_and_replacement_refusal_keep_actual_buffer_owners() {
    // First family installs; second family's whole admission refuses before
    // reserve. Actual first backing stays charged through error and abort.
    {
        let (_file, db) = database();
        let write = db.begin_write().unwrap();
        let mem = db.get_memory();
        let amount = usize::try_from(
            container_bounds::vector_storage::<PageNumber>(ATTEMPTS)
                .unwrap()
                .allocator_bytes,
        )
        .unwrap();
        let resources = NativePageResources::from_retained_pool_data(amount, ()).unwrap();
        let guard = write.transaction_guard();
        let result = mem.prepare_source_debug_families(
            guard.tracker(),
            &guard,
            &resources,
            ATTEMPTS,
            MUTABLE,
        );
        assert!(matches!(result,Err(ref e) if ordinary(e)));
        drop(result);
        drop(guard);
        assert_eq!(resources.current(), amount);
        assert_eq!(
            mem.allocated_pages
                .lock()
                .unwrap()
                .source_buffer_for_test()
                .0,
            ATTEMPTS
        );
        assert!(!mem.open_dirty_pages.lock().unwrap().has_source_storage());
        let before = mem.count_allocated_pages().unwrap();
        let incomplete = mem.allocate_helper_with_resources(512, false, Some(&resources));
        assert!(matches!(incomplete,Err(ref e) if ordinary(e)));
        drop(incomplete);
        assert_eq!(mem.count_allocated_pages().unwrap(), before);
        assert_eq!(resources.current(), amount);
        write.abort().unwrap();
        let retry = db.begin_write().unwrap();
        retry.abort().unwrap();
        assert_eq!(resources.current(), 0);
    }
    // A replacement requires old+new simultaneously. No refund of old backing
    // or mutation of its real address/capacity can manufacture that allowance.
    {
        let (_file, db) = database();
        let write = db.begin_write().unwrap();
        let mem = db.get_memory();
        let old = usize::try_from(
            container_bounds::vector_storage::<PageNumber>(2)
                .unwrap()
                .allocator_bytes
                + container_bounds::vector_storage::<PageNumber>(1)
                    .unwrap()
                    .allocator_bytes,
        )
        .unwrap();
        let resources = NativePageResources::from_retained_pool_data(old, ()).unwrap();
        let proof = prepare(&db, &write, &resources, 2, 1);
        let allocated = mem.allocated_pages.lock().unwrap().source_buffer_for_test();
        let dirty = mem
            .open_dirty_pages
            .lock()
            .unwrap()
            .source_buffer_for_test();
        let guard = write.transaction_guard();
        let result = mem.prepare_source_debug_families(
            guard.tracker(),
            &guard,
            &resources,
            ATTEMPTS,
            MUTABLE,
        );
        assert!(matches!(result,Err(ref e) if ordinary(e)));
        drop(result);
        assert_eq!(resources.current(), old);
        assert_eq!(
            mem.allocated_pages.lock().unwrap().source_buffer_for_test(),
            allocated
        );
        assert_eq!(
            mem.open_dirty_pages
                .lock()
                .unwrap()
                .source_buffer_for_test(),
            dirty
        );
        proof
            .require_for(&mem, guard.tracker(), &guard, &resources)
            .unwrap();
        drop(guard);
        drop(proof);
        write.abort().unwrap();
        let retry = db.begin_write().unwrap();
        retry.abort().unwrap();
        assert_eq!(resources.current(), 0);
    }
}
