use super::{
    SharedPageBytes,
    page_resources::{NativePageAllocation, NativePageResources},
};
use crate::db::StorageBackend;
use crate::sync::{Mutex, MutexGuard, RwLock};
use crate::transaction_tracker::TransactionId;
use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::lru_cache::LRUCache;
use crate::{CacheStats, Result, StorageError};
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::ops::{Index, IndexMut, RangeBounds};
use core::slice::SliceIndex;
#[cfg(feature = "cache_metrics")]
use core::sync::atomic::AtomicU64;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(super) struct WritablePage {
    buffer: Arc<Mutex<LRUWriteCache>>,
    offset: u64,
    data: SharedPageBytes,
}

impl WritablePage {
    pub(super) fn mem(&self) -> &[u8] {
        &self.data
    }

    pub(super) fn mem_mut(&mut self) -> &mut [u8] {
        self.data.get_mut().unwrap()
    }
}

impl Drop for WritablePage {
    fn drop(&mut self) {
        self.buffer
            .lock()
            .unwrap()
            .return_value(self.offset, self.data.clone());
    }
}

impl<I: SliceIndex<[u8]>> Index<I> for WritablePage {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.mem().index(index)
    }
}

impl<I: SliceIndex<[u8]>> IndexMut<I> for WritablePage {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.mem_mut().index_mut(index)
    }
}

#[derive(Default)]
struct LRUWriteCache {
    source_touched: bool,
    preparing_write: bool,
    cache: LRUCache<Option<SharedPageBytes>>,
}

// A single bounded fresh owner, not one allocation per stripe. Native writer
// custody must remain held from preparation through consumption. This owner is
// private and grants no writer, page, Source, or transaction authority.
pub(super) struct PreparedWriteMetadata<'cache> {
    owner: &'cache PagedCachedFile,
    #[cfg(not(redb_no_std))]
    replacement: Option<LRUCache<Option<SharedPageBytes>>>,
}

// If eviction empties the selected stripe before its pending insert, retain
// the prepared storage until that insert or error cleanup. Otherwise an empty
// retirement could discard its allowance and leave the insertion unpriced.
struct PreparedWriteLock<'cache> {
    inner: MutexGuard<'cache, LRUWriteCache>,
}
impl<'cache> core::ops::Deref for PreparedWriteLock<'cache> {
    type Target = MutexGuard<'cache, LRUWriteCache>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl core::ops::DerefMut for PreparedWriteLock<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl Drop for PreparedWriteLock<'_> {
    fn drop(&mut self) {
        if self.inner.preparing_write {
            self.inner.preparing_write = false;
            self.inner.retire_empty_source_capacity();
        }
    }
}

// write() accounts for a prospective buffer before fallible flushing, reads or
// allocation. Until the actual entry is installed, every early return must
// release that prospective amount. Existing entries retain their own bytes.
struct PendingWriteBufferBytes<'cache> {
    counter: &'cache AtomicUsize,
    bytes: usize,
    installed: bool,
}
impl PendingWriteBufferBytes<'_> {
    fn installed(mut self) {
        self.installed = true;
    }
}
impl Drop for PendingWriteBufferBytes<'_> {
    fn drop(&mut self) {
        if !self.installed {
            self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

impl LRUWriteCache {
    fn new() -> Self {
        Self {
            source_touched: false,
            preparing_write: false,
            cache: LRUCache::default(),
        }
    }

    fn insert(&mut self, key: u64, value: SharedPageBytes) {
        self.source_touched |= !value.allows_global_cache();
        assert!(self.cache.insert(key, Some(value)).is_none());
    }

    fn get(&self, key: u64) -> Option<&SharedPageBytes> {
        self.cache.get(key).map(|x| x.as_ref().unwrap())
    }

    fn remove(&mut self, key: u64) -> Option<SharedPageBytes> {
        if let Some(value) = self.cache.remove(key) {
            assert!(value.is_some());
            self.retire_empty_source_capacity();
            return value;
        }
        None
    }

    fn return_value(&mut self, key: u64, value: SharedPageBytes) {
        assert!(self.cache.get_mut(key).unwrap().replace(value).is_none());
    }

    fn take_value(&mut self, key: u64) -> Option<SharedPageBytes> {
        if let Some(value) = self.cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        None
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, SharedPageBytes)> {
        for _ in 0..self.cache.len() {
            if let Some((k, v)) = self.cache.pop_lowest_priority() {
                if let Some(v_inner) = v {
                    self.retire_empty_source_capacity();
                    return Some((k, v_inner));
                }

                // Value is borrowed by take_value(). We can't evict it, so put it back.
                self.cache.insert(k, v);
            } else {
                break;
            }
        }
        None
    }

    fn retire_empty_source_capacity(&mut self) {
        // None entries represent actual outstanding WritablePages and still
        // contribute to len. An actually empty map has no such mutable alias.
        if !self.preparing_write
            && (self.source_touched || self.cache.has_source_storage())
            && self.cache.len() == 0
        {
            self.cache = LRUCache::new();
            self.source_touched = false;
        }
    }

    fn clear(&mut self) {
        if self.source_touched || self.cache.has_source_storage() {
            // Callers first consume/drop EVERY returned value under this stripe
            // mutex. Replacing the owner destroys actual bucket/deque capacity;
            // logical clear/shrink-before-clear is not physical retirement.
            self.cache = LRUCache::new();
            self.source_touched = false;
        } else {
            self.cache.clear();
        }
    }
}

#[derive(Debug)]
struct CheckedBackend {
    file: Box<dyn StorageBackend>,
    io_failed: AtomicBool,
    closed: AtomicBool,
}

// Covers the open paths that fail before there is a Database to drop. Drop cannot report the
// error, but the open is already failing.
impl Drop for CheckedBackend {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            let _ = self.file.close();
        }
    }
}

impl CheckedBackend {
    fn new(file: Box<dyn StorageBackend>) -> Self {
        Self {
            file,
            io_failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn try_lock_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        self.check_failure()?;
        self.file
            .try_lock_range(range.start_bound().cloned(), range.end_bound().cloned())
            .map_err(StorageError::from)
    }

    fn try_lock_shared_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        self.check_failure()?;
        self.file
            .try_lock_shared_range(range.start_bound().cloned(), range.end_bound().cloned())
            .map_err(StorageError::from)
    }

    fn lock_range(&self, range: impl RangeBounds<u64>) -> Result {
        self.check_failure()?;
        self.file
            .lock_range(range.start_bound().cloned(), range.end_bound().cloned())
            .map_err(StorageError::from)
    }

    fn lock_shared_range(&self, range: impl RangeBounds<u64>) -> Result {
        self.check_failure()?;
        self.file
            .lock_shared_range(range.start_bound().cloned(), range.end_bound().cloned())
            .map_err(StorageError::from)
    }

    fn unlock_range(&self, range: impl RangeBounds<u64>) -> Result {
        // Allow unlocking even if there was an io failure, but not if the file is closed
        if self.closed.load(Ordering::Acquire) {
            return Err(StorageError::DatabaseClosed);
        }
        self.file
            .unlock_range(range.start_bound().cloned(), range.end_bound().cloned())
            .map_err(StorageError::from)
    }

    fn query_lock_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        self.check_failure()?;
        self.file
            .query_lock_range(range.start_bound().cloned(), range.end_bound().cloned())
            .map_err(StorageError::from)
    }

    fn check_failure(&self) -> Result<()> {
        if self.io_failed.load(Ordering::Acquire) {
            if self.closed.load(Ordering::Acquire) {
                Err(StorageError::DatabaseClosed)
            } else {
                Err(StorageError::PreviousIo)
            }
        } else {
            Ok(())
        }
    }

    fn close(&self) -> Result {
        self.closed.store(true, Ordering::Release);
        self.io_failed.store(true, Ordering::Release);
        self.file.close()?;

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        self.check_failure()?;
        let result = self.file.len();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.read(offset, out);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.check_failure()?;
        let result = self.file.set_len(len);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn sync_data(&self) -> Result<()> {
        self.check_failure()?;
        let result = self.file.sync_data();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.write(offset, data);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    // Like write(), but a failure does not permanently fail the backend. For a write that only an
    // optimization depends on, latching would turn every later operation into a PreviousIo error
    // over data that nothing was waiting on.
    fn write_best_effort(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        self.file.write(offset, data).map_err(StorageError::from)
    }
}

// Whether the data written by a write buffer flush has to reach the file. Either way a page whose
// write fails stays buffered; they differ in what a failure does to the backend.
#[derive(Copy, Clone)]
enum Writeback {
    // A failure is propagated and permanently fails the backend
    Required,
    // A failure leaves the backend usable
    BestEffort,
}

pub(super) struct PagedCachedFile {
    file: CheckedBackend,
    page_size: u64,
    // Dynamic cache partitioning.  Three invariants:
    //
    // 1. The write buffer is held at or below 50% of max_cache_size.
    //    Pages beyond this limit are flushed to disk immediately, best
    //    effort -- see the eviction in write().
    // 2. The write buffer evicts from the read cache only when
    //    write < 50% AND read > 50% (fairness).
    // 3. write + read never exceeds max_cache_size.
    //
    // Together these guarantee that the read cache can grow up to 100% when no
    // writes are in progress, while write-heavy workloads never starve readers
    // below 50%.
    //
    // Non-durable commits leave their pages in the write buffer (see write_barrier()), so it is
    // not necessarily empty between transactions. Invariant 1 bounds what they accumulate, and
    // read() flushes them out under pressure, so they cannot hold the read cache below 100%.
    //
    // We track usage with two atomic counters and compute the total on the fly.
    // The resulting read is not perfectly atomic (between loading the two
    // counters a concurrent operation could change one), but the budget is a
    // soft limit and momentary over-/under-counting by one page is harmless.
    // A third "total" counter would add contention on every insert/remove for
    // negligible accuracy gain.
    read_cache_bytes: AtomicUsize,
    read_cache_transaction_id: Mutex<Option<TransactionId>>,
    write_buffer_bytes: AtomicUsize,
    // Sticky until the next ordinary writer performs its pre-namespace handoff.
    // This is a fast-path hint, never ownership/admission or a live byte census.
    source_write_metadata_seen: AtomicBool,
    // True when the write buffer holds committed, reader-visible pages, left there by a
    // non-durable commit (see write_barrier()) instead of being written to the file. While set,
    // PageHint::Clean reads must consult the buffer, since a clean page may live there rather
    // than in the file.
    //
    // The buffer also holds the in-progress write transaction's uncommitted pages, but readers
    // cannot observe them: pages are copy-on-write, and a freed offset is only reallocated once
    // no live read transaction can reference it.
    committed_pages_buffered: AtomicBool,
    max_cache_size: usize,
    page_resources: Option<NativePageResources>,
    readonly_no_cache: bool,
    // Rotates the starting stripe for read-cache eviction
    next_eviction_stripe: AtomicUsize,
    #[cfg(feature = "cache_metrics")]
    reads_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    reads_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    evictions: AtomicU64,
    read_cache: Vec<RwLock<LRUCache<SharedPageBytes>>>,
    // Striped by page offset, like read_cache, so that concurrent writers
    // (tables of one WriteTransaction used from different threads) do not
    // contend on a single lock. Each stripe is individually Arc'd because
    // outstanding WritablePages return their buffer to the stripe on drop.
    write_buffer: Vec<Arc<Mutex<LRUWriteCache>>>,
}

// This internal component admits only the cache's fixed graph and returned
// read buffers. The incoming backend Box/locks, original pool bootstrap, file
// header/parser and errors are NOT admitted by this constructor. It grants no
// Source/database authority and is not a cold maintenance/reopen entry.
#[cfg(not(redb_no_std))]
pub(super) struct NativeSourceReadCachedFile {
    state: super::owned_shared::NativeOwnedShared<SourceReadCacheState>,
}
#[cfg(not(redb_no_std))]
struct SourceReadCacheState {
    cache: PagedCachedFile,
    #[cfg(test)]
    _retirement: Option<SourceReadCacheRetirement>,
    admission: super::page_resources::NativeFixedObservationAdmission,
}
#[cfg(not(redb_no_std))]
impl Clone for NativeSourceReadCachedFile {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}
#[cfg(not(redb_no_std))]
impl NativeSourceReadCachedFile {
    fn extra_retained_capacity_bytes() -> Result<usize> {
        use super::container_bounds::vector_storage;
        use super::page_resources::refused;
        use super::read_only_resource::ReadOnlyResourceProfile as P;
        super::resource_build_profile::NativeResourceBuildProfile::require()
            .map_err(|_| refused())?;
        let stripes = usize::try_from(PagedCachedFile::lock_stripes()).map_err(|_| refused())?;
        let read = vector_storage::<RwLock<LRUCache<SharedPageBytes>>>(stripes)
            .map_err(|_| refused())?
            .allocator_bytes;
        let write = vector_storage::<Arc<Mutex<LRUWriteCache>>>(stripes)
            .map_err(|_| refused())?
            .allocator_bytes;
        let controls = P::arc_allocation::<Mutex<LRUWriteCache>>()
            .map_err(|_| refused())?
            .checked_mul(stripes as u64)
            .ok_or_else(refused)?;
        let bytes = read
            .checked_add(write)
            .and_then(|n| n.checked_add(controls))
            .ok_or_else(refused)?;
        usize::try_from(bytes).map_err(|_| refused())
    }
    pub(super) fn source_capacity_bytes() -> Result<usize> {
        super::owned_shared::NativeOwnedShared::<SourceReadCacheState>::source_capacity_bytes()?
            .checked_add(Self::extra_retained_capacity_bytes()?)
            .ok_or_else(super::page_resources::refused)
    }
    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        resources: &NativePageResources,
    ) -> Result<Self> {
        Self::new_observed(
            file,
            page_size,
            resources,
            #[cfg(test)]
            None,
        )
    }
    fn new_observed(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        resources: &NativePageResources,
        #[cfg(test)] trace: Option<Arc<SourceReadCacheTrace>>,
    ) -> Result<Self> {
        use super::page_resources::refused;
        // From the first fallible operation onward, this intact owner closes
        // the backend and retires both vectors/stripe Arcs BEFORE its guard.
        let mut parts = SourceReadCacheConstruction {
            file: Some(CheckedBackend::new(file)),
            read_cache: Some(Vec::new()),
            write_buffer: Some(Vec::new()),
            #[cfg(test)]
            trace,
            prepared: None,
            admission: None,
            resources: resources.clone(),
        };
        if !page_size.is_power_of_two()
            || page_size < super::header::DB_HEADER_SIZE as u64
            || u32::try_from(page_size).is_err()
        {
            return Err(refused());
        }
        parts.admission = Some(resources.seal_fixed_observation_admission()?);
        let extra = Self::extra_retained_capacity_bytes()?;
        parts.prepared = Some(
            super::owned_shared::NativeOwnedShared::prepare_source_retained(resources, extra)?,
        );
        let stripes = usize::try_from(PagedCachedFile::lock_stripes()).map_err(|_| refused())?;
        #[cfg(test)]
        parts.allocation_stage()?;
        parts
            .read_cache
            .as_mut()
            .unwrap()
            .try_reserve_exact(stripes)
            .map_err(|_| refused())?;
        for _ in 0..stripes {
            parts
                .read_cache
                .as_mut()
                .unwrap()
                .push(RwLock::new(LRUCache::new()));
        }
        #[cfg(test)]
        parts.allocation_stage()?;
        parts
            .write_buffer
            .as_mut()
            .unwrap()
            .try_reserve_exact(stripes)
            .map_err(|_| refused())?;
        for _ in 0..stripes {
            #[cfg(test)]
            parts.allocation_stage()?;
            parts
                .write_buffer
                .as_mut()
                .unwrap()
                .push(Arc::new(Mutex::new(LRUWriteCache::new())));
        }
        parts.admission.as_ref().unwrap().require_current()?;
        #[cfg(test)]
        parts.allocation_stage()?;
        // All checks precede the first take. The transfer is infallible and
        // establishes the same data-FIRST/control-LAST graph on success.
        let (Some(file), Some(read_cache), Some(write_buffer), Some(prepared), Some(admission)) = (
            parts.file.take(),
            parts.read_cache.take(),
            parts.write_buffer.take(),
            parts.prepared.take(),
            parts.admission.take(),
        ) else {
            unreachable!("private cache construction fields are present")
        };
        let mut cache = PagedCachedFile::from_stripes(file, page_size, 0, read_cache, write_buffer);
        cache.page_resources = Some(parts.resources.clone());
        cache.readonly_no_cache = true;
        let state = SourceReadCacheState {
            cache,
            #[cfg(test)]
            _retirement: parts.trace.take().map(|trace| SourceReadCacheRetirement {
                resources: parts.resources.clone(),
                trace,
            }),
            admission,
        };
        Ok(Self {
            state: prepared.finish(state),
        })
    }
    pub(super) fn raw_file_len(&self) -> Result<u64> {
        self.state.admission.require_current()?;
        self.state.cache.raw_file_len()
    }
    pub(super) fn read(&self, offset: u64, len: usize) -> Result<SharedPageBytes> {
        use super::page_resources::refused;
        self.state.admission.require_current()?;
        let len64 = u64::try_from(len).map_err(|_| refused())?;
        let size = self.state.cache.page_size;
        let pages = len64 / size;
        if len == 0
            || offset % size != 0
            || len64 % size != 0
            || !pages.is_power_of_two()
            || pages > (1_u64 << super::page_manager::MAX_MAX_PAGE_ORDER)
        {
            return Err(refused());
        }
        let end = offset.checked_add(len64).ok_or_else(refused)?;
        if end > self.state.cache.raw_file_len()? {
            return Err(refused());
        }
        // Sole ordinary I/O/materializer path; no global metadata/payload cache.
        self.state.cache.read(offset, len, PageHint::Clean)
    }
}
#[cfg(not(redb_no_std))]
struct SourceReadCacheConstruction {
    file: Option<CheckedBackend>,
    read_cache: Option<Vec<RwLock<LRUCache<SharedPageBytes>>>>,
    write_buffer: Option<Vec<Arc<Mutex<LRUWriteCache>>>>,
    #[cfg(test)]
    trace: Option<Arc<SourceReadCacheTrace>>,
    prepared: Option<super::owned_shared::NativePreparedShared<SourceReadCacheState>>,
    admission: Option<super::page_resources::NativeFixedObservationAdmission>,
    resources: NativePageResources,
}
#[cfg(not(redb_no_std))]
impl Drop for SourceReadCacheConstruction {
    fn drop(&mut self) {
        drop(self.file.take());
        drop(self.read_cache.take());
        drop(self.write_buffer.take());
        #[cfg(test)]
        if self.prepared.is_some() {
            if let Some(trace) = &self.trace {
                trace
                    .retired_at
                    .store(self.resources.current(), Ordering::Release);
            }
        }
        // Prepared shared guard and original numerical context die afterward.
    }
}
#[cfg(all(test, not(redb_no_std)))]
struct SourceReadCacheTrace {
    fail_after: Option<usize>,
    stages: AtomicUsize,
    retired_at: AtomicUsize,
}
#[cfg(all(test, not(redb_no_std)))]
impl SourceReadCacheTrace {
    fn new(fail_after: Option<usize>) -> Self {
        Self {
            fail_after,
            stages: AtomicUsize::new(0),
            retired_at: AtomicUsize::new(usize::MAX),
        }
    }
}
#[cfg(all(test, not(redb_no_std)))]
impl SourceReadCacheConstruction {
    fn allocation_stage(&self) -> Result<()> {
        if let Some(trace) = &self.trace {
            let n = trace.stages.load(Ordering::Acquire);
            if trace.fail_after == Some(n) {
                return Err(super::page_resources::refused());
            }
            trace.stages.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }
}
#[cfg(all(test, not(redb_no_std)))]
struct SourceReadCacheRetirement {
    resources: NativePageResources,
    trace: Arc<SourceReadCacheTrace>,
}
#[cfg(all(test, not(redb_no_std)))]
impl Drop for SourceReadCacheRetirement {
    fn drop(&mut self) {
        // SourceReadCacheState.cache (all stripe shells/backings) has dropped.
        // The enclosing OwnedValue guard still pays for that complete graph.
        self.trace
            .retired_at
            .store(self.resources.current(), Ordering::Release);
    }
}

// A closed storage choice. The Source variant never reveals PagedCachedFile,
// its stripes, backend or descriptor; ordinary callers keep their original Arc.
#[derive(Clone)]
pub(super) enum NativePageStorage {
    Ordinary(Arc<PagedCachedFile>),
    #[cfg(not(redb_no_std))]
    PrepaidReadonly(NativeSourceReadCachedFile),
}

pub(super) enum NativeDirectRead {
    Ordinary(Vec<u8>),
    #[cfg(not(redb_no_std))]
    Prepaid(super::owned_allocator_vec::NativeOwnedAllocatorVec<u8>),
}
impl NativeDirectRead {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Ordinary(bytes) => bytes,
            #[cfg(not(redb_no_std))]
            Self::Prepaid(bytes) => bytes,
        }
    }
}
impl NativePageStorage {
    pub(super) fn ordinary(storage: Arc<PagedCachedFile>) -> Self {
        Self::Ordinary(storage)
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn prepaid_readonly(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        resources: NativePageResources,
    ) -> Result<Self> {
        Ok(Self::PrepaidReadonly(
            PagedCachedFile::source_readonly_fixed(file, page_size, resources)?,
        ))
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn bootstrap_live_page_slots(&self) -> Option<usize> {
        match self {
            Self::Ordinary(_) => None,
            // One cached root, one bounded forward path, old/new leaf overlap.
            Self::PrepaidReadonly(_) => Some(crate::tree_store::btree_base::MAX_BTREE_DEPTH + 3),
        }
    }
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<NativeDirectRead> {
        match self {
            Self::Ordinary(cache) => cache
                .read_direct(offset, len)
                .map(NativeDirectRead::Ordinary),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => {
                cache.state.admission.require_current()?;
                if offset != 0
                    || !(len == super::header::MAGICNUMBER.len()
                        || len == super::header::DB_HEADER_SIZE)
                {
                    return Err(super::page_resources::refused());
                }
                let resources = cache
                    .state
                    .cache
                    .page_resources
                    .as_ref()
                    .ok_or_else(super::page_resources::refused)?;
                let mut bytes =
                    super::owned_allocator_vec::NativeOwnedAllocatorVec::source(len, resources)?;
                bytes.resize(len, 0);
                cache.state.cache.file.read(offset, &mut bytes)?;
                Ok(NativeDirectRead::Prepaid(bytes))
            }
        }
    }
    pub(super) fn readonly_policy_is_empty(
        &self,
    ) -> core::result::Result<(), super::allocator_reopen_facts::AllocatorResourceError> {
        match self {
            Self::Ordinary(cache) => cache.readonly_policy_is_empty(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.readonly_policy_is_empty(),
        }
    }
    pub(super) fn try_lock_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        match self {
            Self::Ordinary(cache) => cache.try_lock_range(range),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.try_lock_range(range),
        }
    }
    pub(super) fn try_lock_shared_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        match self {
            Self::Ordinary(cache) => cache.try_lock_shared_range(range),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.try_lock_shared_range(range),
        }
    }
    pub(super) fn lock_range(&self, range: impl RangeBounds<u64>) -> Result {
        match self {
            Self::Ordinary(cache) => cache.lock_range(range),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.lock_range(range),
        }
    }
    pub(super) fn lock_shared_range(&self, range: impl RangeBounds<u64>) -> Result {
        match self {
            Self::Ordinary(cache) => cache.lock_shared_range(range),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.lock_shared_range(range),
        }
    }
    pub(super) fn unlock_range(&self, range: impl RangeBounds<u64>) -> Result {
        match self {
            Self::Ordinary(cache) => cache.unlock_range(range),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.unlock_range(range),
        }
    }
    pub(super) fn query_lock_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        match self {
            Self::Ordinary(cache) => cache.query_lock_range(range),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.query_lock_range(range),
        }
    }
    pub(super) fn has_source_write_metadata(&self) -> bool {
        match self {
            Self::Ordinary(cache) => cache.has_source_write_metadata(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => false,
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn handoff_write_metadata_to_ordinary(&self) -> Result<()> {
        match self {
            Self::Ordinary(cache) => cache.handoff_write_metadata_to_ordinary(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn prepare_write_metadata(
        &self,
        resources: Option<&NativePageResources>,
    ) -> Result<PreparedWriteMetadata<'_>> {
        match self {
            Self::Ordinary(cache) => cache.prepare_write_metadata(resources),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn cache_stats(&self) -> CacheStats {
        match self {
            Self::Ordinary(cache) => cache.cache_stats(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.cache_stats(),
        }
    }
    pub(super) fn close(&self) -> Result {
        match self {
            Self::Ordinary(cache) => cache.close(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.close(),
        }
    }
    pub(super) fn check_io_errors(&self) -> Result {
        match self {
            Self::Ordinary(cache) => cache.check_io_errors(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.check_io_errors(),
        }
    }
    pub(super) fn raw_file_len(&self) -> Result<u64> {
        match self {
            Self::Ordinary(cache) => cache.raw_file_len(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.state.cache.raw_file_len(),
        }
    }
    pub(super) fn discard_write_buffer(&self) -> () {
        match self {
            Self::Ordinary(cache) => cache.discard_write_buffer(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn resize(&self, len: u64) -> Result {
        match self {
            Self::Ordinary(cache) => cache.resize(len),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn flush(&self) -> Result {
        match self {
            Self::Ordinary(cache) => cache.flush(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn sync_file(&self) -> Result {
        match self {
            Self::Ordinary(cache) => cache.sync_file(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn write_barrier(&self) -> () {
        match self {
            Self::Ordinary(cache) => cache.write_barrier(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn has_committed_pages_buffered(&self) -> bool {
        match self {
            Self::Ordinary(cache) => cache.has_committed_pages_buffered(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => false,
        }
    }
    pub(super) fn write_direct(&self, offset: u64, data: &[u8]) -> Result<()> {
        match self {
            Self::Ordinary(cache) => cache.write_direct(offset, data),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn read_for_context(
        &self,
        offset: u64,
        len: usize,
        hint: PageHint,
        resources: &NativePageResources,
    ) -> Result<SharedPageBytes> {
        match self {
            Self::Ordinary(cache) => cache.read_for_context(offset, len, hint, resources),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => {
                cache.state.admission.require_for(resources)?;
                cache.read(offset, len)
            }
        }
    }
    pub(super) fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<SharedPageBytes> {
        match self {
            Self::Ordinary(cache) => cache.read(offset, len, hint),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache.read(offset, len),
        }
    }
    pub(super) fn cancel_pending_write(&self, offset: u64, len: usize) -> () {
        match self {
            Self::Ordinary(cache) => cache.cancel_pending_write(offset, len),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn invalidate_cache(&self, offset: u64, len: usize) -> () {
        match self {
            Self::Ordinary(cache) => cache.invalidate_cache(offset, len),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn update_transaction_id(&self, transaction_id: TransactionId) -> () {
        match self {
            Self::Ordinary(cache) => cache.update_transaction_id(transaction_id),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn record_local_transaction_id(&self, transaction_id: TransactionId) -> () {
        match self {
            Self::Ordinary(cache) => cache.record_local_transaction_id(transaction_id),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn invalidate_cache_all(&self) -> () {
        match self {
            Self::Ordinary(cache) => cache.invalidate_cache_all(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => (),
        }
    }
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        match self {
            Self::Ordinary(cache) => cache.write(offset, len, overwrite),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    pub(super) fn write_with_prepared_metadata(
        &self,
        offset: u64,
        len: usize,
        overwrite: bool,
        admitted: Option<NativePageAllocation>,
        prepared: PreparedWriteMetadata<'_>,
    ) -> Result<WritablePage> {
        match self {
            Self::Ordinary(cache) => {
                cache.write_with_prepared_metadata(offset, len, overwrite, admitted, prepared)
            }
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => Err(crate::StorageError::Unsupported),
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn point_payload_first_cache_control_capacity_for_test(&self) -> Result<usize> {
        match self {
            Self::Ordinary(cache) => cache.point_payload_first_cache_control_capacity_for_test(),
            #[cfg(not(redb_no_std))]
            Self::PrepaidReadonly(cache) => cache
                .state
                .cache
                .point_payload_first_cache_control_capacity_for_test(),
        }
    }
}

impl PagedCachedFile {
    #[cfg(not(redb_no_std))]
    pub(super) fn source_readonly_fixed_capacity_bytes() -> Result<usize> {
        NativeSourceReadCachedFile::source_capacity_bytes()
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn source_readonly_fixed(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        resources: NativePageResources,
    ) -> Result<NativeSourceReadCachedFile> {
        NativeSourceReadCachedFile::new(file, page_size, &resources)
    }

    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_cache_size: usize,
    ) -> Self {
        let read_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new()))
            .collect();
        let write_buffer = (0..Self::lock_stripes())
            .map(|_| Arc::new(Mutex::new(LRUWriteCache::new())))
            .collect();

        Self::from_stripes(
            CheckedBackend::new(file),
            page_size,
            max_cache_size,
            read_cache,
            write_buffer,
        )
    }

    fn from_stripes(
        file: CheckedBackend,
        page_size: u64,
        max_cache_size: usize,
        read_cache: Vec<RwLock<LRUCache<SharedPageBytes>>>,
        write_buffer: Vec<Arc<Mutex<LRUWriteCache>>>,
    ) -> Self {
        Self {
            file,
            page_size,
            read_cache_bytes: AtomicUsize::new(0),
            read_cache_transaction_id: Mutex::new(None),
            write_buffer_bytes: AtomicUsize::new(0),
            source_write_metadata_seen: AtomicBool::new(false),
            committed_pages_buffered: AtomicBool::new(false),
            max_cache_size,
            page_resources: None,
            readonly_no_cache: false,
            next_eviction_stripe: AtomicUsize::new(0),
            #[cfg(feature = "cache_metrics")]
            reads_total: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            reads_hits: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            writes_total: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            writes_hits: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            evictions: AtomicU64::default(),
            read_cache,
            write_buffer,
        }
    }

    pub(super) fn with_read_resources(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        cache_size: usize,
        resources: Option<NativePageResources>,
        readonly_no_cache: bool,
    ) -> Self {
        let mut result = Self::new(file, page_size, cache_size);
        result.page_resources = resources;
        result.readonly_no_cache = readonly_no_cache;
        result
    }
    pub(super) fn readonly_policy_is_empty(
        &self,
    ) -> core::result::Result<(), super::allocator_reopen_facts::AllocatorResourceError> {
        use super::allocator_reopen_facts::AllocatorResourceError as E;
        use super::read_only_resource::map_bound;
        if !self.readonly_no_cache
            || self.max_cache_size != 0
            || self.read_cache_bytes.load(Ordering::Acquire) != 0
            || self.write_buffer_bytes.load(Ordering::Acquire) != 0
            || self.committed_pages_buffered.load(Ordering::Acquire)
        {
            return Err(E::Unsupported);
        }
        for stripe in &self.read_cache {
            if stripe
                .read()
                .unwrap()
                .resource_capacity_bytes()
                .map_err(map_bound)?
                != 0
            {
                return Err(E::Unsupported);
            }
        }
        for stripe in &self.write_buffer {
            if stripe
                .lock()
                .unwrap()
                .cache
                .resource_capacity_bytes()
                .map_err(map_bound)?
                != 0
            {
                return Err(E::Unsupported);
            }
        }
        Ok(())
    }
    pub(super) fn readonly_fixed_capacity_bytes()
    -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        use super::container_bounds::vector_storage;
        use super::read_only_resource::{ReadOnlyResourceProfile as P, map_bound};
        let stripes = usize::try_from(Self::lock_stripes())
            .map_err(|_| super::allocator_reopen_facts::AllocatorResourceError::Overflow)?;
        let read = vector_storage::<RwLock<LRUCache<SharedPageBytes>>>(stripes)
            .map_err(map_bound)?
            .allocator_bytes;
        let write = vector_storage::<Arc<Mutex<LRUWriteCache>>>(stripes)
            .map_err(map_bound)?
            .allocator_bytes;
        let controls = P::arc_allocation::<Mutex<LRUWriteCache>>()?
            .checked_mul(stripes as u64)
            .ok_or(super::allocator_reopen_facts::AllocatorResourceError::Overflow)?;
        P::arc_allocation::<Self>()?
            .checked_add(2 * core::mem::size_of::<Self>() as u64)
            .and_then(|n| n.checked_add(read))
            .and_then(|n| n.checked_add(write))
            .and_then(|n| n.checked_add(controls))
            .ok_or(super::allocator_reopen_facts::AllocatorResourceError::Overflow)
    }
    #[cfg(test)]
    fn with_page_resources_for_test(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_cache_size: usize,
        resources: NativePageResources,
    ) -> Self {
        let mut cache = Self::new(file, page_size, max_cache_size);
        cache.page_resources = Some(resources);
        cache
    }

    pub(crate) fn try_lock_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        self.file.try_lock_range(range)
    }

    pub(crate) fn try_lock_shared_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        self.file.try_lock_shared_range(range)
    }

    pub(crate) fn lock_range(&self, range: impl RangeBounds<u64>) -> Result {
        self.file.lock_range(range)
    }

    pub(crate) fn lock_shared_range(&self, range: impl RangeBounds<u64>) -> Result {
        self.file.lock_shared_range(range)
    }

    pub(crate) fn unlock_range(&self, range: impl RangeBounds<u64>) -> Result {
        self.file.unlock_range(range)
    }

    /// Whether an exclusive lock over the range would conflict with one held elsewhere.
    pub(crate) fn query_lock_range(&self, range: impl RangeBounds<u64>) -> Result<bool> {
        self.file.query_lock_range(range)
    }

    pub(super) fn has_source_write_metadata(&self) -> bool {
        self.source_write_metadata_seen.load(Ordering::Acquire)
    }

    // Called only under the genuine new ordinary writer, before constructing
    // either namespace or exposing parallel table aliases. Ordinary readers
    // can only reduce counts/requeue existing entries while each stripe mutex
    // is released. No Source allowance is acquired or consulted.
    #[cfg(not(redb_no_std))]
    pub(super) fn handoff_write_metadata_to_ordinary(&self) -> Result<()> {
        if !self.has_source_write_metadata() {
            return Ok(());
        }
        for stripe in &self.write_buffer {
            let mut lock = stripe.lock().unwrap();
            if !lock.cache.has_source_storage() {
                continue;
            }
            let entries = lock
                .cache
                .len()
                .checked_add(1)
                .ok_or_else(super::page_resources::refused)?;
            let queued = lock
                .cache
                .queue_len()
                .checked_add(1)
                .ok_or_else(super::page_resources::refused)?;
            let replacement = LRUCache::prepare_storage(entries, queued, None)?;
            lock.cache.replace_prepared_storage(replacement);
            // Actual payloads, None slots, sequence and queue order are intact.
            // Source-touched payload policy stays attached to each real value.
        }
        self.source_write_metadata_seen
            .store(false, Ordering::Release);
        Ok(())
    }

    // Pure owning quote for ONE first write's actual prepared cache container.
    // The deterministic fixture has no other writer; reader pressure may only
    // decrease these maxima. No I/O, allocation, quota or Source authority.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn point_payload_first_cache_control_capacity_for_test(&self) -> Result<usize> {
        let mut entries = 0usize;
        let mut queued = 0usize;
        for stripe in &self.write_buffer {
            let lock = stripe.lock().unwrap();
            entries = entries.max(lock.cache.len());
            queued = queued.max(lock.cache.queue_len());
        }
        let bytes = LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(
            entries
                .checked_add(1)
                .ok_or_else(super::page_resources::refused)?,
            queued
                .checked_add(1)
                .ok_or_else(super::page_resources::refused)?,
        )?;
        usize::try_from(bytes).map_err(|_| super::page_resources::refused())
    }

    pub(super) fn prepare_write_metadata(
        &self,
        resources: Option<&NativePageResources>,
    ) -> Result<PreparedWriteMetadata<'_>> {
        #[cfg(not(redb_no_std))]
        {
            let source = resources.filter(|owner| owner.is_source_write());
            if source.is_none() && !self.has_source_write_metadata() {
                return Ok(PreparedWriteMetadata {
                    owner: self,
                    replacement: None,
                });
            }
            let mut needed = source.is_some();
            let mut entries = 0;
            let mut queued = 0;
            for stripe in &self.write_buffer {
                let lock = stripe.lock().unwrap();
                if source.is_some_and(|owner| !lock.cache.source_storage_matches(owner)) {
                    return Err(super::page_resources::refused());
                }
                needed |= lock.cache.has_source_storage();
                entries = entries.max(lock.cache.len());
                queued = queued.max(lock.cache.queue_len());
            }
            let replacement = if needed {
                Some(LRUCache::prepare_storage(
                    entries
                        .checked_add(1)
                        .ok_or_else(super::page_resources::refused)?,
                    queued
                        .checked_add(1)
                        .ok_or_else(super::page_resources::refused)?,
                    source,
                )?)
            } else {
                None
            };
            return Ok(PreparedWriteMetadata {
                owner: self,
                replacement,
            });
        }
        #[cfg(redb_no_std)]
        {
            let _ = resources;
            Ok(PreparedWriteMetadata { owner: self })
        }
    }

    fn write_buffer_stripe(&self, offset: u64) -> &Arc<Mutex<LRUWriteCache>> {
        let stripe: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        &self.write_buffer[stripe]
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn cache_stats(&self) -> CacheStats {
        #[cfg(not(feature = "cache_metrics"))]
        {
            CacheStats {
                evictions: 0,
                read_hits: 0,
                read_misses: 0,
                write_hits: 0,
                write_misses: 0,
                used_bytes: 0,
            }
        }

        #[cfg(feature = "cache_metrics")]
        {
            let read_hits = self.reads_hits.load(Ordering::Acquire);
            let read_total = self.reads_total.load(Ordering::Acquire);
            let write_hits = self.writes_hits.load(Ordering::Acquire);
            let write_total = self.writes_total.load(Ordering::Acquire);
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            CacheStats {
                evictions: self.evictions.load(Ordering::Acquire),
                read_hits,
                read_misses: read_total - read_hits,
                write_hits,
                write_misses: write_total - write_hits,
                used_bytes: read_bytes + write_bytes,
            }
        }
    }

    pub(crate) fn close(&self) -> Result {
        self.file.close()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.file.check_failure()
    }

    pub(crate) fn raw_file_len(&self) -> Result<u64> {
        self.file.len()
    }

    const fn lock_stripes() -> u64 {
        131
    }

    // Evict entries from the read cache to free at least `bytes_needed` bytes.
    // Iterates through cache stripes and pops lowest-priority entries.
    //
    // Caller must hold a write buffer stripe mutex to maintain the lock ordering
    // invariant (write buffer stripe locks are always acquired before read_cache locks).
    fn evict_from_read_cache(
        &self,
        bytes_needed: usize,
        _write_lock: &MutexGuard<'_, LRUWriteCache>,
    ) {
        let num_stripes = self.read_cache.len();
        let start = self.next_eviction_stripe.fetch_add(1, Ordering::Relaxed) % num_stripes;
        let mut freed = 0;
        for i in 0..num_stripes {
            if freed >= bytes_needed {
                break;
            }
            let stripe = (start + i) % num_stripes;
            let mut lock = self.read_cache[stripe].write().unwrap();
            while freed < bytes_needed {
                if let Some((_, v)) = lock.pop_lowest_priority() {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    freed += v.len();
                    self.read_cache_bytes.fetch_sub(v.len(), Ordering::AcqRel);
                } else {
                    break;
                }
            }
        }
    }

    fn flush_write_buffer(&self) -> Result {
        for stripe in &self.write_buffer {
            let mut write_buffer = stripe.lock().unwrap();

            for (offset, buffer) in write_buffer.cache.iter() {
                self.file.write(*offset, buffer.as_ref().unwrap())?;
            }
            // Transfer flushed pages into the read cache so they are available
            // for subsequent reads without a file I/O.  The write buffer is being
            // drained, so the total check only considers the read cache size.
            for (offset, buffer) in write_buffer.cache.iter_mut() {
                let buffer = buffer.take().unwrap();
                let len = buffer.len();
                let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);

                if buffer.allows_global_cache() && cache_size + len <= self.max_cache_size {
                    let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                    let mut lock = self.read_cache[cache_slot].write().unwrap();
                    if let Some(replaced) = lock.insert(*offset, buffer) {
                        // A race could cause us to replace an existing buffer
                        self.read_cache_bytes
                            .fetch_sub(replaced.len(), Ordering::AcqRel);
                    }
                } else {
                    self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
                }
                self.write_buffer_bytes.fetch_sub(len, Ordering::AcqRel);
            }
            write_buffer.clear();
        }
        // The buffer is empty, so readers no longer need to consult it
        self.committed_pages_buffered
            .store(false, Ordering::Release);

        Ok(())
    }

    // Drop all buffered writes without writing them to the file. Only valid when the state they
    // belong to is being abandoned (e.g. rolling back to the on-disk state).
    pub(super) fn discard_write_buffer(&self) {
        self.committed_pages_buffered
            .store(false, Ordering::Release);
        for stripe in &self.write_buffer {
            let mut write_buffer = stripe.lock().unwrap();
            for (_, buffer) in write_buffer.cache.iter() {
                // Like flush_write_buffer(), decrement per page rather than storing zero: a
                // concurrent writer may be adding to a stripe this loop has already passed.
                self.write_buffer_bytes
                    .fetch_sub(buffer.as_ref().unwrap().len(), Ordering::AcqRel);
            }
            write_buffer.clear();
        }
    }

    // When shrinking, the caller must drop all writable pages and ensure no live page
    // overlaps the removed region. Writes must be serialized with resizing; reads may continue
    // within the retained region, including flushing buffered pages under cache pressure.
    pub(super) fn resize(&self, len: u64) -> Result {
        // Growth can occur with writable pages outstanding and leaves every cached page valid.
        let old_len = self.file.len()?;
        if len < old_len {
            self.invalidate_cache_above(len);
        }

        self.file.set_len(len)
    }

    // Cancel writes before truncating so a concurrent reader's writeback cannot re-extend the
    // file. Take write-buffer locks before read-cache locks, as in read() and flush_write_buffer().
    fn invalidate_cache_above(&self, threshold: u64) {
        for cache_slot in 0..self.read_cache.len() {
            let mut write_buffer = self.write_buffer[cache_slot].lock().unwrap();
            let stale: Vec<u64> = write_buffer
                .cache
                .iter()
                .filter_map(|(offset, buffer)| {
                    let buffer = buffer
                        .as_ref()
                        .expect("Cannot shrink with writable pages outstanding");
                    (*offset >= threshold || buffer.len() as u64 > threshold - *offset)
                        .then_some(*offset)
                })
                .collect();
            for offset in stale {
                let removed = write_buffer.remove(offset).unwrap();
                self.write_buffer_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
            }
            // Retained writes may belong to non-durable commits, so keep their visibility flag.
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            let stale: Vec<u64> = lock
                .iter()
                .filter_map(|(offset, buffer)| {
                    (*offset >= threshold || buffer.len() as u64 > threshold - *offset)
                        .then_some(*offset)
                })
                .collect();
            for k in stale {
                if let Some(removed) = lock.remove(k) {
                    self.read_cache_bytes
                        .fetch_sub(removed.len(), Ordering::AcqRel);
                }
            }
        }
    }

    pub(super) fn flush(&self) -> Result {
        self.flush_write_buffer()?;

        self.file.sync_data()
    }

    // Make the backing file durable without flushing the in-memory write buffer. `set_len` is
    // issued directly to the file (it is not buffered), so this is enough to make the current file
    // length durable, and unlike `flush()` it is safe to call while writable pages are still
    // outstanding (e.g. during `grow()`).
    pub(super) fn sync_file(&self) -> Result {
        self.file.sync_data()
    }

    // Make buffered writes visible to readers, without writing them to the file or guaranteeing
    // durability. They stay in the buffer, against its budget, until flushed or evicted.
    pub(super) fn write_barrier(&self) {
        if self.write_buffer_bytes.load(Ordering::Acquire) > 0 {
            self.committed_pages_buffered.store(true, Ordering::Release);
        }
    }

    // Whether a commit has left pages in the write buffer instead of writing them to the file.
    // Only a non-durable commit does that, so this is always false in the multi-process modes,
    // which refuse `Durability::None`.
    pub(super) fn has_committed_pages_buffered(&self) -> bool {
        self.committed_pages_buffered.load(Ordering::Acquire)
    }

    // Write directly to the file, bypassing the write buffer, so the bytes are on the file when
    // this returns rather than whenever the buffer is next flushed
    pub(super) fn write_direct(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.invalidate_cache(offset, data.len());
        self.cancel_pending_write(offset, data.len());
        self.file.write(offset, data)
    }

    // Read directly from the file, ignoring any cached data
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.read(offset, &mut buffer)?;
        Ok(buffer)
    }

    // Like `read_direct`, but preserves the actual shared page allocation owner.
    // Legacy pages retain their single-allocation Arc construction. The buffer is zero-filled
    // because `StorageBackend::read` takes `&mut [u8]`.
    fn read_direct_into_arc(&self, offset: u64, len: usize) -> Result<SharedPageBytes> {
        let mut arc = SharedPageBytes::zeroed(len, self.page_resources.as_ref())?;
        self.file.read(offset, arc.get_mut().unwrap())?;
        Ok(arc)
    }

    // A numerical read context does not insert pages in the ordinary cache or
    // adopt its existing allocation owner. Preserve exactly the existing cached
    // and committed-buffer view, copying into a preadmitted private page.
    pub(super) fn read_for_context(
        &self,
        offset: u64,
        len: usize,
        hint: PageHint,
        resources: &NativePageResources,
    ) -> Result<SharedPageBytes> {
        let copy = |source: &[u8]| -> Result<SharedPageBytes> {
            if source.len() != len {
                return Err(super::page_resources::refused());
            }
            let mut page = SharedPageBytes::zeroed(len, Some(resources))?;
            page.get_mut().unwrap().copy_from_slice(source);
            Ok(page)
        };
        if matches!(hint, PageHint::None) {
            let lock = self.write_buffer_stripe(offset).lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                return copy(cached);
            }
        }
        let slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let lock = self.read_cache[slot].read().unwrap();
            if let Some(cached) = lock.get(offset) {
                return copy(cached);
            }
        }
        if matches!(hint, PageHint::Clean) && self.committed_pages_buffered.load(Ordering::Acquire)
        {
            let lock = self.write_buffer_stripe(offset).lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                return copy(cached);
            }
        }
        let mut page = SharedPageBytes::zeroed(len, Some(resources))?;
        self.file.read(offset, page.get_mut().unwrap())?;
        Ok(page)
    }

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    pub(super) fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<SharedPageBytes> {
        debug_assert_eq!(0, offset % self.page_size);
        if self.readonly_no_cache {
            return self.read_direct_into_arc(offset, len);
        }
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        // A write transaction's own dirty pages are in the write buffer, so look there first.
        if matches!(hint, PageHint::None) {
            let lock = self.write_buffer_stripe(offset).lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        // A clean page is only in the write buffer after a non-durable commit left committed pages
        // there, and then never also in the read cache: write() drops the read cache entry when it
        // buffers a page, and flush_write_buffer() empties the buffer as it repopulates the cache.
        // Checking the read cache first keeps the buffer's mutex, which serializes readers that
        // the striped read cache lets run concurrently, off the path of pages cached for reading.
        if matches!(hint, PageHint::Clean) && self.committed_pages_buffered.load(Ordering::Acquire)
        {
            let lock = self.write_buffer_stripe(offset).lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                let result = cached.clone();
                // Copy the page into the read cache, so that further reads of it are served by
                // the striped read locks rather than serializing on this stripe's mutex. Both
                // caches then hold the same Arc and count it, which only makes the budget
                // conservative; whichever copy is dropped first -- the buffer's, once it is
                // flushed, or the read cache's, once it is evicted -- leaves the other counted
                // exactly once.
                let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
                if result.allows_global_cache() && cache_size + len <= self.max_cache_size {
                    let mut write_lock = self.read_cache[cache_slot].write().unwrap();
                    if let Some(replaced) = write_lock.insert(offset, result.clone()) {
                        // A race could cause us to replace an existing buffer
                        self.read_cache_bytes
                            .fetch_sub(replaced.len(), Ordering::AcqRel);
                    }
                } else {
                    self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
                }
                return Ok(result);
            }
        }

        let buffer = self.read_direct_into_arc(offset, len)?;
        if !buffer.allows_global_cache() {
            return Ok(buffer);
        }

        // Pages a non-durable commit left in the write buffer would otherwise hold up to half the
        // cache until the next durable commit. They can always be written out, so reclaim their
        // space rather than evicting read cache entries for them. Done before the read cache lock
        // is taken, to keep the ordering of write buffer before read cache.
        //
        // Reclaiming is best effort: on a write error the page stays buffered, the eviction below
        // makes room instead, and the error is neither propagated nor latched, so a read is not
        // failed -- nor every operation after it -- by a writeback nothing depends on.
        if self.committed_pages_buffered.load(Ordering::Acquire) {
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            if read_bytes + len + write_bytes > self.max_cache_size {
                let _ = self.flush_buffered_pages(len);
            }
        }

        let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
        let mut write_lock = self.read_cache[cache_slot].write().unwrap();
        let cache_size = if let Some(replaced) = write_lock.insert(offset, buffer.clone()) {
            // A race could cause us to replace an existing buffer
            self.read_cache_bytes
                .fetch_sub(replaced.len(), Ordering::AcqRel)
        } else {
            cache_size
        };

        // Rule 3: evict from this read-cache slot if the total exceeds the
        // budget.  We evict exactly `len` bytes (one page) per miss to avoid
        // over-eviction spikes.  `write_bytes` is read after any reclaim above,
        // so this only evicts for a shortfall the reclaim did not cover.
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        let over_total = cache_size + len + write_bytes > self.max_cache_size;
        let mut removed = 0;
        if over_total {
            while removed < len {
                if let Some((_, v)) = write_lock.pop_lowest_priority() {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    removed += v.len();
                } else {
                    break;
                }
            }
        }
        if removed > 0 {
            self.read_cache_bytes.fetch_sub(removed, Ordering::AcqRel);
        }

        Ok(buffer)
    }

    // Discard pending writes to the given range
    pub(super) fn cancel_pending_write(&self, offset: u64, _len: usize) {
        assert_eq!(0, offset % self.page_size);
        if let Some(removed) = self
            .write_buffer_stripe(offset)
            .lock()
            .unwrap()
            .remove(offset)
        {
            self.write_buffer_bytes
                .fetch_sub(removed.len(), Ordering::Release);
        }
    }

    // Invalidate any caching of the given range. After this call overlapping reads of the range are allowed
    //
    // NOTE: Invalidating a cached region in subsections is permitted, as long as all subsections are invalidated
    pub(super) fn invalidate_cache(&self, offset: u64, len: usize) {
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let mut lock = self.read_cache[cache_slot].write().unwrap();
        if let Some(removed) = lock.remove(offset) {
            assert_eq!(len, removed.len());
            self.read_cache_bytes
                .fetch_sub(removed.len(), Ordering::AcqRel);
        }
    }

    // The caller holds the header lock across observing this id and updating the cache.
    pub(super) fn update_transaction_id(&self, transaction_id: TransactionId) {
        let mut current = self.read_cache_transaction_id.lock().unwrap();
        if *current != Some(transaction_id) {
            self.invalidate_cache_all();
            *current = Some(transaction_id);
        }
    }

    // The caller holds the header lock across publishing this local commit and updating the tag.
    pub(super) fn record_local_transaction_id(&self, transaction_id: TransactionId) {
        *self.read_cache_transaction_id.lock().unwrap() = Some(transaction_id);
    }

    pub(super) fn invalidate_cache_all(&self) {
        for cache_slot in 0..self.read_cache.len() {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            while let Some((_, removed)) = lock.pop_lowest_priority() {
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
            }
        }
    }

    // Writes lowest-priority pages from `stripe` to the backend until `bytes_needed` bytes have
    // been flushed, or the stripe runs out of evictable pages. Returns the bytes flushed, which
    // can overshoot `bytes_needed` by up to a page.
    fn flush_lowest_priority(
        &self,
        stripe: &mut LRUWriteCache,
        bytes_needed: usize,
        writeback: Writeback,
    ) -> Result<usize> {
        let mut flushed = 0;
        while flushed < bytes_needed {
            #[cfg(not(redb_no_std))]
            if stripe.cache.has_source_storage() {
                let Some((offset, buffer)) = stripe.cache.take_writeback_candidate() else {
                    break;
                };
                let removed_len = buffer.len();
                let result = match writeback {
                    Writeback::Required => self.file.write(offset, &buffer),
                    Writeback::BestEffort => self.file.write_best_effort(offset, &buffer),
                };
                if let Err(error) = result {
                    stripe
                        .cache
                        .restore_writeback_candidate(offset, Some(buffer));
                    return Err(error);
                }
                stripe.cache.finish_writeback_candidate(offset);
                stripe.retire_empty_source_capacity();
                self.write_buffer_bytes
                    .fetch_sub(removed_len, Ordering::Release);
                #[cfg(feature = "cache_metrics")]
                self.evictions.fetch_add(1, Ordering::Relaxed);
                flushed += removed_len;
                continue;
            }
            if let Some((offset, buffer)) = stripe.pop_lowest_priority() {
                let removed_len = buffer.len();
                let result = match writeback {
                    Writeback::Required => self.file.write(offset, &buffer),
                    Writeback::BestEffort => self.file.write_best_effort(offset, &buffer),
                };
                if result.is_err() {
                    stripe.insert(offset, buffer);
                }
                result?;
                self.write_buffer_bytes
                    .fetch_sub(removed_len, Ordering::Release);
                #[cfg(feature = "cache_metrics")]
                {
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                flushed += removed_len;
            } else {
                break;
            }
        }
        Ok(flushed)
    }

    // Writes buffered pages to the backend until `bytes_needed` bytes have been flushed, or no
    // stripe yields more. Returns the bytes flushed. Stripes are taken with try_lock, like the
    // over-budget eviction in write(), so a stripe another thread holds is skipped rather than
    // risking a deadlock against a writer evicting toward this one.
    fn flush_buffered_pages(&self, bytes_needed: usize) -> Result<usize> {
        let num_stripes = self.write_buffer.len();
        let start = self.next_eviction_stripe.fetch_add(1, Ordering::Relaxed) % num_stripes;
        let mut flushed = 0;
        for i in 0..num_stripes {
            if flushed >= bytes_needed {
                break;
            }
            let stripe = (start + i) % num_stripes;
            if let Ok(mut lock) = self.write_buffer[stripe].try_lock() {
                flushed += self.flush_lowest_priority(
                    &mut lock,
                    bytes_needed - flushed,
                    Writeback::BestEffort,
                )?;
            }
        }
        Ok(flushed)
    }

    // If overwrite is true, the page is initialized to zero
    // cache_policy takes the existing data as an argument and returns the priority. The priority should be stable and not change after WritablePage is dropped
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        self.write_with_admission(offset, len, overwrite, None)
    }

    pub(super) fn write_with_admission(
        &self,
        offset: u64,
        len: usize,
        overwrite: bool,
        admitted: Option<NativePageAllocation>,
    ) -> Result<WritablePage> {
        #[cfg(not(redb_no_std))]
        if !admitted
            .as_ref()
            .is_some_and(NativePageAllocation::forbids_global_cache)
            && self.has_source_write_metadata()
        {
            // Direct private cached-file callers have no native page-number
            // mutation here. Keep this selected mutex from exact snapshot and
            // ordinary allocation through insertion; parallel direct callers
            // cannot invalidate its prepared shape. Actual ordinary native
            // writers already performed the pre-namespace global handoff.
            assert_eq!(0, offset % self.page_size);
            let mut lock = self.write_buffer_stripe(offset).lock().unwrap();
            if lock.cache.has_source_storage() {
                let entries = lock
                    .cache
                    .len()
                    .checked_add(1)
                    .ok_or_else(super::page_resources::refused)?;
                let queued = lock
                    .cache
                    .queue_len()
                    .checked_add(1)
                    .ok_or_else(super::page_resources::refused)?;
                let replacement = LRUCache::prepare_storage(entries, queued, None)?;
                lock.cache.replace_prepared_storage(replacement);
                lock.preparing_write = true;
            }
            return self.write_with_locked_metadata(offset, len, overwrite, admitted, lock);
        }
        let resources = admitted.as_ref().map(NativePageAllocation::resources);
        let prepared = self.prepare_write_metadata(resources.as_ref())?;
        self.write_with_prepared_metadata(offset, len, overwrite, admitted, prepared)
    }

    pub(super) fn write_with_prepared_metadata(
        &self,
        offset: u64,
        len: usize,
        overwrite: bool,
        admitted: Option<NativePageAllocation>,
        mut prepared: PreparedWriteMetadata<'_>,
    ) -> Result<WritablePage> {
        assert!(core::ptr::eq(self, prepared.owner));
        assert_eq!(0, offset % self.page_size);
        let stripe = self.write_buffer_stripe(offset);
        let mut lock = stripe.lock().unwrap();
        #[cfg(not(redb_no_std))]
        {
            let source = admitted
                .as_ref()
                .is_some_and(NativePageAllocation::forbids_global_cache);
            if source || lock.cache.has_source_storage() {
                lock.cache
                    .replace_prepared_storage(prepared.replacement.take().unwrap());
                if source {
                    self.source_write_metadata_seen
                        .store(true, Ordering::Release);
                }
                lock.preparing_write = true;
            }
        }
        #[cfg(redb_no_std)]
        let _ = &mut prepared;
        self.write_with_locked_metadata(offset, len, overwrite, admitted, lock)
    }

    fn write_with_locked_metadata(
        &self,
        offset: u64,
        len: usize,
        overwrite: bool,
        admitted: Option<NativePageAllocation>,
        lock: MutexGuard<'_, LRUWriteCache>,
    ) -> Result<WritablePage> {
        let stripe = self.write_buffer_stripe(offset);
        let mut lock = PreparedWriteLock { inner: lock };
        if let (Some(admitted), Some(existing)) = (admitted.as_ref(), lock.get(offset)) {
            if !existing.may_reuse_for_admission(admitted) {
                return Err(super::page_resources::refused());
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let existing = {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let (Some(admitted), Some(existing)) = (admitted.as_ref(), lock.get(offset)) {
                if !existing.may_reuse_for_admission(admitted) {
                    return Err(super::page_resources::refused());
                }
            }
            if let Some(removed) = lock.remove(offset) {
                assert_eq!(
                    len,
                    removed.len(),
                    "cache inconsistency {len} != {} for offset {offset}",
                    removed.len()
                );
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
                Some(removed)
            } else {
                None
            }
        };

        let source_write = admitted
            .as_ref()
            .is_some_and(NativePageAllocation::forbids_global_cache);
        let mut admitted = admitted;
        let mut data = if let Some(removed) = lock.take_value(offset) {
            #[cfg(feature = "cache_metrics")]
            self.writes_hits.fetch_add(1, Ordering::AcqRel);
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            let pending_bytes = PendingWriteBufferBytes {
                counter: &self.write_buffer_bytes,
                bytes: len,
                installed: false,
            };
            let mut write_bytes = previous + len;
            let half = self.max_cache_size / 2;

            // Rule 1: hold the write buffer at or below 50%, flushing the
            // excess to disk. The budget is global, so drain the stripe we
            // already hold first, then cover the remainder from the other
            // stripes. Those are acquired with try_lock: blocking on a second
            // stripe could deadlock with a concurrent writer evicting toward
            // this one. Eviction is therefore best effort -- a stripe another
            // thread holds is skipped, and a page with an outstanding
            // WritablePage can never be flushed -- so the buffer can briefly
            // sit above the budget. `excess` is recomputed from the live total
            // by every write(), so a later one flushes what this one could not,
            // and a commit flushes every stripe regardless.
            if write_bytes > half {
                let mut excess = write_bytes - half;
                excess = excess.saturating_sub(self.flush_lowest_priority(
                    &mut lock,
                    excess,
                    Writeback::Required,
                )?);
                if excess > 0 {
                    let own: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                    for i in 1..self.write_buffer.len() {
                        let other = (own + i) % self.write_buffer.len();
                        if let Ok(mut other_lock) = self.write_buffer[other].try_lock() {
                            excess = excess.saturating_sub(self.flush_lowest_priority(
                                &mut other_lock,
                                excess,
                                Writeback::Required,
                            )?);
                            if excess == 0 {
                                break;
                            }
                        }
                    }
                }
                write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            }

            // Rules 2 + 3: after rule 1, write <= 50%.  If the total still
            // exceeds the budget then read must be > 50%, so evict from the
            // read cache (fairness: we only take from read when read > 50%).
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            if write_bytes + read_bytes > self.max_cache_size {
                self.evict_from_read_cache(write_bytes + read_bytes - self.max_cache_size, &lock);
            }
            let result = if let Some(data) = existing {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                data
            } else if let Some(allocation) = admitted.take() {
                let mut buffer = SharedPageBytes::zeroed_admitted(len, allocation)?;
                if !overwrite {
                    self.file.read(offset, buffer.get_mut().unwrap())?;
                }
                buffer
            } else if overwrite {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                SharedPageBytes::zeroed(len, self.page_resources.as_ref())?
            } else {
                self.read_direct_into_arc(offset, len)?
            };
            let mut result = result;
            if source_write {
                result.forbid_global_cache();
            }
            lock.insert(offset, result);
            pending_bytes.installed();
            lock.take_value(offset).unwrap()
        };
        #[cfg(feature = "cache_metrics")]
        self.writes_total.fetch_add(1, Ordering::AcqRel);
        if source_write {
            data.forbid_global_cache();
            lock.source_touched = true;
        }
        Ok(WritablePage {
            buffer: stripe.clone(),
            offset,
            data,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::StorageBackend;
    use crate::backends::InMemoryBackend;
    use crate::tree_store::PageHint;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    struct CountingBackend {
        inner: InMemoryBackend,
        writes: Arc<AtomicU64>,
        fail_truncate: bool,
        fail_write_once: Arc<core::sync::atomic::AtomicBool>,
    }

    impl CountingBackend {
        fn new(len: u64) -> (Self, Arc<AtomicU64>) {
            let inner = InMemoryBackend::new();
            inner.set_len(len).unwrap();
            let writes = Arc::new(AtomicU64::new(0));
            (
                Self {
                    inner,
                    writes: writes.clone(),
                    fail_truncate: false,
                    fail_write_once: Arc::new(core::sync::atomic::AtomicBool::new(false)),
                },
                writes,
            )
        }
    }

    impl StorageBackend for CountingBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            if self.fail_truncate && len < self.inner.len()? {
                return Err(std::io::Error::other("truncate failed"));
            }
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), std::io::Error> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if self.fail_write_once.swap(false, Ordering::AcqRel) {
                return Err(std::io::Error::other("injected cache writeback failure"));
            }
            self.inner.write(offset, data)
        }
    }

    #[cfg(not(redb_no_std))]
    fn source_cache_fixture_limit() -> usize {
        use crate::tree_store::page_store::{
            SharedPageBytes, lru_cache::LRUCache, page_resources::NativePageResources,
        };
        // Fixture: at most two payloads in one stripe; prepared owner and
        // retained old owner overlap. Three entries/queue slots cover the one
        // pending new key, without a production workload demand theorem.
        let payload = NativePageResources::page_allocation_bound(128).unwrap();
        let metadata = LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(3, 3).unwrap();
        payload
            .checked_mul(2)
            .unwrap()
            .checked_add(usize::try_from(metadata).unwrap() * 2)
            .unwrap()
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn source_cache_writeback_error_retains_slot_bytes_and_real_capacity_until_retry() {
        use super::Writeback;
        use crate::tree_store::page_store::page_resources::NativePageResources;
        let (backend, writes) = CountingBackend::new(131 * 128);
        let fail_once = backend.fail_write_once.clone();
        let cache = PagedCachedFile::new(Box::new(backend), 128, 4096);
        let pool =
            NativePageResources::from_retained_pool_data(source_cache_fixture_limit(), ()).unwrap();
        let mut page = cache
            .write_with_admission(0, 128, true, Some(pool.admit_page(128).unwrap()))
            .unwrap();
        page.mem_mut().fill(0x7b);
        drop(page);
        let before = pool.current_for_test();
        let mut stripe = cache.write_buffer_stripe(0).lock().unwrap();
        let capacity = stripe.cache.resource_capacity_bytes().unwrap();
        fail_once.store(true, Ordering::Release);
        assert!(
            cache
                .flush_lowest_priority(&mut stripe, 128, Writeback::BestEffort)
                .is_err()
        );
        assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 128);
        assert_eq!(&**stripe.get(0).unwrap(), &[0x7b; 128]);
        assert_eq!(stripe.cache.resource_capacity_bytes().unwrap(), capacity);
        assert_eq!(pool.current_for_test(), before);
        assert_eq!(
            cache
                .flush_lowest_priority(&mut stripe, 128, Writeback::BestEffort)
                .unwrap(),
            128
        );
        assert_eq!(writes.load(Ordering::Acquire), 2);
        assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 0);
        assert_eq!(stripe.cache.resource_capacity_bytes().unwrap(), 0);
        assert_eq!(pool.current_for_test(), 0);
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn source_cache_metadata_preparation_refuses_before_shape_and_ordinary_replaces_without_quota()
    {
        use crate::tree_store::page_store::page_resources::NativePageResources;
        let backend = InMemoryBackend::new();
        backend.set_len(131 * 128).unwrap();
        let cache = PagedCachedFile::new(Box::new(backend), 128, 4096);
        let pool =
            NativePageResources::from_retained_pool_data(source_cache_fixture_limit(), ()).unwrap();
        drop(
            cache
                .write_with_admission(0, 128, true, Some(pool.admit_page(128).unwrap()))
                .unwrap(),
        );
        let before = pool.current_for_test();
        let occupied = pool
            .admit_control(source_cache_fixture_limit() - before)
            .unwrap();
        assert!(cache.prepare_write_metadata(Some(&pool)).is_err());
        assert_eq!(cache.write_buffer_bytes.load(Ordering::Acquire), 128);
        assert!(
            cache
                .write_buffer_stripe(0)
                .lock()
                .unwrap()
                .cache
                .has_source_storage()
        );
        // This owner-level control performs the exact ordinary constructor's
        // pre-namespace transition. Whole transaction proof is still separate.
        cache.handoff_write_metadata_to_ordinary().unwrap();
        assert!(!cache.has_source_write_metadata());
        // Actual ordinary write never asks the exhausted Source pool for growth.
        drop(cache.write(0, 128, false).unwrap());
        assert!(
            !cache
                .write_buffer_stripe(0)
                .lock()
                .unwrap()
                .cache
                .has_source_storage()
        );
        assert!(pool.current_for_test() < source_cache_fixture_limit());
        drop(occupied);
        cache.flush().unwrap();
        assert_eq!(pool.current_for_test(), 0);
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn source_cache_direct_ordinary_parallel_aliases_replace_charged_shape_without_quota() {
        use crate::tree_store::page_store::page_resources::NativePageResources;
        let backend = InMemoryBackend::new();
        backend.set_len(2 * 131 * 128).unwrap();
        let cache = Arc::new(PagedCachedFile::new(Box::new(backend), 128, 4096));
        let pool =
            NativePageResources::from_retained_pool_data(source_cache_fixture_limit(), ()).unwrap();
        drop(
            cache
                .write_with_admission(0, 128, true, Some(pool.admit_page(128).unwrap()))
                .unwrap(),
        );
        let resident = pool.current_for_test();
        let saturated = pool
            .admit_control(source_cache_fixture_limit() - resident)
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|scope| {
            for (offset, byte) in [(0, 11_u8), (131 * 128, 22_u8)] {
                let cache = cache.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let mut page = cache.write(offset, 128, true).unwrap();
                    page.mem_mut().fill(byte);
                    drop(page);
                });
            }
            barrier.wait();
        });
        assert!(
            !cache
                .write_buffer_stripe(0)
                .lock()
                .unwrap()
                .cache
                .has_source_storage()
        );
        assert_eq!(cache.read(0, 128, PageHint::None).unwrap()[0], 11);
        assert_eq!(cache.read(131 * 128, 128, PageHint::None).unwrap()[0], 22);
        drop(saturated);
        cache.flush().unwrap();
        assert_eq!(pool.current_for_test(), 0);
        // Direct-cache scaffolding is not a complete Source transaction proof.
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn source_cache_prepared_replacement_owns_both_real_container_allocations() {
        use crate::tree_store::page_store::{
            SharedPageBytes, lru_cache::LRUCache, page_resources::NativePageResources,
        };
        let backend = InMemoryBackend::new();
        backend.set_len(128).unwrap();
        let cache = PagedCachedFile::new(Box::new(backend), 128, 4096);
        let pool =
            NativePageResources::from_retained_pool_data(source_cache_fixture_limit(), ()).unwrap();
        drop(
            cache
                .write_with_admission(0, 128, true, Some(pool.admit_page(128).unwrap()))
                .unwrap(),
        );
        let before = pool.current_for_test();
        let payload = NativePageResources::page_allocation_bound(128).unwrap();
        let next_metadata = usize::try_from(
            LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(2, 2).unwrap(),
        )
        .unwrap();
        let admitted = pool.admit_page(128).unwrap();
        let prepared = cache.prepare_write_metadata(Some(&pool)).unwrap();
        assert_eq!(pool.current_for_test(), before + payload + next_metadata);
        assert!(prepared.replacement.as_ref().unwrap().has_source_storage());
        // Existing physical payload is reused within the SAME pool; unused
        // prospective payload admission is dropped. Old buckets/queue retire,
        // then only their control allowance may be released.
        drop(
            cache
                .write_with_prepared_metadata(0, 128, false, Some(admitted), prepared)
                .unwrap(),
        );
        assert_eq!(pool.current_for_test(), payload + next_metadata);
        assert!(pool.admitted_high_water_for_test() >= before + payload + next_metadata);
        cache.flush().unwrap();
        assert_eq!(pool.current_for_test(), 0);
    }

    #[test]
    fn cache_leak() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024);
        let cached_file = Arc::new(cached_file);

        let t1 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };
        let t2 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();
        cached_file.invalidate_cache(0, 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
    }

    // The write buffer's budget is global even though the buffer is striped: a writer whose own
    // stripe has nothing evictable must flush pages from the other stripes.
    #[test]
    fn write_buffer_budget_enforced_across_stripes() {
        let backend = InMemoryBackend::new();
        backend.set_len(4096).unwrap();
        let page_size: usize = 128;
        let max_cache_size = 1024;
        let budget = max_cache_size / 2;
        let cached_file = PagedCachedFile::new(Box::new(backend), page_size as u64, max_cache_size);

        // Dirty twice as many pages as the write budget holds. Consecutive page offsets land in
        // different stripes, so each over-budget write finds its own stripe empty and must cover
        // the excess from the stripes holding the previously written pages.
        for i in 0..8u64 {
            let mut page = cached_file
                .write(i * page_size as u64, page_size, true)
                .unwrap();
            page.mem_mut().fill(0xab);
            drop(page);
            assert!(cached_file.write_buffer_bytes.load(Ordering::Acquire) <= budget);
        }

        // Every page must read back intact, whether it was flushed to disk or is still buffered.
        for i in 0..8u64 {
            let data = cached_file
                .read(i * page_size as u64, page_size, PageHint::None)
                .unwrap();
            assert!(data.iter().all(|&b| b == 0xab));
        }
    }

    #[test]
    fn resize_preserves_cached_pages() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 4096);

        // Populate the read cache with two pages from opposite ends of the file.
        cached_file.read(0, 128, PageHint::None).unwrap();
        cached_file.read(512, 128, PageHint::None).unwrap();
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 256);

        // Growing must keep every cached page valid.
        cached_file.resize(2048).unwrap();
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 256);
        assert_eq!(cached_file.raw_file_len().unwrap(), 2048);

        // Shrinking preserves pages that fit within the retained region.
        cached_file.resize(256).unwrap();
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 128);
        assert_eq!(cached_file.raw_file_len().unwrap(), 256);
    }

    #[test]
    fn resize_discards_buffered_pages_past_eof() {
        let (backend, writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 4096);
        for offset in [128, 256, 512] {
            cached_file
                .write(offset, 128, true)
                .unwrap()
                .mem_mut()
                .fill(0xAB);
        }
        cached_file.write_barrier();
        // Cache a retained read page and copies of the two buffered tail pages.
        for offset in [0, 256, 512] {
            cached_file.read(offset, 128, PageHint::Clean).unwrap();
        }

        cached_file.resize(256).unwrap();
        assert_eq!(cached_file.write_buffer_bytes.load(Ordering::Acquire), 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 128);
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        // The surviving non-durable page must still be visible to clean reads.
        assert_eq!(
            &*cached_file.read(128, 128, PageHint::Clean).unwrap(),
            &[0xAB; 128]
        );
        cached_file.flush().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert_eq!(cached_file.raw_file_len().unwrap(), 256);
        assert_eq!(cached_file.write_buffer_bytes.load(Ordering::Acquire), 0);
        assert_eq!(cached_file.read_direct(128, 128).unwrap(), vec![0xAB; 128]);

        cached_file.resize(1024).unwrap();
        for offset in [256, 512] {
            assert_eq!(
                &*cached_file.read(offset, 128, PageHint::Clean).unwrap(),
                &[0; 128]
            );
        }
    }

    #[test]
    fn resize_discards_pages_straddling_eof() {
        for buffered in [false, true] {
            let (backend, _) = CountingBackend::new(1024);
            let cached_file = PagedCachedFile::new(Box::new(backend), 128, 4096);
            cached_file
                .write(256, 512, true)
                .unwrap()
                .mem_mut()
                .fill(0xCD);
            if buffered {
                cached_file.write_barrier();
                cached_file.read(256, 512, PageHint::Clean).unwrap();
            } else {
                cached_file.flush().unwrap();
            }

            cached_file.resize(512).unwrap();
            assert_eq!(cached_file.write_buffer_bytes.load(Ordering::Acquire), 0);
            assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
            cached_file.flush().unwrap();
            cached_file.resize(1024).unwrap();
            let expected = cached_file.read_direct(256, 512).unwrap();
            assert_eq!(&expected[256..], &[0; 256]);
            assert_eq!(
                &*cached_file.read(256, 512, PageHint::Clean).unwrap(),
                expected.as_slice()
            );
        }
    }

    #[test]
    fn resize_growth_preserves_writable_pages() {
        let (backend, _) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 4096);
        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0xAB);
        cached_file.resize(2048).unwrap();
        drop(page);
        cached_file.flush().unwrap();
        assert_eq!(cached_file.raw_file_len().unwrap(), 2048);
        assert_eq!(cached_file.read_direct(0, 128).unwrap(), vec![0xAB; 128]);
    }

    #[test]
    fn resize_failure_prevents_buffered_writes() {
        let (mut backend, writes) = CountingBackend::new(1024);
        backend.fail_truncate = true;
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 4096);
        for offset in [0, 512] {
            cached_file.write(offset, 128, true).unwrap();
        }
        assert!(matches!(
            cached_file.resize(256),
            Err(crate::StorageError::Io(_))
        ));
        assert_eq!(cached_file.write_buffer_bytes.load(Ordering::Acquire), 128);
        assert!(matches!(
            cached_file.flush(),
            Err(crate::StorageError::PreviousIo)
        ));
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    // write_barrier() must not write to the file: the pages stay in the buffer, still visible to
    // readers, including PageHint::Clean ones
    #[test]
    fn write_barrier_issues_no_file_writes() {
        let (backend, writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0xAB);
        drop(page);
        cached_file.write_barrier();
        assert_eq!(writes.load(Ordering::SeqCst), 0);

        assert_eq!(
            &*cached_file.read(0, 128, PageHint::Clean).unwrap(),
            [0xAB; 128].as_slice()
        );
        assert_eq!(
            &*cached_file.read(0, 128, PageHint::None).unwrap(),
            [0xAB; 128].as_slice()
        );
        assert_eq!(cached_file.read_direct(0, 128).unwrap(), vec![0; 128]);

        // A flush writes the page out and clears the buffer
        cached_file.flush().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert_eq!(cached_file.read_direct(0, 128).unwrap(), vec![0xAB; 128]);
        assert_eq!(
            &*cached_file.read(0, 128, PageHint::Clean).unwrap(),
            [0xAB; 128].as_slice()
        );
    }

    // discard_write_buffer() must drop buffered pages, so reads fall through to the file
    #[test]
    fn discard_write_buffer_drops_buffered_pages() {
        let (backend, writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0xCD);
        drop(page);
        cached_file.write_barrier();

        cached_file.discard_write_buffer();
        assert_eq!(cached_file.write_buffer_bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            &*cached_file.read(0, 128, PageHint::None).unwrap(),
            [0u8; 128].as_slice()
        );
        cached_file.flush().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    // A direct write is the file's content from the moment it returns, so a buffered write of the
    // same range must not reach the file after it.
    #[test]
    fn write_direct_supersedes_a_buffered_write() {
        let (backend, _writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0xAA);
        drop(page);
        cached_file.write_barrier();

        cached_file.write_direct(0, &[0xBB; 128]).unwrap();
        cached_file.flush().unwrap();

        assert_eq!(cached_file.read_direct(0, 128).unwrap(), vec![0xBB; 128]);
    }

    // Pages retained by a non-durable commit must not hold the read cache below its share: they
    // can always be written out, so a read-heavy workload reclaims the space they occupy.
    #[test]
    fn retained_pages_do_not_starve_read_cache() {
        const PAGE: usize = 128;
        const MAX_CACHE: usize = 4096;
        const FILE_LEN: u64 = 16 * 1024;

        let (backend, _writes) = CountingBackend::new(FILE_LEN);
        let cached_file = PagedCachedFile::new(Box::new(backend), PAGE as u64, MAX_CACHE);

        // Fill the write buffer to its half-of-cache cap, then commit non-durably so the pages
        // stay buffered
        for i in 0..(MAX_CACHE / 2 / PAGE) as u64 {
            let mut page = cached_file.write(i * PAGE as u64, PAGE, true).unwrap();
            page.mem_mut().fill(0x11);
            drop(page);
        }
        cached_file.write_barrier();
        assert_eq!(
            cached_file.write_buffer_bytes.load(Ordering::Acquire),
            MAX_CACHE / 2
        );

        // Read far more distinct pages than the cache holds, none of them buffered
        let first_unbuffered = (MAX_CACHE / 2 / PAGE) as u64;
        for i in first_unbuffered..FILE_LEN / PAGE as u64 {
            cached_file
                .read(i * PAGE as u64, PAGE, PageHint::Clean)
                .unwrap();
        }

        // The retained pages must have been flushed to make room, rather than capping the read
        // cache at the half of the budget they were occupying
        assert!(
            cached_file.read_cache_bytes.load(Ordering::Acquire) > MAX_CACHE / 2,
            "read cache starved at {} bytes by {} bytes of retained pages",
            cached_file.read_cache_bytes.load(Ordering::Acquire),
            cached_file.write_buffer_bytes.load(Ordering::Acquire)
        );
    }

    // A zero-size cache must stay empty, so that reads always reach the backend, even when
    // reclaiming write buffer space could not get the total under the budget
    #[test]
    fn zero_size_cache_stays_empty_after_reclaim() {
        let (backend, _writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 0);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0x22);
        drop(page);
        cached_file.write_barrier();

        for i in 1..8u64 {
            cached_file.read(i * 128, 128, PageHint::Clean).unwrap();
            assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
        }
    }

    // Pages spilled to the file by write-buffer pressure must stay visible to readers, alongside
    // the pages still buffered
    #[test]
    fn buffered_pages_spill_under_pressure() {
        let (backend, writes) = CountingBackend::new(1024);
        // A two page budget caps the buffer at one page, so each write() spills an earlier one
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 256);

        for i in 0..4u8 {
            let offset = u64::from(i) * 128;
            let mut page = cached_file.write(offset, 128, true).unwrap();
            page.mem_mut().fill(i);
            drop(page);
        }
        cached_file.write_barrier();
        assert!(writes.load(Ordering::SeqCst) > 0);

        for i in 0..4u8 {
            let offset = u64::from(i) * 128;
            assert_eq!(
                &*cached_file.read(offset, 128, PageHint::Clean).unwrap(),
                [i; 128].as_slice()
            );
        }
    }
}

#[cfg(test)]
#[path = "page_resource_tests.rs"]
mod page_resource_tests;

// Numerical cache-construction component only. Backend/lock and original pool
// construction are fixture inputs, not admitted by this slice.
#[cfg(all(test, not(redb_no_std)))]
mod source_read_cache_tests {
    use super::*;
    use crate::BackendError;
    use crate::backends::FileBackend;
    use core::ops::Bound;
    use std::fs::{File, OpenOptions};
    use std::io::Write;

    struct ObservedBackend {
        file: FileBackend,
        resources: NativePageResources,
        closed_at: Arc<AtomicUsize>,
        reads: Arc<AtomicUsize>,
    }
    impl core::fmt::Debug for ObservedBackend {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("ObservedBackend")
        }
    }
    impl StorageBackend for ObservedBackend {
        fn len(&self) -> std::io::Result<u64> {
            self.file.len()
        }
        fn read(&self, offset: u64, bytes: &mut [u8]) -> std::io::Result<()> {
            self.reads.fetch_add(1, Ordering::AcqRel);
            self.file.read(offset, bytes)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            self.file.set_len(len)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.file.sync_data()
        }
        fn write(&self, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
            self.file.write(offset, bytes)
        }
        fn close(&self) -> std::io::Result<()> {
            self.closed_at
                .store(self.resources.current(), Ordering::Release);
            self.file.close()
        }
        fn try_lock_range(
            &self,
            a: Bound<u64>,
            b: Bound<u64>,
        ) -> core::result::Result<bool, BackendError> {
            self.file.try_lock_range(a, b)
        }
        fn try_lock_shared_range(
            &self,
            a: Bound<u64>,
            b: Bound<u64>,
        ) -> core::result::Result<bool, BackendError> {
            self.file.try_lock_shared_range(a, b)
        }
    }
    fn file_fixture() -> (tempfile::NamedTempFile, [u8; 1024]) {
        let mut file = crate::create_tempfile();
        let mut bytes = [0_u8; 1024];
        bytes[..512].fill(0x39);
        bytes[512..].fill(0x75);
        file.as_file_mut().write_all(&bytes).unwrap();
        (file, bytes)
    }
    fn observed_backend(
        path: &std::path::Path,
        resources: &NativePageResources,
    ) -> (Box<dyn StorageBackend>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        // Genuine O_RDONLY descriptor and native shared full-range lock.
        let file = FileBackend::new(File::open(path).unwrap()).unwrap();
        assert!(
            file.try_lock_shared_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        let closed = Arc::new(AtomicUsize::new(usize::MAX));
        let reads = Arc::new(AtomicUsize::new(0));
        (
            Box::new(ObservedBackend {
                file,
                resources: resources.clone(),
                closed_at: closed.clone(),
                reads: reads.clone(),
            }),
            closed,
            reads,
        )
    }
    fn assert_refused<T>(result: Result<T>) {
        match result {
            Err(StorageError::Io(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory)
            }
            Err(error) => panic!("unexpected refusal classification: {error}"),
            Ok(_) => panic!("expected checked native resource refusal"),
        }
    }
    fn writer_probe(path: &std::path::Path) -> FileBackend {
        FileBackend::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap(),
        )
        .unwrap()
    }

    // req: STO-020, REC-001
    #[test]
    fn source_fixed_read_cache_n_minus_one_refuses_before_stripe_allocation() {
        let needed = PagedCachedFile::source_readonly_fixed_capacity_bytes().unwrap();
        let pool = NativePageResources::from_retained_pool_data(needed - 1, ()).unwrap();
        let epoch = pool.begin_managed_epoch(needed - 1).unwrap();
        let resources = epoch.resources();
        let (file, _) = file_fixture();
        let (backend, closed, reads) = observed_backend(file.path(), resources);
        let trace = Arc::new(SourceReadCacheTrace::new(None));
        assert_refused(NativeSourceReadCachedFile::new_observed(
            backend,
            512,
            resources,
            Some(trace.clone()),
        ));
        assert_eq!(trace.stages.load(Ordering::Acquire), 0);
        assert_eq!(resources.current(), 0);
        assert_eq!(resources.admitted_high_water_for_test(), 0);
        assert_eq!(closed.load(Ordering::Acquire), 0);
        assert_eq!(reads.load(Ordering::Acquire), 0);
        let writer = writer_probe(file.path());
        assert!(
            writer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        writer.close().unwrap();
    }

    // req: STO-020, REC-001
    #[test]
    fn source_fixed_read_cache_aliases_and_real_pages_retain_separate_same_pool_charges() {
        let fixed = PagedCachedFile::source_readonly_fixed_capacity_bytes().unwrap();
        let page = NativePageResources::page_allocation_bound(512).unwrap();
        let ceiling = fixed + 2 * page;
        let pool = NativePageResources::from_retained_pool_data(ceiling, ()).unwrap();
        let epoch = pool.begin_managed_epoch(ceiling).unwrap();
        let resources = epoch.resources();
        let (file, bytes) = file_fixture();
        let sibling_descriptor = File::open(file.path()).unwrap();
        let (backend, closed, reads) = observed_backend(file.path(), resources);
        let trace = Arc::new(SourceReadCacheTrace::new(None));
        let cache =
            NativeSourceReadCachedFile::new_observed(backend, 512, resources, Some(trace.clone()))
                .unwrap();
        assert_eq!(resources.current(), fixed);
        assert_eq!(
            trace.stages.load(Ordering::Acquire),
            2 + PagedCachedFile::lock_stripes() as usize + 1
        );
        cache.state.cache.readonly_policy_is_empty().unwrap();
        assert_eq!(cache.raw_file_len().unwrap(), 1024);
        let a = cache.read(0, 512).unwrap();
        let b = cache.read(512, 512).unwrap();
        assert_eq!(a.as_ref(), &bytes[..512]);
        assert_eq!(b.as_ref(), &bytes[512..]);
        assert!(!a.allows_global_cache());
        assert!(!b.allows_global_cache());
        assert_eq!(reads.load(Ordering::Acquire), 2);
        assert_eq!(resources.current(), ceiling);
        let a_alias = a.clone();
        let alias = cache.clone();
        drop(cache);
        let writer = writer_probe(file.path());
        assert!(
            !writer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        assert_eq!(closed.load(Ordering::Acquire), usize::MAX);
        drop(alias);
        assert_eq!(closed.load(Ordering::Acquire), ceiling);
        assert_eq!(trace.retired_at.load(Ordering::Acquire), ceiling);
        assert_eq!(resources.current(), 2 * page);
        assert_eq!(sibling_descriptor.metadata().unwrap().len(), 1024);
        assert!(
            writer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        writer.close().unwrap();
        drop(a);
        assert_eq!(resources.current(), 2 * page);
        drop(a_alias);
        assert_eq!(resources.current(), page);
        assert_eq!(b.as_ref(), &bytes[512..]);
        drop(b);
        assert_eq!(resources.current(), 0);
        assert_eq!(resources.admitted_high_water_for_test(), ceiling);
    }

    // req: STO-020, REC-001
    #[test]
    fn source_fixed_read_cache_partial_stripes_retire_before_fixed_charge() {
        let fixed = PagedCachedFile::source_readonly_fixed_capacity_bytes().unwrap();
        let pool = NativePageResources::from_retained_pool_data(fixed, ()).unwrap();
        let epoch = pool.begin_managed_epoch(fixed).unwrap();
        let resources = epoch.resources();
        let (file, _) = file_fixture();
        let (backend, closed, reads) = observed_backend(file.path(), resources);
        // Two vector backings and two stripe Arcs exist at this refusal.
        let trace = Arc::new(SourceReadCacheTrace::new(Some(4)));
        assert_refused(NativeSourceReadCachedFile::new_observed(
            backend,
            512,
            resources,
            Some(trace.clone()),
        ));
        assert_eq!(trace.stages.load(Ordering::Acquire), 4);
        assert_eq!(trace.retired_at.load(Ordering::Acquire), fixed);
        assert_eq!(closed.load(Ordering::Acquire), fixed);
        assert_eq!(reads.load(Ordering::Acquire), 0);
        assert_eq!(resources.admitted_high_water_for_test(), fixed);
        assert_eq!(resources.current(), 0);
        let writer = writer_probe(file.path());
        assert!(
            writer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        writer.close().unwrap();
    }

    // req: STO-020, REC-001
    #[test]
    fn source_fixed_read_cache_invalid_geometry_and_stale_serial_refuse_before_page_allocation() {
        let fixed = PagedCachedFile::source_readonly_fixed_capacity_bytes().unwrap();
        let ceiling = fixed + NativePageResources::page_allocation_bound(512).unwrap();
        let pool = NativePageResources::from_retained_pool_data(ceiling, ()).unwrap();
        let epoch = pool.begin_managed_epoch(ceiling).unwrap();
        let resources = epoch.resources().clone();
        let (file, _) = file_fixture();
        for page_size in [0, 256, 513, u64::from(u32::MAX) + 1] {
            let (backend, closed, _) = observed_backend(file.path(), &resources);
            let trace = Arc::new(SourceReadCacheTrace::new(None));
            assert_refused(NativeSourceReadCachedFile::new_observed(
                backend,
                page_size,
                &resources,
                Some(trace.clone()),
            ));
            assert_eq!(trace.stages.load(Ordering::Acquire), 0);
            assert_eq!(closed.load(Ordering::Acquire), 0);
            assert_eq!(resources.current(), 0);
        }
        let (backend, _, reads) = observed_backend(file.path(), &resources);
        let cache =
            PagedCachedFile::source_readonly_fixed(backend, 512, resources.clone()).unwrap();
        for (offset, len) in [
            (0, 0),
            (1, 512),
            (0, 513),
            (0, 1536),
            (1024, 512),
            (u64::MAX - 511, 512),
            (
                0,
                512usize << (super::super::page_manager::MAX_MAX_PAGE_ORDER + 1),
            ),
        ] {
            assert_refused(cache.read(offset, len));
            assert_eq!(resources.current(), fixed);
            assert_eq!(resources.admitted_high_water_for_test(), fixed);
        }
        assert_eq!(reads.load(Ordering::Acquire), 0);
        drop(epoch);
        let next_epoch = pool.begin_managed_epoch(ceiling).unwrap();
        assert_refused(cache.read(0, 512));
        assert_refused(cache.raw_file_len());
        assert_eq!(reads.load(Ordering::Acquire), 0);
        assert_eq!(resources.current(), fixed);
        drop(cache);
        assert_eq!(resources.current(), 0);
        drop(next_epoch);
    }
}
