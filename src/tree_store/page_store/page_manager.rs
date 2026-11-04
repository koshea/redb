use super::allocator_reopen_facts::{
    AllocatorParseRecorder, AllocatorReopenFacts, AllocatorResourceError,
};
use super::cached_file::NativePageStorage;
#[cfg(debug_assertions)]
use super::debug_page_set::NativeDebugPageSet;
use super::owned_page_batch::{
    NativeAdoptedPageBatch, NativeOwnedPageBatch, NativeOwnedPageVec, NativeOwnedTxnPageBatches,
};
use crate::CacheStats;
use crate::db::{
    BACKEND_LOCK_RANGE, CONSISTENT_BYTE, ConcurrencyMode, FULL_RANGE, SHARED_READER_BYTE,
    SHARED_WRITER_BYTE, TXN_BASE, WHOLE_FILE_READER_BYTE, WRITER_BYTE, byte_range,
};
use crate::io;
use crate::sync::Mutex;
use crate::transaction_tracker::TransactionId;
use crate::transactions::{AllocatorStateKey, AllocatorStateTree, AllocatorStateTreeMut};
use crate::tree_store::btree_base::{BtreeHeader, Checksum};
use crate::tree_store::page_store::active_transactions;
use crate::tree_store::page_store::base::{MAX_PAGE_INDEX, PageHint};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::PagedCachedFile;
use crate::tree_store::page_store::fast_hash::{PageNumberHashMap, PageNumberHashSet, Shrink};
use crate::tree_store::page_store::header::{
    DB_HEADER_SIZE, DatabaseHeader, MAGICNUMBER, TransactionHeader, UnrepairedDatabaseHeader,
};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{Allocators, RegionTracker};
use crate::tree_store::page_store::{PageImpl, PageMut, ScopedBorrow, hash128_with_seed};
use crate::tree_store::{Page, PageNumber, PageTracker};
use crate::{DatabaseError, Result, StorageBackend, StorageError};
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::{max, min};
use core::convert::TryInto;
use core::mem;
use core::ops::{Bound, Deref, Range};
use core::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "logging")]
use log::warn;

// The region header is optional in the v3 file format
// It's an artifact of the v2 file format, so we initialize new databases without headers to save space
const NO_HEADER: u32 = 0;

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
const MAX_USABLE_REGION_SPACE: u64 = 4 * 1024 * 1024 * 1024;
// A region holds at most `MAX_PAGE_INDEX + 1` pages (the page index within a region is 20 bits),
// so the largest buddy-allocator order any region can have is log2(MAX_PAGE_INDEX + 1).
// `u32::ilog2` is always <= 31, so the cast to u8 is lossless.
#[allow(clippy::cast_possible_truncation)]
pub(crate) const MAX_MAX_PAGE_ORDER: u8 = (MAX_PAGE_INDEX + 1).ilog2() as u8;
pub(super) const MIN_USABLE_PAGES: u32 = 10;
const MIN_DESIRED_USABLE_BYTES: u64 = 1024 * 1024;

pub(super) const INITIAL_REGIONS: u32 = 1000; // Enough for a 4TiB database

// Pure native extent failures; no page open, allocator mutation or heap.
enum NativePageNumberRegionFailure {
    MissingRegion { regions: u32 },
    OutsideRegion { region_pages: u64 },
}

// Original file format. No lengths stored with btrees
pub(crate) const FILE_FORMAT_VERSION1: u8 = 1;
// New file format. All btrees have a separate length stored in their header for constant time access
pub(crate) const FILE_FORMAT_VERSION2: u8 = 2;
// New file format:
// * Allocator state is stored in a system table, instead of in the region headers
// * Freed tree split into two system tables: one for the data tables, and one for the system tables
//   It is no longer stored in a separate tree
// * New "allocated pages table" which tracks the pages allocated, in the data tree, by a transaction.
//   This is a system table. It is only written when a savepoint exists
// * New persistent savepoint format
pub(crate) const FILE_FORMAT_VERSION3: u8 = 3;

#[derive(Copy, Clone)]
pub(crate) enum ShrinkPolicy {
    // Try to shrink the file by the default amount
    Default,
    // Try to shrink the file by the maximum amount
    Maximum,
    // Do not try to shrink the file
    Never,
}

/// Controls how `allocate()` picks a free page.
#[derive(Copy, Clone)]
pub(crate) enum AllocationPolicy {
    /// Find a free block at the requested order, or recursively split from a
    /// higher order. Cheaper than `Lowest`, but after `grow()` has appended
    /// buddy-aligned free blocks at high page indices this can allocate at
    /// high absolute pages.
    Default,
    /// Pick the lowest-page-number allocation across all orders. More
    /// expensive, but keeps trailing pages free so `try_shrink()` can
    /// reclaim recently-grown space.
    Lowest,
}

/// Read-only view over `TransactionalMemory` exposing only the methods that
/// btree read paths and stats helpers need. Cheap to clone -- a single
/// `Arc<TransactionalMemory>` bump.
///
/// Construct one from `PageAllocator::resolver()` (write-transaction context)
/// or `PageResolver::new(mem)` (read-transaction context). Read-only btree
/// types accept `PageResolver` rather than `Arc<TransactionalMemory>` so that
/// they cannot be used to bypass `PageAllocator`'s allocation tracking.
#[derive(Clone)]
pub(crate) struct PageResolver {
    mem: Arc<TransactionalMemory>,
    #[cfg(not(redb_no_std))]
    read_context: Option<NativeReadContext>,
}
#[cfg(not(redb_no_std))]
#[derive(Clone)]
pub(crate) struct NativeReadContext {
    pub(crate) limits: super::read_only_resource::NativeReadLimits,
    pub(crate) resources: super::page_resources::NativePageResources,
    #[cfg(debug_assertions)]
    copied_open_pages: super::copied_page_metadata::SharedCopiedPageMetadata,
}
#[cfg(not(redb_no_std))]
impl NativeReadContext {
    pub(crate) fn metadata_capacity_bytes(
        limits: super::read_only_resource::NativeReadLimits,
    ) -> core::result::Result<usize, AllocatorResourceError> {
        #[cfg(debug_assertions)]
        return super::copied_page_metadata::SharedCopiedPageMetadata::capacity_bytes(
            limits.page_bytes,
            limits.base_page,
        );
        #[cfg(not(debug_assertions))]
        {
            let _ = limits;
            Ok(0)
        }
    }
    pub(crate) fn fixed_write_tracking_capacity_bytes(total_requests: usize) -> Result<usize> {
        SharedUncommittedPages::capacity_bytes(total_requests)
    }
    // Genuine numerical Source-write fixture only. The Source allocator is
    // chosen before namespace construction; no ordinary guard/ID is retagged.
    // The marker is unit data, not Source64 admission or a production factory.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn new_fixed_write_for_test(
        limits: super::read_only_resource::NativeReadLimits,
        total_requests: usize,
        fixture_shared_and_cache_controls: usize,
    ) -> Result<Self> {
        let metadata =
            Self::metadata_capacity_bytes(limits).map_err(|_| super::page_resources::refused())?;
        let total = limits
            .page_bytes
            .checked_add(metadata)
            .and_then(|bytes| {
                bytes.checked_add(SharedUncommittedPages::capacity_bytes(total_requests).ok()?)
            })
            .and_then(|bytes| bytes.checked_add(fixture_shared_and_cache_controls))
            .ok_or_else(super::page_resources::refused)?;
        let resources = super::page_resources::NativePageResources::for_fixed_write(
            total,
            super::page_resources::NativeNumericalRetainer::new(()),
        );
        #[cfg(debug_assertions)]
        let copied_open_pages = super::copied_page_metadata::SharedCopiedPageMetadata::new(
            &resources,
            limits.page_bytes,
            limits.base_page,
        )?;
        Ok(Self {
            limits,
            resources,
            #[cfg(debug_assertions)]
            copied_open_pages,
        })
    }

    // Complete native fixed-recipe preparation uses the SAME actual Source
    // pool as its subsequent writer. No fresh numerical/unit counter or
    // process-global read cache can own these copied observation pages.
    // This internal constructor grants no write/Source authority; the full
    // closed planner still owns read/root/profile/recipe eligibility.
    pub(crate) fn for_fixed_recipe(
        limits: super::read_only_resource::NativeReadLimits,
        epoch: &super::page_resources::NativeNumericalEpoch,
    ) -> Result<Self> {
        epoch.require_open()?;
        let resources = epoch.resources().clone();
        if !resources.is_source_write() {
            return Err(super::page_resources::refused());
        }
        #[cfg(debug_assertions)]
        let copied_open_pages = super::copied_page_metadata::SharedCopiedPageMetadata::new(
            &resources,
            limits.page_bytes,
            limits.base_page,
        )?;
        Ok(Self {
            limits,
            resources,
            #[cfg(debug_assertions)]
            copied_open_pages,
        })
    }

    // Caller has already admitted the fixed plan's complete page+metadata peak.
    // One counter owns both. Reserving metadata first leaves precisely the
    // unchanged limits.page_bytes available for actual copied page allocations.
    pub(crate) fn new_numerical(
        limits: super::read_only_resource::NativeReadLimits,
    ) -> Result<Self> {
        let metadata =
            Self::metadata_capacity_bytes(limits).map_err(|_| super::page_resources::refused())?;
        let total = limits
            .page_bytes
            .checked_add(metadata)
            .ok_or_else(super::page_resources::refused)?;
        let resources = super::page_resources::NativePageResources::numerical(total);
        #[cfg(debug_assertions)]
        let copied_open_pages = super::copied_page_metadata::SharedCopiedPageMetadata::new(
            &resources,
            limits.page_bytes,
            limits.base_page,
        )?;
        Ok(Self {
            limits,
            resources,
            #[cfg(debug_assertions)]
            copied_open_pages,
        })
    }
}

impl PageResolver {
    // Native-owned base geometry from this exact resolver's memory owner,
    // available for numerical inspection in every compiled profile. No raw
    // memory handle, admission authority or caller-selected geometry escapes.
    pub(crate) fn resource_base_page_size(&self) -> usize {
        self.mem.get_page_size()
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_prepaid_definition(
        &self,
        data: &[u8],
        name: &str,
        key: usize,
        value: usize,
    ) -> Result {
        self.mem.prepare_prepaid_definition(data, name, key, value)
    }
    pub(crate) fn observe_table_definition(&self, bytes: usize) -> Result {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.read_context {
            context.limits.before_definition(bytes)?;
        }
        self.mem.observe_table_definition(bytes)
    }

    // Closed fixed-recipe observation only: the allocation uses this exact
    // copied-read context's managed Source pool, never an independently paired
    // numerical pool. No resource or memory handle escapes this boundary.
    #[cfg(not(redb_no_std))]
    pub(crate) fn admit_fixed_observation(
        &self,
        bytes: usize,
    ) -> Result<super::page_resources::NativeControlAllocation> {
        let context = self
            .read_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        if !context.resources.is_source_write() {
            return Err(super::page_resources::refused());
        }
        context.resources.admit_control(bytes)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn fixed_page_number_validation_control_bytes() -> usize {
        core::mem::size_of::<(
            crate::sync::MutexGuard<'static, InMemoryState>,
            PageNumber,
            Option<NativePageNumberRegionFailure>,
            super::page_resources::NativeControlAllocation,
            crate::tree_store::NativeKeyPathError,
            crate::transactions::PageList<'static>,
            DatabaseLayout,
            crate::tree_store::page_store::layout::RegionLayout,
            core::result::Result<(), crate::tree_store::NativeKeyPathError>,
            core::ops::Range<usize>,
            // Both the caller and pure bounds helper retain copied PN/scalars.
            PageNumber,
            &'static DatabaseLayout,
            &'static [u8],
            usize,
            usize,
            u32,
            u64,
            u64,
            bool,
        )>()
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_fixed_allocated_page_numbers(
        &self,
        pages: &crate::transactions::PageList<'_>,
    ) -> core::result::Result<(), crate::tree_store::NativeKeyPathError> {
        // Same actual managed context, never caller memory/layout/resources.
        let _open = self.admit_fixed_observation(0)?;
        self.mem.require_allocated_page_number_layout(pages)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn require_fixed_observation_completed_buffer(
        &self,
        buffer: &crate::tree_store::NativeOwnedTxnIds,
        removed: usize,
    ) -> Result<()> {
        let context = self
            .read_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        if !context.resources.is_source_write() {
            return Err(super::page_resources::refused());
        }
        let _open = context.resources.admit_control(0)?;
        buffer.require_for(removed, &context.resources)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn require_fixed_observation_allocation(
        &self,
        allocation: &super::page_resources::NativeControlAllocation,
    ) -> Result<()> {
        let context = self
            .read_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        if !context.resources.is_source_write() || !allocation.belongs_to(&context.resources) {
            return Err(super::page_resources::refused());
        }
        let _open = context.resources.admit_control(0)?;
        Ok(())
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn seal_fixed_observation_admission(
        &self,
    ) -> Result<super::page_resources::NativeFixedObservationAdmission> {
        self.read_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?
            .resources
            .seal_fixed_observation_admission()
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn fixed_observation_definition_limit(&self) -> Result<usize> {
        let _current = self.admit_fixed_observation(0)?;
        Ok(self
            .read_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?
            .limits
            .definition)
    }

    pub(crate) fn resource_matches_current_read(
        &self,
        actual: &Arc<TransactionalMemory>,
        transaction_id: TransactionId,
    ) -> Result<bool> {
        if !Arc::ptr_eq(&self.mem, actual) {
            return Ok(false);
        }
        Ok(self.mem.get_last_committed_transaction_id()? == transaction_id)
    }

    pub(crate) fn new(mem: Arc<TransactionalMemory>) -> Self {
        Self {
            mem,
            #[cfg(not(redb_no_std))]
            read_context: None,
        }
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn with_read_context(
        mem: Arc<TransactionalMemory>,
        context: NativeReadContext,
    ) -> Self {
        Self {
            mem,
            read_context: Some(context),
        }
    }
    // Diagnostic only: exact captured limits of this existing native context.
    // No admission, limit increase, physical read or context/owner escape.
    #[cfg(all(test, debug_assertions, not(redb_no_std)))]
    pub(crate) fn copied_page_references_for_test(&self, page: PageNumber) -> u64 {
        self.read_context.as_ref().map_or(0, |context| {
            context.copied_open_pages.references_for_test(page)
        })
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn context_refusal_for_test(&self) -> Option<(usize, usize, usize, usize)> {
        self.read_context
            .as_ref()
            .map(|context| context.resources.read_refusal_for_test())
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn context_limits_for_test(&self) -> Option<(usize, usize, usize, usize)> {
        self.read_context.as_ref().map(|context| {
            (
                context.limits.branch,
                context.limits.leaf,
                context.limits.definition,
                context.limits.page_bytes,
            )
        })
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn context_current_for_test(&self) -> usize {
        // Preserve this existing page-only probe: tests use it to set exact
        // physical-page refusal ceilings. The REAL counter still includes all
        // live metadata; total accounting is a separate view below.
        self.read_context.as_ref().map_or(0, |context| {
            let total = context.resources.current();
            #[cfg(debug_assertions)]
            return total
                .checked_sub(context.copied_open_pages.reserved_control_bytes_for_test())
                .expect("live context retains its metadata reservation");
            #[cfg(not(debug_assertions))]
            total
        })
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn context_total_for_test(&self) -> usize {
        self.read_context
            .as_ref()
            .map_or(0, |context| context.resources.current())
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn actual_mem_for_read_context(&self) -> &Arc<TransactionalMemory> {
        &self.mem
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.read_context {
            return self.mem.get_page_for_context(page_number, hint, context);
        }
        self.mem.get_page(page_number, hint)
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        self.mem.count_allocated_pages()
    }
}

// Shards for `UncommittedPages`, padded to a cache line: the contention being removed is cores
// handing one lock's line back and forth, worst between cores in different L3 domains, which
// shards sharing a line would reintroduce.
const UNCOMMITTED_SHARDS: usize = 64;

#[repr(align(64))]
struct UncommittedShard(Mutex<PageNumberHashSet>);

/// The pages a write transaction has allocated since its last commit, which `uncommitted()`
/// answers from and rollback frees. Every allocation, free and `uncommitted()` check consults it,
/// and it can never be switched off, so it is sharded by page to keep concurrent writers -- tables
/// of one transaction may be written from different threads -- off a single lock.
enum UncommittedStorage {
    Ordinary(Vec<UncommittedShard>),
    #[cfg(not(redb_no_std))]
    Source(Mutex<SourceUncommittedPages>),
}
#[cfg(not(redb_no_std))]
struct SourceUncommittedPages {
    active: NativeOwnedPageBatch,
    spare: Option<NativeOwnedPageBatch>,
    // Counts allocation/adoption attempts, not current live members. Removing
    // a page cannot restore this budget or permit tombstone-driven growth.
    requests_left: usize,
}
struct UncommittedPages {
    storage: UncommittedStorage,
    #[cfg(all(test, not(redb_no_std)))]
    point_trace: Mutex<Option<Arc<super::NativePointMutatorTrace>>>,
    #[cfg(not(redb_no_std))]
    _control: Option<super::page_resources::NativeControlAllocation>,
}

// All strong references use into_inner. The final Arc shell physically retires
// before UncommittedPages can release the quote for that shell.
#[derive(Clone)]
struct SharedUncommittedPages(Option<Arc<UncommittedPages>>);
impl Deref for SharedUncommittedPages {
    type Target = UncommittedPages;
    fn deref(&self) -> &UncommittedPages {
        self.0.as_deref().expect("live uncommitted page owner")
    }
}
impl Drop for SharedUncommittedPages {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            drop(Arc::into_inner(owner));
        }
    }
}
impl SharedUncommittedPages {
    fn ordinary() -> Self {
        Self(Some(Arc::new(UncommittedPages::new())))
    }
    #[cfg(not(redb_no_std))]
    fn capacity_bytes(requests: usize) -> Result<usize> {
        use super::{container_bounds, page_resources::refused};
        let layout = core::mem::size_of::<UncommittedPages>()
            .checked_add(2 * core::mem::size_of::<usize>())
            .ok_or_else(refused)?;
        let control = container_bounds::allocation(
            layout,
            core::mem::align_of::<UncommittedPages>().max(core::mem::align_of::<usize>()),
        )
        .map_err(|_| refused())?
        .allocator_bytes;
        let set =
            container_bounds::hash_storage_from_historical_fresh_capacity::<PageNumber>(requests)
                .map_err(|_| refused())?
                .allocator_bytes;
        let vector = container_bounds::vector_storage::<PageNumber>(requests)
            .map_err(|_| refused())?
            .allocator_bytes;
        let total = set
            .checked_add(vector)
            .and_then(|each| each.checked_mul(2))
            .and_then(|both| both.checked_add(control))
            .ok_or_else(refused)?;
        usize::try_from(total).map_err(|_| refused())
    }
    #[cfg(not(redb_no_std))]
    fn source(
        resources: &super::page_resources::NativePageResources,
        requests: usize,
    ) -> Result<Self> {
        use super::{container_bounds, page_resources::refused};
        let layout = core::mem::size_of::<UncommittedPages>()
            .checked_add(2 * core::mem::size_of::<usize>())
            .ok_or_else(refused)?;
        let bound = container_bounds::allocation(
            layout,
            core::mem::align_of::<UncommittedPages>().max(core::mem::align_of::<usize>()),
        )
        .map_err(|_| refused())?;
        let control = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let active = NativeOwnedPageBatch::prepare_source(resources, requests)?;
        let spare = NativeOwnedPageBatch::prepare_source(resources, requests)?;
        // All variable tracking allocations finish before this owner can be
        // installed in PageAllocator or any page/allocator mutation can occur.
        Ok(Self(Some(Arc::new(UncommittedPages {
            storage: UncommittedStorage::Source(Mutex::new(SourceUncommittedPages {
                active,
                spare: Some(spare),
                requests_left: requests,
            })),
            #[cfg(all(test, not(redb_no_std)))]
            point_trace: Mutex::new(None),
            _control: Some(control),
        }))))
    }
}
impl UncommittedPages {
    fn new() -> Self {
        Self {
            #[cfg(all(test, not(redb_no_std)))]
            point_trace: Mutex::new(None),
            storage: UncommittedStorage::Ordinary(
                (0..UNCOMMITTED_SHARDS)
                    .map(|_| UncommittedShard(Mutex::new(PageNumberHashSet::default())))
                    .collect(),
            ),
            #[cfg(not(redb_no_std))]
            _control: None,
        }
    }
    // The caller must obtain this slot BEFORE the native allocator, file, page,
    // or debug mutation. Failure consumes no page and requires no late cleanup.
    #[cfg(not(redb_no_std))]
    fn prepare_requests(&self, count: usize) -> Result<()> {
        if let UncommittedStorage::Source(state) = &self.storage {
            let mut state = state.lock().unwrap();
            if count != 0 && !state.active.is_source() {
                return Err(super::page_resources::refused());
            }
            state.requests_left = state
                .requests_left
                .checked_sub(count)
                .ok_or_else(super::page_resources::refused)?;
        }
        Ok(())
    }
    fn insert(&self, page: PageNumber) {
        match &self.storage {
            UncommittedStorage::Ordinary(shards) => {
                assert!(
                    shards[page.page_index as usize % UNCOMMITTED_SHARDS]
                        .0
                        .lock()
                        .unwrap()
                        .insert(page)
                );
            }
            #[cfg(not(redb_no_std))]
            UncommittedStorage::Source(state) => {
                let mut state = state.lock().unwrap();
                // Total attempted insertions are bounded by the fresh request
                // count, including failed allocations and removed members.
                assert!(state.active.len() < state.active.capacity());
                assert!(state.active.insert(page));
            }
        }
    }
    fn remove(&self, page: PageNumber) -> bool {
        match &self.storage {
            UncommittedStorage::Ordinary(shards) => shards
                [page.page_index as usize % UNCOMMITTED_SHARDS]
                .0
                .lock()
                .unwrap()
                .remove(&page),
            #[cfg(not(redb_no_std))]
            UncommittedStorage::Source(state) => state.lock().unwrap().active.remove(&page),
        }
    }
    fn contains(&self, page: PageNumber) -> bool {
        match &self.storage {
            UncommittedStorage::Ordinary(shards) => shards
                [page.page_index as usize % UNCOMMITTED_SHARDS]
                .0
                .lock()
                .unwrap()
                .contains(&page),
            #[cfg(not(redb_no_std))]
            UncommittedStorage::Source(state) => state.lock().unwrap().active.contains(&page),
        }
    }
    fn take_all(&self) -> NativeOwnedPageBatch {
        match &self.storage {
            UncommittedStorage::Ordinary(shards) => {
                let mut result = PageNumberHashSet::default();
                for shard in shards {
                    result.extend(mem::take(&mut *shard.0.lock().unwrap()));
                }
                NativeOwnedPageBatch::ordinary(result)
            }
            #[cfg(not(redb_no_std))]
            UncommittedStorage::Source(state) => {
                let mut state = state.lock().unwrap();
                // Empty error/abort cleanup must never allocate or consume the
                // spare needed by a later post-commit pass.
                if state.active.is_empty() {
                    return NativeOwnedPageBatch::default();
                }
                let replacement = state.spare.take().unwrap_or_default();
                mem::replace(&mut state.active, replacement)
            }
        }
    }
}

/// Per-write-transaction handle through which btree mutation code allocates
/// and frees pages. Bundles the shared `TransactionalMemory` with the write
/// transaction's `AllocationPolicy`.
#[cfg(not(redb_no_std))]
pub(crate) struct NativePreparedDebugFamilies {
    actual_memory: usize,
    profile: crate::transaction_tracker::NativeSourceNoSavepointWriteProfile,
    pool: super::page_resources::NativeControlAllocation,
}
#[cfg(not(redb_no_std))]
impl NativePreparedDebugFamilies {
    pub(crate) fn require_for(
        &self,
        memory: &TransactionalMemory,
        tracker: &crate::transaction_tracker::TransactionTracker,
        guard: &crate::db::TransactionGuard,
        resources: &super::page_resources::NativePageResources,
    ) -> Result<()> {
        if self.actual_memory != core::ptr::from_ref(memory) as usize
            || !self.pool.belongs_to(resources)
        {
            return Err(super::page_resources::refused());
        }
        self.profile.require_for(tracker, memory, guard)?;
        #[cfg(debug_assertions)]
        {
            let allocated = memory.allocated_pages.lock().unwrap().belongs_to(resources);
            let dirty = memory
                .open_dirty_pages
                .lock()
                .unwrap()
                .belongs_to(resources);
            if !allocated || !dirty {
                return Err(super::page_resources::refused());
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct PageAllocator {
    mem: Arc<TransactionalMemory>,
    policy: AllocationPolicy,
    allocated_since_commit: SharedUncommittedPages,
    #[cfg(not(redb_no_std))]
    source_context: Option<NativeReadContext>,
    #[cfg(not(redb_no_std))]
    require_prepared_debug: bool,
    #[cfg(not(redb_no_std))]
    source_checksum: Option<crate::tree_store::NativeChecksumPool>,
    #[cfg(not(redb_no_std))]
    source_entries: Option<crate::tree_store::NativeEntryBufferPool>,
    #[cfg(not(redb_no_std))]
    source_cursor: Option<crate::tree_store::NativeFixedCursorPool>,
    #[cfg(not(redb_no_std))]
    source_packing: Option<crate::tree_store::NativeFixedPackingPool>,
    #[cfg(not(redb_no_std))]
    source_branch_children: Option<crate::tree_store::NativeBranchChildrenPool>,
    #[cfg(not(redb_no_std))]
    source_branch_keys: Option<crate::tree_store::NativeBranchKeyPool>,
}

impl PageAllocator {
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn install_point_trace(&self) -> Arc<super::NativePointMutatorTrace> {
        let trace = Arc::new(super::NativePointMutatorTrace::default());
        let mut installed = self.allocated_since_commit.point_trace.lock().unwrap();
        assert!(installed.is_none());
        *installed = Some(trace.clone());
        trace
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn point_trace(&self) -> Option<Arc<super::NativePointMutatorTrace>> {
        self.allocated_since_commit
            .point_trace
            .lock()
            .unwrap()
            .clone()
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(in crate::tree_store) fn observe_branch_children_for_test(
        &self,
        prepared: bool,
        transfer: bool,
    ) {
        if self.require_prepared_debug {
            self.source_context
                .as_ref()
                .expect("checked Source context")
                .resources
                .observe_branch_children_for_test(prepared, transfer);
        }
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(in crate::tree_store) fn observe_branch_keys_for_test(
        &self,
        prepared: bool,
        transfer: bool,
    ) {
        if self.require_prepared_debug {
            self.source_context
                .as_ref()
                .expect("checked Source context")
                .resources
                .observe_branch_keys_for_test(prepared, transfer);
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(in crate::tree_store) fn observe_split_key_for_test(&self, prepared: bool) {
        if self.require_prepared_debug {
            self.source_context
                .as_ref()
                .expect("checked Source context")
                .resources
                .observe_split_key_for_test(prepared);
        }
    }

    pub(crate) fn new(mem: Arc<TransactionalMemory>, policy: AllocationPolicy) -> Self {
        Self {
            mem,
            policy,
            allocated_since_commit: SharedUncommittedPages::ordinary(),
            #[cfg(not(redb_no_std))]
            source_context: None,
            #[cfg(not(redb_no_std))]
            require_prepared_debug: false,
            #[cfg(not(redb_no_std))]
            source_checksum: None,
            #[cfg(not(redb_no_std))]
            source_entries: None,
            #[cfg(not(redb_no_std))]
            source_cursor: None,
            #[cfg(not(redb_no_std))]
            source_packing: None,
            #[cfg(not(redb_no_std))]
            source_branch_children: None,
            #[cfg(not(redb_no_std))]
            source_branch_keys: None,
        }
    }

    // Only a complete native fixed-write constructor may build this context;
    // this component does not price allocator/container/control allocations.
    #[cfg(not(redb_no_std))]
    pub(crate) fn with_source_context(
        mem: Arc<TransactionalMemory>,
        policy: AllocationPolicy,
        context: NativeReadContext,
        total_requests: usize,
    ) -> Result<Self> {
        if !context.resources.is_source_write() {
            return Err(super::page_resources::refused());
        }
        Ok(Self {
            mem,
            policy,
            allocated_since_commit: SharedUncommittedPages::source(
                &context.resources,
                total_requests,
            )?,
            source_context: Some(context),
            require_prepared_debug: false,
            source_checksum: None,
            source_entries: None,
            source_cursor: None,
            source_packing: None,
            #[cfg(not(redb_no_std))]
            source_branch_children: None,
            #[cfg(not(redb_no_std))]
            source_branch_keys: None,
        })
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn requiring_prepared_debug(mut self) -> Self {
        self.require_prepared_debug = true;
        self
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn with_prepared_checksums(
        mut self,
        prepared: crate::tree_store::NativeChecksumPool,
        entries: crate::tree_store::NativeEntryBufferPool,
        cursor: crate::tree_store::NativeFixedCursorPool,
        packing: crate::tree_store::NativeFixedPackingPool,
        branch_children: crate::tree_store::NativeBranchChildrenPool,
        branch_keys: crate::tree_store::NativeBranchKeyPool,
    ) -> Result<Self> {
        let context = self
            .source_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        prepared.require_available(&context.resources)?;
        entries.require_for(&context.resources)?;
        cursor.require_available(&context.resources)?;
        packing.require_available(&context.resources)?;
        branch_children.require_available(&context.resources)?;
        branch_keys.require_available(&context.resources)?;
        self.source_packing = Some(packing);
        self.source_branch_children = Some(branch_children);
        self.source_branch_keys = Some(branch_keys);
        self.source_cursor = Some(cursor);
        self.source_entries = Some(entries);
        self.source_checksum = Some(prepared);
        Ok(self)
    }
    pub(in crate::tree_store) fn prepare_checksum_frame(
        &self,
        depth: usize,
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<Option<(usize, PageNumber, Checksum)>>>
    {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_checksum
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .checkout(&context.resources, depth);
        }
        let _ = depth;
        Ok(crate::tree_store::fixed_scratch::ScratchVec::default())
    }

    pub(in crate::tree_store) fn prepare_sparse_entries(
        &self,
    ) -> Result<Option<crate::tree_store::btree_base::OwnedEntryBuffer>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_entries
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .checkout(&context.resources)
                .map(Some);
        }
        Ok(None)
    }

    pub(in crate::tree_store) fn is_checked_source(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.require_prepared_debug
        }
        #[cfg(redb_no_std)]
        {
            false
        }
    }
    pub(in crate::tree_store) fn prepare_cursor_path(
        &self,
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<crate::tree_store::btree_cursor::Branch>>
    {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_cursor
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .path(&context.resources);
        }
        Ok(crate::tree_store::fixed_scratch::ScratchVec::default())
    }
    pub(in crate::tree_store) fn require_closed_branch_key<K: crate::Key + 'static>(
        &self,
    ) -> Result<()> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            use core::any::TypeId;
            if TypeId::of::<K>() != TypeId::of::<&'static [u8]>()
                && TypeId::of::<K>() != TypeId::of::<&'static str>()
                && TypeId::of::<K>()
                    != TypeId::of::<crate::transactions::TransactionIdWithPagination>()
            {
                return Err(super::page_resources::refused());
            }
        }
        Ok(())
    }
    pub(in crate::tree_store) fn prepare_branch_keys<'a>(
        &self,
        requested: usize,
    ) -> Result<crate::tree_store::owned_branch_keys::BranchKeys<'a>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_branch_keys
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .checkout(&context.resources, requested)
                .map(crate::tree_store::owned_branch_keys::BranchKeys::Source);
        }
        Ok(crate::tree_store::owned_branch_keys::BranchKeys::ordinary(
            requested,
        ))
    }
    #[cfg(not(redb_no_std))]
    pub(in crate::tree_store) fn prepare_split_separator(
        &self,
        value: &[u8],
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<u8>> {
        if !self.require_prepared_debug {
            return Err(super::page_resources::refused());
        }
        let context = self
            .source_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        self.source_branch_keys
            .as_ref()
            .ok_or_else(super::page_resources::refused)?
            .separator(&context.resources, value)
    }

    pub(in crate::tree_store) fn prepare_branch_children(
        &self,
        requested: usize,
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<(PageNumber, Checksum)>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_branch_children
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .checkout(&context.resources, requested);
        }
        Ok(crate::tree_store::fixed_scratch::ScratchVec::ordinary(
            Vec::with_capacity(requested),
        ))
    }

    pub(in crate::tree_store) fn prepare_cursor_parts(
        &self,
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<(PageImpl, usize)>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_cursor
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .parts(&context.resources);
        }
        Ok(crate::tree_store::fixed_scratch::ScratchVec::default())
    }
    pub(in crate::tree_store) fn prepare_cursor_removed(
        &self,
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<usize>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_cursor
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .removed(&context.resources);
        }
        Ok(crate::tree_store::fixed_scratch::ScratchVec::default())
    }
    pub(in crate::tree_store) fn copy_cursor_key(
        &self,
        role: crate::tree_store::FixedCursorKey,
        bytes: &[u8],
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<u8>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_cursor
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .key(&context.resources, role, bytes);
        }
        let _ = role;
        Ok(crate::tree_store::fixed_scratch::ScratchVec::ordinary(
            bytes.to_vec(),
        ))
    }

    pub(in crate::tree_store) fn prepare_removed_leaf_pages(
        &self,
    ) -> Result<crate::tree_store::fixed_scratch::ScratchVec<PageNumber>> {
        #[cfg(not(redb_no_std))]
        if self.require_prepared_debug {
            let context = self
                .source_context
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            return self
                .source_packing
                .as_ref()
                .ok_or_else(super::page_resources::refused)?
                .removed(&context.resources);
        }
        Ok(crate::tree_store::fixed_scratch::ScratchVec::default())
    }
    #[cfg(not(redb_no_std))]
    pub(in crate::tree_store) fn prepare_fixed_packing(
        &self,
    ) -> Result<(
        crate::tree_store::fixed_scratch::ScratchVec<core::ops::Range<usize>>,
        crate::tree_store::fixed_scratch::ScratchVec<
            crate::tree_store::btree_mutator::FixedPackedLeaf,
        >,
    )> {
        if !self.require_prepared_debug {
            return Err(super::page_resources::refused());
        }
        let context = self
            .source_context
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        let packing = self
            .source_packing
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        Ok((
            packing.plan(&context.resources)?,
            packing.leaves(&context.resources)?,
        ))
    }

    pub(crate) fn prepare_source_shared<T>(
        &self,
        value: T,
    ) -> Result<Option<super::NativeOwnedShared<T>>> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            return super::NativeOwnedShared::source(value, &context.resources).map(Some);
        }
        drop(value);
        Ok(None)
    }

    // Native owning controls only; no raw resource/context getter escapes.
    // Admission and Vec reserve finish before a mutator/cursor is constructed.
    pub(crate) fn prepare_local_freed(
        &self,
        original: &super::NativeOwnedPageVec,
        local: &mut super::NativeLocalPageVec,
    ) -> Result<()> {
        #[cfg(not(redb_no_std))]
        local.prepare_for(
            original,
            self.source_context
                .as_ref()
                .map(|context| &context.resources),
        )?;
        #[cfg(redb_no_std)]
        let _ = (original, local);
        Ok(())
    }

    pub(crate) fn copy_return_bytes(&self, bytes: &[u8]) -> Result<super::NativeOwnedValueBytes> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            return super::NativeOwnedValueBytes::copy_source(bytes, &context.resources);
        }
        // Original ordinary allocation remains one direct Vec copy.
        Ok(super::NativeOwnedValueBytes::ordinary(bytes.to_vec()))
    }
    pub(crate) fn new_shared<T>(&self, value: T) -> Result<super::NativeOwnedShared<T>> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            return super::NativeOwnedShared::source(value, &context.resources);
        }
        Ok(super::NativeOwnedShared::ordinary(value))
    }

    /// Returns a `PageResolver` for constructing read-only views of this transaction's pages.
    pub(crate) fn resolver(&self) -> PageResolver {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            return PageResolver::with_read_context(self.mem.clone(), context.clone());
        }
        PageResolver::new(self.mem.clone())
    }

    /// Drains the set of pages allocated since the last commit, returning
    /// them. Used by commit and non-durable commit paths to hand the set over
    /// to `TransactionalMemory`.
    pub(crate) fn take_allocated_since_commit(&self) -> NativeOwnedPageBatch {
        self.allocated_since_commit.take_all()
    }

    // Takes ownership of pages an earlier transaction allocated, so this one can update them in
    // place. Only sound once this transaction can no longer abort, since rollback_all() frees
    // whatever is adopted. The page leaves the unpersisted set: it is an ordinary uncommitted page
    // from here, and the copy-on-write that may free it does not maintain that set.
    pub(crate) fn adopt_post_commit_allocations(&self) -> Result<()> {
        let pages = {
            let mut unpersisted = self.mem.unpersisted.lock().unwrap();
            #[cfg(not(redb_no_std))]
            self.allocated_since_commit
                .prepare_requests(unpersisted.post_commit_count())?;
            // Capacity refusal leaves the actual pending membership/owner
            // untouched. Only a successful preflight can move the claim.
            unpersisted.take_post_commit_allocations()
        };
        for page in pages.pages {
            if !pages.already_claimed {
                assert!(self.mem.claim_unpersisted(page));
            }
            self.allocated_since_commit.insert(page);
        }
        Ok(())
    }

    /// Reverses every allocation made since the last commit: drains the
    /// allocated-since-commit set and frees each page.
    pub(crate) fn rollback_all(&self) {
        self.mem.debug_assert_no_dirty_pages();
        let drained = self.take_allocated_since_commit();
        for page in &drained {
            self.mem.free(*page, &PageTracker::ignore());
        }
    }

    pub(crate) fn allocate<'a>(&self, size: usize, allocated: &PageTracker) -> Result<PageMut<'a>> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            let physical = size
                .div_ceil(self.mem.get_page_size())
                .checked_next_power_of_two()
                .and_then(|pages| pages.checked_mul(self.mem.get_page_size()))
                .ok_or_else(super::page_resources::refused)?;
            context.limits.before_page(physical)?;
            self.mem.before_debug_page_mutation(
                Some(&context.resources),
                true,
                self.require_prepared_debug,
            )?;
            self.allocated_since_commit.prepare_requests(1)?;
            let page = self.mem.allocate_helper_with_resources(
                size,
                matches!(self.policy, AllocationPolicy::Lowest),
                Some(&context.resources),
            )?;
            allocated.insert(page.get_page_number());
            self.allocated_since_commit.insert(page.get_page_number());
            return Ok(page);
        }
        let page = match self.policy {
            AllocationPolicy::Default => self.mem.allocate(size, allocated)?,
            AllocationPolicy::Lowest => self.mem.allocate_lowest(size, allocated)?,
        };
        self.allocated_since_commit.insert(page.get_page_number());
        Ok(page)
    }

    // Always allocates at the lowest free page, ignoring `self.policy`. Used
    // by compaction's probe loop where the point is specifically to test
    // whether a page can land below its current position.
    pub(crate) fn allocate_lowest<'a>(
        &self,
        size: usize,
        allocated: &PageTracker,
    ) -> Result<PageMut<'a>> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            let physical = size
                .div_ceil(self.mem.get_page_size())
                .checked_next_power_of_two()
                .and_then(|pages| pages.checked_mul(self.mem.get_page_size()))
                .ok_or_else(super::page_resources::refused)?;
            context.limits.before_page(physical)?;
            self.mem.before_debug_page_mutation(
                Some(&context.resources),
                true,
                self.require_prepared_debug,
            )?;
            self.allocated_since_commit.prepare_requests(1)?;
            let page =
                self.mem
                    .allocate_helper_with_resources(size, true, Some(&context.resources))?;
            allocated.insert(page.get_page_number());
            self.allocated_since_commit.insert(page.get_page_number());
            return Ok(page);
        }
        let page = self.mem.allocate_lowest(size, allocated)?;
        self.allocated_since_commit.insert(page.get_page_number());
        Ok(page)
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &PageTracker) {
        self.allocated_since_commit.remove(page);
        self.mem.free(page, allocated);
    }

    pub(crate) fn free_if_uncommitted(&self, page: PageNumber, allocated: &PageTracker) -> bool {
        if self.allocated_since_commit.remove(page) {
            self.mem.free(page, allocated);
            true
        } else {
            false
        }
    }

    // Frees the page immediately if it was allocated in this transaction;
    // otherwise defers it to `freed` for release at commit.
    pub(crate) fn conditional_free(
        &self,
        page: PageNumber,
        allocated: &PageTracker,
        freed: &mut NativeOwnedPageVec,
    ) {
        if !self.free_if_uncommitted(page, allocated) {
            freed.push(page);
        }
    }

    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.contains(page)
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        #[cfg(not(redb_no_std))]
        if self.source_context.is_some() {
            return self.resolver().get_page(page_number, hint);
        }
        self.mem.get_page(page_number, hint)
    }

    pub(crate) fn get_page_mut<'a>(&self, page_number: PageNumber) -> Result<PageMut<'a>> {
        #[cfg(not(redb_no_std))]
        if let Some(context) = &self.source_context {
            let len = self.mem.resource_page_length(page_number)?;
            context.limits.before_page(len)?;
            self.mem.before_debug_page_mutation(
                Some(&context.resources),
                false,
                self.require_prepared_debug,
            )?;
            return self
                .mem
                .get_page_mut_with_resources(page_number, Some(&context.resources));
        }
        self.mem.get_page_mut(page_number)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.mem.get_page_size()
    }
}

fn ceil_log2(x: usize) -> u8 {
    if x.is_power_of_two() {
        x.trailing_zeros().try_into().unwrap()
    } else {
        x.next_power_of_two().trailing_zeros().try_into().unwrap()
    }
}

pub(crate) fn xxh3_checksum(data: &[u8]) -> Checksum {
    hash128_with_seed(data, 0)
}

enum AllocatorState {
    /// Not loaded yet, either during open or after reloading the header.
    None,
    Loaded(Allocators),
    /// An interrupted operation left the allocator state unreliable.
    /// Reopen the database to rebuild it before writing.
    Invalidated,
}

impl AllocatorState {
    fn is_loaded(&self) -> bool {
        matches!(self, AllocatorState::Loaded(_))
    }

    fn is_invalidated(&self) -> bool {
        matches!(self, AllocatorState::Invalidated)
    }
}

struct InMemoryState {
    header: DatabaseHeader,
    allocators: AllocatorState,
    // True if a non-durable commit has updated the secondary slot and that data should be served
    // to readers until a durable commit promotes it to the primary slot on disk. Protected by the
    // enclosing Mutex so updates happen atomically with the header changes they describe.
    read_from_secondary: bool,
}

impl InMemoryState {
    fn new(header: DatabaseHeader) -> Self {
        Self {
            header,
            allocators: AllocatorState::None,
            read_from_secondary: false,
        }
    }

    fn allocators(&self) -> &Allocators {
        match &self.allocators {
            AllocatorState::Loaded(allocators) => allocators,
            AllocatorState::None => panic!("allocator state has not been loaded yet"),
            AllocatorState::Invalidated => panic!("allocator state has been invalidated"),
        }
    }

    fn allocators_mut(&mut self) -> &mut Allocators {
        match &mut self.allocators {
            AllocatorState::Loaded(allocators) => allocators,
            AllocatorState::None => panic!("allocator state has not been loaded yet"),
            AllocatorState::Invalidated => panic!("allocator state has been invalidated"),
        }
    }

    fn get_region(&self, region: u32) -> &BuddyAllocator {
        &self.allocators().region_allocators[region as usize]
    }

    fn get_region_mut(&mut self, region: u32) -> &mut BuddyAllocator {
        &mut self.allocators_mut().region_allocators[region as usize]
    }

    fn get_region_tracker_mut(&mut self) -> &mut RegionTracker {
        &mut self.allocators_mut().region_tracker
    }

    // Slot that reads should be served from: the secondary when a non-durable commit is pending,
    // otherwise the primary.
    fn latest_slot(&self) -> &TransactionHeader {
        if self.read_from_secondary {
            self.header.secondary_slot()
        } else {
            self.header.primary_slot()
        }
    }
}

/// What non-durable commits record in memory instead of in the file. All of it is volatile by
/// construction -- a crash rolls those commits back -- so it shares one lifecycle, and `clear()`
/// is the only way it is emptied: by the durable `commit()` that persists what it stands in for,
/// or the `clear_cache_and_reload()` that abandons it. It lives here because of that coupling,
/// and because reclaiming a page consults it and the allocator together.
#[cfg(all(test, not(redb_no_std)))]
pub(crate) struct ReclamationUnpersistedSnapshot {
    pub(crate) pages: usize,
    pub(crate) pages_capacity: usize,
    pub(crate) post_pages: usize,
    pub(crate) post_capacity: usize,
    pub(crate) allocation_index_capacity: usize,
    pub(crate) post_only: bool,
    pub(crate) weighted_pages: Option<u64>,
}

#[derive(Default)]
struct UnpersistedState {
    // Pages allocated by non-durable commits. Still reclaimable while they are here, since no
    // durable commit references them.
    pages: PageNumberHashSet,
    // Data-tree pages allocated per transaction: the in-memory stand-in for DATA_ALLOCATED_TABLE,
    // kept in memory so that reclaiming a page can cheaply drop its record. Flushed to the table
    // by durable_commit().
    allocations: NativeOwnedTxnPageBatches,
    // Reverse index into `allocations`
    allocation_txn: PageNumberHashMap<TransactionId>,
    // Data-tree pages freed per transaction: the in-memory stand-in for DATA_FREED_TABLE. Held
    // here for the same reason as `allocations` -- the commits that freed them are volatile, so a
    // crash that loses the records also rolls back the commits they describe.
    data_freed: BTreeMap<TransactionId, Vec<PageNumber>>,
    // System-tree pages allocated by the post-commit epilogue, which the next durable commit may
    // take over once it is irreversible. Always a subset of `pages`, so a reused page number is
    // never mistaken for the allocation that held it before.
    post_commit_allocations: PageNumberHashSet,
    // The fixed Immediate Source epilogue moves its actual set into this
    // inline slot. Ordinary pending pages remain in their existing sets. No
    // copied membership/reverse-index allocation is needed for post-only SYS.
    #[cfg(not(redb_no_std))]
    source_pages: Option<NativeOwnedPageBatch>,
    #[cfg(not(redb_no_std))]
    source_pages_are_post_commit: bool,
}

impl UnpersistedState {
    fn clear(&mut self) {
        #[cfg(not(redb_no_std))]
        {
            self.source_pages = None;
            self.source_pages_are_post_commit = false;
        }
        self.pages.clear();
        self.pages.shrink();
        self.allocations.clear();
        self.allocation_txn.clear();
        self.data_freed.clear();
        self.post_commit_allocations.clear();
    }

    fn contains(&self, page: PageNumber) -> bool {
        #[cfg(not(redb_no_std))]
        if self
            .source_pages
            .as_ref()
            .is_some_and(|pages| pages.contains(&page))
        {
            return true;
        }
        self.pages.contains(&page)
    }

    fn extend(&mut self, pages: NativeOwnedPageBatch, post_commit: bool) -> Result<()> {
        #[cfg(not(redb_no_std))]
        if pages.is_source() {
            if self.source_pages.is_some() {
                return Err(super::page_resources::refused());
            }
            self.source_pages = Some(pages);
            self.source_pages_are_post_commit = post_commit;
            return Ok(());
        }
        self.pages.extend(pages);
        Ok(())
    }

    #[cfg(not(redb_no_std))]
    fn post_commit_count(&self) -> usize {
        if self.source_pages_are_post_commit {
            self.source_pages
                .as_ref()
                .expect("Source post-only batch exists")
                .len()
        } else {
            self.post_commit_allocations.len()
        }
    }

    fn take_post_commit_allocations(&mut self) -> NativeAdoptedPageBatch {
        #[cfg(not(redb_no_std))]
        if self.source_pages_are_post_commit {
            self.source_pages_are_post_commit = false;
            // Remove the full post-only membership while holding this same
            // mutex. The returned private owner is the claim, not a naked set.
            return NativeAdoptedPageBatch {
                pages: self
                    .source_pages
                    .take()
                    .expect("Source post-only batch exists"),
                already_claimed: true,
            };
        }
        NativeAdoptedPageBatch {
            pages: NativeOwnedPageBatch::ordinary(mem::take(&mut self.post_commit_allocations)),
            already_claimed: false,
        }
    }

    /// Claims `page` for reclamation, returning whether this call got it. The page and its
    /// allocation record are dropped together, so a claimed page leaves nothing to write out.
    fn claim(&mut self, page: PageNumber) -> bool {
        #[cfg(not(redb_no_std))]
        if self
            .source_pages
            .as_mut()
            .is_some_and(|pages| pages.remove(&page))
        {
            return true;
        }
        if !self.pages.remove(&page) {
            return false;
        }
        // Keeps the subset invariant; see the field.
        self.post_commit_allocations.remove(&page);
        if let Some(txn) = self.allocation_txn.remove(&page) {
            let pages = self
                .allocations
                .get_mut(&txn)
                .expect("allocation_txn points to a missing entry");
            let removed = pages.remove(&page);
            debug_assert!(removed);
            if pages.is_empty() {
                self.allocations.remove(&txn);
            }
        }
        true
    }

    fn record_allocations(
        &mut self,
        transaction_id: TransactionId,
        pages: impl IntoIterator<Item = PageNumber>,
    ) {
        let entry = self.allocations.entry(transaction_id).or_default();
        for page in pages {
            if entry.insert(page) {
                let prev = self.allocation_txn.insert(page, transaction_id);
                debug_assert!(prev.is_none(), "page {page:?} already tracked");
            }
        }
        if entry.is_empty() {
            self.allocations.remove(&transaction_id);
        }
    }

    fn take_allocations(&mut self) -> NativeOwnedTxnPageBatches {
        self.allocation_txn.clear();
        mem::take(&mut self.allocations)
    }

    /// The pages allocated by transactions after `transaction_id`, which a savepoint restore to
    /// that point has to queue for freeing.
    fn allocations_after(&self, transaction_id: TransactionId) -> Vec<PageNumber> {
        self.allocations
            .range(transaction_id.next()..)
            .flat_map(|(_, pages)| pages.iter().copied())
            .collect()
    }

    fn record_data_freed(&mut self, transaction_id: TransactionId, pages: Vec<PageNumber>) {
        if !pages.is_empty() {
            self.data_freed
                .entry(transaction_id)
                .or_default()
                .extend(pages);
        }
    }

    /// The records of transactions in `start..end`, copied out so that the caller can reclaim
    /// pages without holding this state locked.
    fn data_freed_in_range(
        &self,
        start: TransactionId,
        end: TransactionId,
    ) -> Vec<(TransactionId, Vec<PageNumber>)> {
        // The bounds are derived independently -- `start` from the oldest unprocessed non-durable
        // commit, `end` from the oldest live read -- so a live read older than that commit inverts
        // the range, which BTreeMap::range panics on.
        if start >= end {
            return vec![];
        }
        self.data_freed
            .range(start..end)
            .map(|(id, pages)| (*id, pages.clone()))
            .collect()
    }

    /// Replaces a transaction's record with the pages that were not reclaimed, dropping the
    /// record entirely when none are left.
    fn replace_data_freed(&mut self, transaction_id: TransactionId, pages: Vec<PageNumber>) {
        if pages.is_empty() {
            self.data_freed.remove(&transaction_id);
        } else {
            self.data_freed.insert(transaction_id, pages);
        }
    }

    fn take_data_freed(&mut self) -> BTreeMap<TransactionId, Vec<PageNumber>> {
        mem::take(&mut self.data_freed)
    }

    /// Drops the records of transactions after `transaction_id`, whose commits a savepoint restore
    /// has discarded.
    fn drop_data_freed_after(&mut self, transaction_id: TransactionId) {
        self.data_freed.split_off(&transaction_id.next());
    }

    /// The pages this state keeps allocated but unreachable from the roots: those freed by a
    /// non-durable commit and not yet released. An allocator rebuild has to mark them allocated,
    /// exactly as it does for the pages named by the on-disk freed tables.
    fn pages_pending_free(&self) -> Vec<PageNumber> {
        self.data_freed.values().flatten().copied().collect()
    }
}

/// The header bytes themselves rather than a byte standing for them, so that Windows' mandatory
/// locks hold back another process's read of them.
pub(crate) const HEADER_LOCK: Range<u64> = 0..DB_HEADER_SIZE as u64;

/// A hold on the header lock, released when it drops. Taken ahead of the memory's and the
/// tracker's state locks, which a reader's registration and the reclamation scan take under it.
/// Lent to the work under it rather than taken again: the in-process half is a mutex, which its
/// holder cannot take twice.
pub(crate) struct HeaderGuard<'a> {
    storage: Option<&'a NativePageStorage>,
    _in_process: crate::sync::MutexGuard<'a, ()>,
}

impl Drop for HeaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(storage) = self.storage {
            let _ = storage.unlock_range(HEADER_LOCK);
        }
    }
}

/// One operation's exclusive header hold, released after it unless it was the caller's
pub(crate) enum HeaderHold<'a> {
    Lent(&'a HeaderGuard<'a>),
    Own(HeaderGuard<'a>),
}

impl<'a> Deref for HeaderHold<'a> {
    type Target = HeaderGuard<'a>;

    fn deref(&self) -> &HeaderGuard<'a> {
        match self {
            Self::Lent(guard) => guard,
            Self::Own(guard) => guard,
        }
    }
}

/// The lock admitting this process to write. Writer-byte locks are released when the last
/// holder drops; exclusive-writer locks are released by `close()`. Holds the storage rather than
/// `TransactionalMemory`, so integrity checks can borrow the memory through `Arc::get_mut()`.
pub(crate) struct WriterLock {
    storage: NativePageStorage,
    range: Option<(Bound<u64>, Bound<u64>)>,
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        if let Some(range) = self.range {
            let _ = self.storage.unlock_range(range);
        }
    }
}

/// The writer lock a handle holds for the life of the database: the whole file where the
/// database is not shared, and the writer byte where one process writes. Nothing in multi-writer
/// mode, where a write transaction takes that byte itself, or for a read-only handle
fn database_writer_lock_for_mode(
    storage: &NativePageStorage,
    read_only: bool,
    concurrency_mode: ConcurrencyMode,
) -> Option<Arc<WriterLock>> {
    if read_only {
        return None;
    }
    let range = match concurrency_mode {
        // Exclusive-writer locks belong to the backend until close(), including when locking
        // fell back to a whole-storage lock or was unsupported altogether.
        ConcurrencyMode::ExclusiveWriter => None,
        ConcurrencyMode::SingleWriter => Some(byte_range(WRITER_BYTE)),
        ConcurrencyMode::MultiWriter => return None,
    };

    Some(Arc::new(WriterLock {
        storage: storage.clone(),
        range,
    }))
}

// Acquire the writer byte lock that open() should use for repair
fn open_writer_lock_for_mode(
    storage: &NativePageStorage,
    read_only: bool,
    concurrency_mode: ConcurrencyMode,
    database_writer_lock: Option<Arc<WriterLock>>,
) -> Result<Option<Arc<WriterLock>>> {
    if read_only {
        return Ok(None);
    }
    match concurrency_mode {
        ConcurrencyMode::ExclusiveWriter | ConcurrencyMode::SingleWriter => {
            Ok(database_writer_lock)
        }
        ConcurrencyMode::MultiWriter => {
            storage.lock_range(byte_range(WRITER_BYTE))?;

            Ok(Some(Arc::new(WriterLock {
                storage: storage.clone(),
                range: Some(byte_range(WRITER_BYTE)),
            })))
        }
    }
}

pub(crate) struct TransactionalMemory {
    // Native parser observations only; not source/validation/open authority.
    read_observations: super::read_only_resource::ReadOnlyReadObservations,
    read_resource_profile: Option<super::read_only_resource::ReadOnlyResourceProfile>,
    allocator_reopen_component_facts: Mutex<
        Option<core::result::Result<RecordedAllocatorReopenComponent, AllocatorResourceError>>,
    >,
    #[cfg(not(redb_no_std))]
    prepaid_read_resources: Option<super::page_resources::NativePageResources>,
    #[cfg(not(redb_no_std))]
    prepaid_definition_work: Mutex<Option<super::page_resources::NativeControlAllocation>>,
    unpersisted: Mutex<UnpersistedState>,
    // Shared, so that a `WriterLock` can outlive this and still release its range
    storage: NativePageStorage,
    state: Mutex<InMemoryState>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Arc<Mutex<NativeDebugPageSet>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Arc<Mutex<PageNumberHashMap<u64>>>,
    #[cfg(debug_assertions)]
    readonly_debug_hash_capacity_upper: usize,
    // Set of all allocated pages for debugging assertions
    #[cfg(debug_assertions)]
    allocated_pages: Arc<Mutex<NativeDebugPageSet>>,
    // While set, no allocator state is persisted and shutdowns are not recorded as clean,
    // forcing the next open to rebuild the state from the committed trees
    needs_repair: AtomicBool,
    concurrency_mode: ConcurrencyMode,
    read_only: bool,
    // Held for the duration of every header file lock. Ensures in-process threads do not overlap
    // acquiring the lock. Because it's an OFD lock on a single FD, only one may take it at a time.
    // TODO: This could probably be optimized, so that multiple threads can read at once, by making
    // this a counter or RwLock
    in_process_header_lock: Mutex<()>,
    // Where the next scan of the active transaction range starts: a pin lands only at or above
    // the ceiling of the scan before it, since every handle reads the id it pins from the file
    scan_from: Mutex<u64>,
    // The lock the open took, which every write transaction holds while it runs. `None` in
    // multi-writer mode, where a write transaction takes the writer byte itself, and for a
    // read-only handle
    writer_lock: Option<Arc<WriterLock>>,
    page_size: u32,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: u64,
    region_header_with_padding_size: u64,
}

#[cfg(not(redb_no_std))]
pub(crate) struct ReadOnlyAliasDrain {
    #[cfg(debug_assertions)]
    read_pages: Arc<Mutex<PageNumberHashMap<u64>>>,
}
#[cfg(not(redb_no_std))]
impl ReadOnlyAliasDrain {
    pub(crate) fn try_into_drained(self) -> core::result::Result<(), AllocatorResourceError> {
        #[cfg(debug_assertions)]
        {
            let counts = Arc::try_unwrap(self.read_pages)
                .map_err(|_| AllocatorResourceError::Unsupported)?;
            let counts = counts
                .into_inner()
                .map_err(|_| AllocatorResourceError::Unsupported)?;
            if !counts.is_empty() {
                return Err(AllocatorResourceError::Unsupported);
            }
            // Both the real map allocation and its Arc/Mutex control have now
            // been consumed, not inferred from empty rows or numerical bytes.
            drop(counts);
        }
        Ok(())
    }
}
// Only a resource-shape binding: the transient writable recovery flag is not
// a tree/resource change. Full planned-open header/artifact checks remain a
// separate factory responsibility. No source/validation authority is minted.
fn allocator_resource_stamp(header: &DatabaseHeader) -> [u8; DB_HEADER_SIZE] {
    let mut header = header.clone();
    header.recovery_required = false;
    header.to_bytes(true)
}

#[derive(Clone, Copy)]
struct RecordedAllocatorReopenComponent {
    stamp: [u8; DB_HEADER_SIZE],
    validated_layout_len: u64,
    facts: AllocatorReopenFacts,
}

pub(crate) struct NativeAllocatorReopenComponent<'engine> {
    owner: &'engine TransactionalMemory,
    recorded: RecordedAllocatorReopenComponent,
}

impl NativeAllocatorReopenComponent<'_> {
    pub(crate) fn facts(&self) -> AllocatorReopenFacts {
        self.recorded.facts
    }
    pub(crate) fn native_header_binding(&self) -> &[u8; DB_HEADER_SIZE] {
        &self.recorded.stamp
    }
    pub(crate) fn validated_layout_len(&self) -> u64 {
        self.recorded.validated_layout_len
    }

    pub(crate) fn verify_for(
        &self,
        actual: &TransactionalMemory,
    ) -> core::result::Result<(), AllocatorResourceError> {
        if !core::ptr::eq(self.owner, actual) {
            return Err(AllocatorResourceError::Stale);
        }
        let state = actual.state.lock().unwrap();
        // A changed native root/transaction/layout invalidates this captured
        // observation even when the new view also has an unsupported profile.
        if allocator_resource_stamp(&state.header) != self.recorded.stamp
            || state.header.layout().len() != self.recorded.validated_layout_len
        {
            return Err(AllocatorResourceError::Stale);
        }
        if actual.concurrency_mode != ConcurrencyMode::ExclusiveWriter
            || state.read_from_secondary
            || actual.needs_repair()
        {
            return Err(AllocatorResourceError::Unsupported);
        }
        let AllocatorState::Loaded(allocators) = &state.allocators else {
            return Err(AllocatorResourceError::Incomplete);
        };
        if allocators.reopen_capacity_facts()? != self.recorded.facts.final_retained() {
            return Err(AllocatorResourceError::Stale);
        }
        Ok(())
    }
}

impl TransactionalMemory {
    pub(crate) fn allocator_reopen_component(
        &self,
    ) -> core::result::Result<NativeAllocatorReopenComponent<'_>, AllocatorResourceError> {
        let recorded = self
            .allocator_reopen_component_facts
            .lock()
            .unwrap()
            .as_ref()
            .copied()
            .ok_or(AllocatorResourceError::Incomplete)??;
        let observation = NativeAllocatorReopenComponent {
            owner: self,
            recorded,
        };
        observation.verify_for(self)?;
        Ok(observation)
    }
    /// The locks a handle holds for as long as the database is open, taken before anything is
    /// read from the storage.
    fn lock_for_open(
        storage: &NativePageStorage,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<(), DatabaseError> {
        match concurrency_mode {
            ConcurrencyMode::ExclusiveWriter => Self::lock_whole_storage(storage, read_only),
            mode => Self::lock_mode_bytes(storage, read_only, mode),
        }
    }

    fn lock_whole_storage(
        storage: &NativePageStorage,
        read_only: bool,
    ) -> Result<(), DatabaseError> {
        let try_lock = |range| {
            if read_only {
                storage.try_lock_shared_range(range)
            } else {
                storage.try_lock_range(range)
            }
        };
        let suffix_range = (Bound::Included(BACKEND_LOCK_RANGE.end), Bound::Unbounded);
        let middle_range = (
            Bound::Excluded(SHARED_WRITER_BYTE),
            Bound::Excluded(BACKEND_LOCK_RANGE.start),
        );
        let prefix_end = if read_only {
            Bound::Excluded(SHARED_WRITER_BYTE)
        } else {
            Bound::Included(SHARED_WRITER_BYTE)
        };
        let prefix_range = (Bound::Unbounded, prefix_end);
        let ranges = [suffix_range, middle_range, prefix_range];
        for (taken, range) in ranges.iter().enumerate() {
            match try_lock(*range) {
                Ok(true) => {}
                Ok(false) => return Err(DatabaseError::DatabaseAlreadyOpen),
                Err(StorageError::Unsupported) => {
                    for held in &ranges[..taken] {
                        storage.unlock_range(*held)?;
                    }
                    // A whole-storage lock also excludes multi-process writers, so no query
                    // is needed (or permitted) inside that lock.
                    return match try_lock(FULL_RANGE) {
                        Ok(true) => Ok(()),
                        Ok(false) => Err(DatabaseError::DatabaseAlreadyOpen),
                        Err(StorageError::Unsupported) => {
                            #[cfg(feature = "logging")]
                            warn!(
                                "File locks not supported on this platform. You must ensure that only a single process opens the database file, at a time"
                            );
                            Ok(())
                        }
                        Err(err) => Err(err.into()),
                    };
                }
                Err(err) => return Err(err.into()),
            }
        }

        // A multi-writer cohort holds nothing a read-only open's shared locks conflict with, so
        // the open probes for one, which its own ranges leave uncovered for the purpose
        if read_only && Self::locked_for_multi_process_writing(storage)? {
            return Err(DatabaseError::DatabaseAlreadyOpen);
        }

        Ok(())
    }

    /// Whether a multi-writer cohort has the database open. Not gated on the feature that forms
    /// one: the cohort is another process, which may have been built with it when this was not.
    fn locked_for_multi_process_writing(storage: &NativePageStorage) -> Result<bool> {
        storage.query_lock_range(byte_range(SHARED_WRITER_BYTE))
    }

    fn lock_mode_bytes(
        storage: &NativePageStorage,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<(), DatabaseError> {
        let acquired = if read_only {
            storage.try_lock_shared_range(byte_range(SHARED_READER_BYTE))?
        } else {
            match concurrency_mode {
                ConcurrencyMode::ExclusiveWriter => unreachable!(),
                ConcurrencyMode::SingleWriter => {
                    storage.try_lock_range(byte_range(SHARED_WRITER_BYTE))?
                        && storage.try_lock_range(byte_range(WRITER_BYTE))?
                }
                ConcurrencyMode::MultiWriter => {
                    storage.try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))?
                        // A read-only exclusive-writer handle leaves the shared writer byte free,
                        // so only this byte reveals one
                        && !storage.query_lock_range(byte_range(WHOLE_FILE_READER_BYTE))?
                }
            }
        };

        if acquired {
            Ok(())
        } else {
            Err(DatabaseError::DatabaseAlreadyOpen)
        }
    }

    /// Takes the storage and the mutex rather than `&self`, so that the open path can hold the
    /// header before there is a `TransactionalMemory` to hold it from.
    fn lock_header<'a>(
        storage: &'a NativePageStorage,
        in_process: &'a Mutex<()>,
        concurrency_mode: ConcurrencyMode,
        exclusive: bool,
    ) -> Result<HeaderGuard<'a>> {
        let guard = in_process.lock().unwrap();
        if !concurrency_mode.is_multi_process_writable() {
            return Ok(HeaderGuard {
                storage: None,
                _in_process: guard,
            });
        }
        if exclusive {
            storage.lock_range(HEADER_LOCK)?;
        } else {
            storage.lock_shared_range(HEADER_LOCK)?;
        }

        Ok(HeaderGuard {
            storage: Some(storage),
            _in_process: guard,
        })
    }

    fn read_file_header(
        storage: &NativePageStorage,
        page_size: u32,
        _header_lock: &HeaderGuard<'_>,
    ) -> Result<UnrepairedDatabaseHeader, DatabaseError> {
        let header_bytes = storage.read_direct(0, DB_HEADER_SIZE)?;
        UnrepairedDatabaseHeader::from_bytes(header_bytes.as_slice(), page_size)
    }

    /// Reads the header, reconciling the layout against the file's length and writing back a
    /// repair if one was needed. A shared reader does neither: see `read_shared_header()`.
    fn read_header(
        storage: &NativePageStorage,
        in_process_header_lock: &Mutex<()>,
        concurrency_mode: ConcurrencyMode,
        page_size: usize,
        read_only: bool,
    ) -> Result<DatabaseHeader, DatabaseError> {
        if read_only && concurrency_mode.is_multi_process_writable() {
            return Self::read_shared_header(
                storage,
                in_process_header_lock,
                concurrency_mode,
                page_size,
            );
        }
        let unrepaired = {
            let header_lock =
                Self::lock_header(storage, in_process_header_lock, concurrency_mode, false)?;
            Self::read_file_header(storage, page_size.try_into().unwrap(), &header_lock)?
        };
        let file_len = storage.raw_file_len()?;
        let needs_recovery = unrepaired.recovery_required(file_len);
        if needs_recovery && read_only {
            return Err(DatabaseError::RepairAborted);
        }
        let (header, _) = unrepaired.finalize(file_len)?;
        if needs_recovery {
            let _guard =
                Self::lock_header(storage, in_process_header_lock, concurrency_mode, true)?;
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }
        assert_eq!(header.layout().len(), storage.raw_file_len()?);

        Ok(header)
    }

    /// Reads the header for a shared reader. It consults neither the layout nor the file's length:
    /// it never allocates, and reads pages by the immutable geometry alone. A writer takes
    /// `CONSISTENT_BYTE` before it publishes the recovery flag, under its exclusive hold, and
    /// holds the byte while the flag means a live writer, so the flag is never found without it.
    fn read_shared_header(
        storage: &NativePageStorage,
        in_process_header_lock: &Mutex<()>,
        concurrency_mode: ConcurrencyMode,
        page_size: usize,
    ) -> Result<DatabaseHeader, DatabaseError> {
        let (unrepaired, consistent) = {
            let header_lock =
                Self::lock_header(storage, in_process_header_lock, concurrency_mode, false)?;
            (
                Self::read_file_header(storage, page_size.try_into().unwrap(), &header_lock)?,
                storage.query_lock_range(byte_range(CONSISTENT_BYTE))?,
            )
        };
        // An unclean file is a live writer's to repair, if there is one
        if unrepaired.unclean() && !consistent {
            return Err(DatabaseError::RepairAborted);
        }
        Ok(unrepaired.finalize_transaction_slots()?)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_ordinary_writer_cache(
        &self,
        tracker: &crate::transaction_tracker::TransactionTracker,
        guard: &crate::db::TransactionGuard,
    ) -> Result<()> {
        let allocator_storage = {
            let state = self.state.lock().unwrap();
            matches!(&state.allocators, AllocatorState::Loaded(allocators) if allocators.has_source_storage())
        };
        #[cfg(debug_assertions)]
        let debug_storage = {
            let allocated = self.allocated_pages.lock().unwrap().has_source_storage();
            let dirty = self.open_dirty_pages.lock().unwrap().has_source_storage();
            allocated || dirty
        };
        #[cfg(not(debug_assertions))]
        let debug_storage = false;
        // Independent physical families: any earlier family handoff may have
        // completed before a later reserve failed. All remaining owners count.
        if !self.storage.has_source_write_metadata() && !allocator_storage && !debug_storage {
            return Ok(());
        }
        tracker.require_ordinary_cache_handoff_writer(self, guard)?;
        self.storage.handoff_write_metadata_to_ordinary()?;
        // No nested cache/state locking. Exact Live ordinary writer remains
        // held; no namespace/table aliases exist at this constructor boundary.
        {
            let mut state = self.state.lock().unwrap();
            if let AllocatorState::Loaded(allocators) = &mut state.allocators {
                allocators.handoff_ordinary()?;
            }
        }
        #[cfg(debug_assertions)]
        {
            self.allocated_pages.lock().unwrap().handoff_ordinary()?;
            self.open_dirty_pages.lock().unwrap().handoff_ordinary()?;
        }
        Ok(())
    }

    #[cfg(not(redb_no_std))]
    fn before_debug_page_mutation(
        &self,
        resources: Option<&super::page_resources::NativePageResources>,
        new_page: bool,
        required: bool,
    ) -> Result<()> {
        #[cfg(debug_assertions)]
        {
            let allocated_present = self.allocated_pages.lock().unwrap().has_source_storage();
            let dirty_present = self.open_dirty_pages.lock().unwrap().has_source_storage();
            let required = required || allocated_present || dirty_present;
            // Partial preparation never grants a mixed Source/ordinary route.
            // Both families must be installed on the actual same pool before
            // any subsequent prepared operation, even in a numerical fixture.
            self.allocated_pages
                .lock()
                .unwrap()
                .before_mutation(resources, new_page, required)?;
            self.open_dirty_pages
                .lock()
                .unwrap()
                .before_mutation(resources, true, required)?;
        }
        #[cfg(not(debug_assertions))]
        let _ = (resources, new_page, required);
        Ok(())
    }

    // Closed actual native writer only. The complete fixed plan must derive
    // BOTH demands from actual recipe/old frontier/pre+post/adopted tail. This
    // component deliberately supplies no guessed production-demand factory.
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_source_debug_families(
        &self,
        tracker: &crate::transaction_tracker::TransactionTracker,
        guard: &crate::db::TransactionGuard,
        resources: &super::page_resources::NativePageResources,
        additional_new_pages: usize,
        concurrent_mutable_pages: usize,
    ) -> Result<NativePreparedDebugFamilies> {
        if !resources.is_source_write() {
            return Err(super::page_resources::refused());
        }
        let profile = tracker.check_source_no_savepoint_writer(self, guard)?;
        #[cfg(debug_assertions)]
        {
            if !self.open_dirty_pages.lock().unwrap().is_empty() {
                return Err(super::page_resources::refused());
            }
            self.allocated_pages
                .lock()
                .unwrap()
                .prepare_source(additional_new_pages, resources)?;
            self.open_dirty_pages
                .lock()
                .unwrap()
                .prepare_source(concurrent_mutable_pages, resources)?;
        }
        #[cfg(not(debug_assertions))]
        let _ = (additional_new_pages, concurrent_mutable_pages);
        Ok(NativePreparedDebugFamilies {
            actual_memory: core::ptr::from_ref(self) as usize,
            profile,
            pool: resources.admit_control(0)?,
        })
    }

    pub(crate) fn owns_exclusive_writer_lock(&self, writer: &Arc<WriterLock>) -> bool {
        self.concurrency_mode == ConcurrencyMode::ExclusiveWriter
            && self
                .writer_lock
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, writer))
    }

    /// The one the open took, or the writer byte a write transaction takes in multi-writer
    /// mode, waiting on the peer that holds it. Taken after the in-process write slot: locks on
    /// that byte from this file description are one lock, so the slot is what orders them
    pub(crate) fn lock_writer(&self) -> Result<Arc<WriterLock>> {
        if let Some(from_open) = &self.writer_lock {
            return Ok(from_open.clone());
        }
        self.storage.lock_range(byte_range(WRITER_BYTE))?;

        Ok(Arc::new(WriterLock {
            storage: self.storage.clone(),
            range: Some(byte_range(WRITER_BYTE)),
        }))
    }

    fn active_transaction_byte(id: TransactionId) -> Result<u64> {
        match TXN_BASE.checked_add(id.raw_id()) {
            Some(offset) if offset < 1 << 63 => Ok(offset),
            _ => Err(StorageError::Corrupted(format!(
                "transaction id {} is outside the multi-process lock range",
                id.raw_id()
            ))),
        }
    }

    /// Checks that `id` is one this file's locks can announce. A persistent savepoint takes no
    /// "active transaction byte", so nothing else checks the ids the file names.
    pub(crate) fn check_active_transaction_id(&self, id: TransactionId) -> Result {
        if self.concurrency_mode.is_multi_process_writable() {
            Self::active_transaction_byte(id)?;
        }
        Ok(())
    }

    pub(crate) fn lock_header_shared(&self) -> Result<HeaderGuard<'_>> {
        Self::lock_header(
            &self.storage,
            &self.in_process_header_lock,
            self.concurrency_mode,
            false,
        )
    }

    pub(crate) fn lock_header_exclusive(&self) -> Result<HeaderGuard<'_>> {
        Self::lock_header(
            &self.storage,
            &self.in_process_header_lock,
            self.concurrency_mode,
            true,
        )
    }

    /// The exclusive header hold for one operation: `lent` where the caller holds it already, and
    /// a hold of its own otherwise
    pub(crate) fn header_hold<'a>(
        &'a self,
        lent: Option<&'a HeaderGuard<'a>>,
    ) -> Result<HeaderHold<'a>> {
        Ok(match lent {
            Some(guard) => HeaderHold::Lent(guard),
            None => HeaderHold::Own(self.lock_header_exclusive()?),
        })
    }

    /// The oldest transaction still being read in any process, given `local`, the oldest this
    /// process reads. Where the file is shared it is found under the caller's exclusive header
    /// hold, which every registration waits behind, so no pin lands below what this reports.
    pub(crate) fn oldest_active_transaction(
        &self,
        local: Option<TransactionId>,
        _header_lock: &HeaderGuard<'_>,
    ) -> Result<Option<TransactionId>> {
        if self.concurrency_mode.is_multi_process_writable() {
            let ceiling = self.get_last_committed_transaction_id()?;
            // Every id scanned is at most the ceiling, so this bounds the whole scan's offsets
            Self::active_transaction_byte(ceiling)?;
            let mut scan_from = self.scan_from.lock().unwrap();
            // Only a byte below this process's own oldest read can lower the answer, and only
            // there is a held byte another process's: on Windows a probe sees this description's
            // own locks too
            let high = local.map_or(Some(ceiling.raw_id()), |id| id.raw_id().checked_sub(1));
            let peer = match high {
                Some(high) => active_transactions::lowest_held(
                    |range| self.storage.query_lock_range(range),
                    *scan_from,
                    high,
                )?,
                None => None,
            };
            let oldest = peer.map(TransactionId::new).or(local);
            *scan_from = oldest.unwrap_or(ceiling).raw_id();
            return Ok(oldest);
        }
        Ok(local)
    }

    /// Marks `id` active, keeping the pages its snapshot references from being reclaimed by any
    /// process until [`Self::unlock_mp_transaction`]. The caller's shared header hold orders this
    /// against a writer's reclamation scan, which holds it exclusively: the scan either sees this
    /// lock or ran entirely before `id` was read, so the hold must cover that read too.
    ///
    /// Caller must not attempt to re-lock an already locked transaction.
    ///
    /// The caller must guarantee that `id` cannot already have been collected.
    pub(crate) fn lock_mp_transaction(
        &self,
        id: TransactionId,
        _header: &HeaderGuard<'_>,
    ) -> Result {
        // Nothing to publish to: an exclusive writer locks the whole file
        if !self.concurrency_mode.is_multi_process_writable() {
            return Ok(());
        }
        let byte = Self::active_transaction_byte(id)?;
        // Refused only by an exclusive holder, which the caller's shared header hold excludes
        if !self.storage.try_lock_shared_range(byte_range(byte))? {
            return Err(StorageError::Corrupted(
                "another process holds an active transaction byte exclusively".to_string(),
            ));
        }

        Ok(())
    }

    /// Releases the byte. Taken without the header lock: releasing only widens what a scanning
    /// writer may reclaim, and a scan that misses it reclaims less
    pub(crate) fn unlock_mp_transaction(&self, id: TransactionId) -> Result {
        if !self.concurrency_mode.is_multi_process_writable() {
            return Ok(());
        }
        self.storage
            .unlock_range(byte_range(Self::active_transaction_byte(id)?))?;

        Ok(())
    }

    /// Returns the memory and if opened for writing, the writer lock, which should be held if
    /// repair is needed to complete the opening process
    pub(crate) fn new(
        file: Box<dyn StorageBackend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<(Self, Option<Arc<WriterLock>>), DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);

        Self::new_read_resources(
            file,
            allow_initialize,
            page_size,
            requested_region_size,
            cache_size,
            read_only,
            concurrency_mode,
            None,
            None,
        )
    }

    pub(crate) fn new_read_resources(
        file: Box<dyn StorageBackend>,
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
        page_resources: Option<super::page_resources::NativePageResources>,
        read_resource_profile: Option<super::read_only_resource::ReadOnlyResourceProfile>,
    ) -> Result<(Self, Option<Arc<WriterLock>>), DatabaseError> {
        Self::new_read_resources_inner(
            file,
            allow_initialize,
            page_size,
            requested_region_size,
            cache_size,
            read_only,
            concurrency_mode,
            page_resources,
            read_resource_profile,
            #[cfg(not(redb_no_std))]
            false,
        )
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn new_prepaid_read_resources(
        file: Box<dyn StorageBackend>,
        page_size: usize,
        resources: &super::page_resources::NativePageResources,
        profile: Option<super::read_only_resource::ReadOnlyResourceProfile>,
    ) -> Result<Self, DatabaseError> {
        if !page_size.is_power_of_two()
            || page_size < DB_HEADER_SIZE
            || u32::try_from(page_size).is_err()
        {
            return Err(super::page_resources::refused().into());
        }
        resources
            .seal_fixed_observation_admission()?
            .require_current()?;
        let (memory, writer) = Self::new_read_resources_inner(
            file,
            false,
            page_size,
            None,
            0,
            true,
            ConcurrencyMode::ExclusiveWriter,
            Some(resources.clone()),
            profile,
            true,
        )?;
        debug_assert!(writer.is_none());
        Ok(memory)
    }
    fn new_read_resources_inner(
        file: Box<dyn StorageBackend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
        page_resources: Option<super::page_resources::NativePageResources>,
        read_resource_profile: Option<super::read_only_resource::ReadOnlyResourceProfile>,
        #[cfg(not(redb_no_std))] prepaid: bool,
    ) -> Result<(Self, Option<Arc<WriterLock>>), DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);

        let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
        let region_size = min(
            region_size,
            (u64::from(MAX_PAGE_INDEX) + 1) * page_size as u64,
        );
        assert!(region_size.is_power_of_two());

        #[cfg(not(redb_no_std))]
        let prepaid_read_resources = if prepaid {
            page_resources.clone()
        } else {
            None
        };
        #[cfg(not(redb_no_std))]
        let storage = if prepaid {
            let resources = page_resources
                .as_ref()
                .ok_or_else(super::page_resources::refused)?;
            NativePageStorage::prepaid_readonly(file, page_size as u64, resources.clone())?
        } else {
            NativePageStorage::ordinary(Arc::new(PagedCachedFile::with_read_resources(
                file,
                page_size as u64,
                cache_size,
                page_resources,
                read_resource_profile.is_some(),
            )))
        };
        #[cfg(redb_no_std)]
        let storage = NativePageStorage::ordinary(Arc::new(PagedCachedFile::with_read_resources(
            file,
            page_size as u64,
            cache_size,
            page_resources,
            read_resource_profile.is_some(),
        )));
        // Dropping the storage releases whatever this took, so an open that fails below does too
        Self::lock_for_open(&storage, read_only, concurrency_mode)?;
        let writer_lock = database_writer_lock_for_mode(&storage, read_only, concurrency_mode);
        // The lock the open() runs under: its own where it took one, and the handle's otherwise
        // Taken before anything is read, so that the header the open reads is the header it
        // repairs and commits over
        let open_writer_lock =
            open_writer_lock_for_mode(&storage, read_only, concurrency_mode, writer_lock.clone())?;
        let in_process_header_lock = Mutex::new(());

        // Ensure that we have a consistent view across reading the file length and metadata
        let init_guard = Self::lock_header(
            &storage,
            &in_process_header_lock,
            concurrency_mode,
            allow_initialize,
        )?;

        let initial_storage_len = storage.raw_file_len()?;

        let magic_number: [u8; MAGICNUMBER.len()] =
            if initial_storage_len >= MAGICNUMBER.len() as u64 {
                storage
                    .read_direct(0, MAGICNUMBER.len())?
                    .as_slice()
                    .try_into()
                    .unwrap()
            } else {
                [0; MAGICNUMBER.len()]
            };

        if initial_storage_len > 0 {
            // File already exists check that the magic number matches
            if magic_number != MAGICNUMBER {
                return Err(StorageError::Io(io::invalid_data(
                    "Not a redb database: magic number mismatch",
                ))
                .into());
            }
        } else {
            // File is empty, check that we're allowed to initialize a new database (i.e. the caller is Database::create() and not open())
            if !allow_initialize {
                return Err(StorageError::Io(io::invalid_data(
                    "Database file is empty and creating a new database was not requested",
                ))
                .into());
            }
        }

        if magic_number != MAGICNUMBER {
            let region_tracker_required_bytes =
                RegionTracker::new(INITIAL_REGIONS, MAX_MAX_PAGE_ORDER + 1)
                    .to_vec()
                    .len();

            // Make sure that there is enough room to allocate the region tracker into a page
            let size: u64 = max(
                MIN_DESIRED_USABLE_BYTES,
                page_size as u64 * u64::from(MIN_USABLE_PAGES),
            );
            let tracker_space =
                (page_size * region_tracker_required_bytes.div_ceil(page_size)) as u64;
            let starting_size = size + tracker_space;

            let page_capacity = (region_size / u64::try_from(page_size).unwrap())
                .try_into()
                .unwrap();
            let layout = DatabaseLayout::calculate(
                starting_size,
                page_capacity,
                NO_HEADER,
                page_size.try_into().unwrap(),
            );

            {
                let file_len = storage.raw_file_len()?;

                if file_len < layout.len() {
                    storage.resize(layout.len())?;
                }
            }

            let mut header = DatabaseHeader::new(layout, TransactionId::new(0));

            header.recovery_required = false;
            header.two_phase_commit = true;
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(false));
            storage.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }
        // Given up before the read below takes it again: neither hold nests
        drop(init_guard);
        let header = Self::read_header(
            &storage,
            &in_process_header_lock,
            concurrency_mode,
            page_size,
            read_only,
        )?;

        let layout = header.layout();
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;
        let state = InMemoryState::new(header);

        assert!(page_size >= DB_HEADER_SIZE);

        #[cfg(debug_assertions)]
        let (read_page_ref_counts, readonly_debug_hash_capacity_upper) = {
            let counts = PageNumberHashMap::default();
            #[cfg(not(redb_no_std))]
            let counts = {
                let mut counts = counts;
                let live_slots = read_resource_profile
                    .map(|profile| profile.max_live_pages)
                    .or_else(|| storage.bootstrap_live_page_slots());
                if let Some(max_live_pages) = live_slots {
                    let reserve = max_live_pages
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(super::page_resources::refused)?;
                    if prepaid {
                        counts
                            .try_reserve(reserve)
                            .map_err(|_| super::page_resources::refused())?;
                    } else {
                        counts.reserve(reserve);
                    }
                }
                counts
            };
            // This is the historical FRESH capacity. Later erases may lower
            // HashMap::capacity without releasing its actual allocation.
            #[cfg(not(redb_no_std))]
            let capacity = counts.capacity();
            #[cfg(redb_no_std)]
            let capacity = 0;
            (Arc::new(Mutex::new(counts)), capacity)
        };
        let mem = Self {
            read_observations: super::read_only_resource::ReadOnlyReadObservations::default(),
            read_resource_profile,
            allocator_reopen_component_facts: Mutex::new(None),
            #[cfg(not(redb_no_std))]
            prepaid_read_resources,
            #[cfg(not(redb_no_std))]
            prepaid_definition_work: Mutex::new(None),
            unpersisted: Mutex::new(UnpersistedState::default()),
            storage,
            state: Mutex::new(state),
            #[cfg(debug_assertions)]
            open_dirty_pages: Arc::new(Mutex::new(NativeDebugPageSet::default())),
            #[cfg(debug_assertions)]
            read_page_ref_counts,
            #[cfg(debug_assertions)]
            readonly_debug_hash_capacity_upper,
            #[cfg(debug_assertions)]
            allocated_pages: Arc::new(Mutex::new(NativeDebugPageSet::default())),
            needs_repair: AtomicBool::new(false),
            concurrency_mode,
            read_only,
            in_process_header_lock,
            scan_from: Mutex::new(0),
            writer_lock,
            page_size: page_size.try_into().unwrap(),
            region_size,
            region_header_with_padding_size: region_header_size,
        };

        Ok((mem, open_writer_lock))
    }

    fn supported_page_order(page: PageNumber) -> bool {
        page.page_order <= MAX_MAX_PAGE_ORDER
    }

    // Call only after the native order check. A u32 index+1 shifted by the
    // native maximum order <=20 fits u64; no untrusted address is opened.
    fn page_number_region_failure(
        page: PageNumber,
        layout: &DatabaseLayout,
    ) -> Option<NativePageNumberRegionFailure> {
        if page.region >= layout.num_regions() {
            return Some(NativePageNumberRegionFailure::MissingRegion {
                regions: layout.num_regions(),
            });
        }
        let region_pages = u64::from(layout.region_layout(page.region).num_pages());
        let end_page = (u64::from(page.page_index) + 1) << page.page_order;
        if end_page > region_pages {
            return Some(NativePageNumberRegionFailure::OutsideRegion { region_pages });
        }
        None
    }

    // An order read from a corrupted file would otherwise size a multi-terabyte read buffer, whose
    // failed allocation aborts the process instead of returning an error.
    fn check_page_order(page: PageNumber) -> Result<()> {
        if !Self::supported_page_order(page) {
            return Err(StorageError::Corrupted(format!(
                "Page {page:?} has order greater than the maximum of {MAX_MAX_PAGE_ORDER}"
            )));
        }
        Ok(())
    }

    pub(crate) fn cache_stats(&self) -> CacheStats {
        self.storage.cache_stats()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.storage.check_io_errors()
    }

    // Panics in debug builds if any `PageMut` handed out by `get_page_mut` or
    // `allocate*` has not yet been dropped. Intended as a precondition for
    // commit/abort paths, which assume no mutable page references remain.
    pub(crate) fn debug_assert_no_dirty_pages(&self) {
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(
                dirty_pages.is_empty(),
                "Dirty pages outstanding: {dirty_pages:?}"
            );
        }
    }

    #[cfg(debug_assertions)]
    pub(crate) fn mark_debug_allocated_page(&self, page: PageNumber) {
        assert!(self.allocated_pages.lock().unwrap().insert(page));
    }

    #[cfg(debug_assertions)]
    #[cfg_attr(redb_no_std, expect(dead_code))]
    pub(crate) fn all_allocated_pages(&self) -> Vec<PageNumber> {
        self.allocated_pages
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    #[cfg(debug_assertions)]
    #[cfg_attr(redb_no_std, expect(dead_code))]
    pub(crate) fn debug_check_allocator_consistency(&self) {
        let state = self.state.lock().unwrap();
        let allocators = state.allocators();
        let mut region_pages = vec![vec![]; allocators.region_allocators.len()];
        for p in self.allocated_pages.lock().unwrap().iter() {
            region_pages[p.region as usize].push(*p);
        }
        for (i, allocator) in allocators.region_allocators.iter().enumerate() {
            allocator.check_allocated_pages(i.try_into().unwrap(), &region_pages[i]);
        }
    }

    pub(crate) fn clear_read_cache(&self) {
        self.storage.invalidate_cache_all();
    }

    pub(crate) fn clear_cache_and_reload(&mut self) -> Result<(bool, bool), DatabaseError> {
        // The in-memory state is being discarded for the on-disk state, so buffered writes --
        // which can only belong to the discarded state -- are dropped rather than written out;
        // after an external truncation, writing them could even fail beyond the end of the file.
        // Both caches are cleared before the fallible sync, so an early error return cannot
        // leave cached pages that disagree with the file.
        self.storage.discard_write_buffer();
        self.storage.invalidate_cache_all();
        self.storage.sync_file()?;

        let unrepaired = {
            let header_lock = self.lock_header_shared()?;
            Self::read_file_header(&self.storage, self.page_size, &header_lock)?
        };
        let (header, was_clean) = unrepaired.finalize(self.storage.raw_file_len()?)?;
        if !was_clean {
            let header_lock = self.lock_header_exclusive()?;
            self.write_header(&header, &header_lock)?;
            self.storage.flush()?;
        }

        let changed = {
            let mut state = self.state.lock().unwrap();
            let changed =
                header.primary_slot().transaction_id != state.header.primary_slot().transaction_id;
            state.header = header;
            state.read_from_secondary = false;
            // Drop the previous allocator state -- it described the layout that was in memory
            // before the reload. The caller is required to repopulate it (via reset_allocator_state or
            // load_allocator_state) before any allocation/free path runs.
            state.allocators = AllocatorState::None;
            changed
        };
        // Reloading from disk discards in-memory roots, so drop volatile allocation state
        // that belonged only to those roots.
        self.unpersisted.lock().unwrap().clear();

        Ok((was_clean, changed))
    }

    /// Captures the committed id and data root together, reading the file when a peer may have
    /// committed. The caller's `header` hold must also cover registering the returned id as active.
    pub(crate) fn latest_committed_snapshot(
        &self,
        header: &HeaderGuard<'_>,
    ) -> Result<(TransactionId, Option<BtreeHeader>)> {
        if self.externally_writable() {
            return self.read_snapshot_from_file(header);
        }
        // Local commits can publish their in-memory state without holding the header lock.
        let state = self.state.lock()?;
        let slot = state.latest_slot();
        Ok((slot.transaction_id, slot.user_root))
    }

    /// Whether another process may commit to this file
    fn externally_writable(&self) -> bool {
        match self.concurrency_mode {
            ConcurrencyMode::ExclusiveWriter => false,
            ConcurrencyMode::SingleWriter => self.read_only,
            ConcurrencyMode::MultiWriter => true,
        }
    }

    /// Reads a snapshot without changing the in-memory header or allocator state.
    fn read_snapshot_from_file(
        &self,
        header: &HeaderGuard<'_>,
    ) -> Result<(TransactionId, Option<BtreeHeader>)> {
        let unrepaired = Self::read_file_header(&self.storage, self.page_size, header)
            .map_err(DatabaseError::into_storage_error_or_corrupted)?;
        let file_header = unrepaired.finalize_transaction_slots()?;
        let slot = file_header.primary_slot();
        self.storage.update_transaction_id(slot.transaction_id);

        Ok((slot.transaction_id, slot.user_root))
    }

    /// Brings this handle up to the file's latest commit, when it is not already on it, so that
    /// a write transaction starts from whatever another process committed last. The cached
    /// pages, the buffered writes and the allocator state all describe the commit being left
    /// behind, so they are dropped here; the caller then loads the allocator state that the new
    /// commit recorded. Returns whether anything was synced.
    ///
    /// `writer_lock` and `header_lock` prove that the caller holds the writer byte and the
    /// header lock, so that no other process commits while this reads the file.
    pub(crate) fn reload_for_write(
        &self,
        _writer_lock: &WriterLock,
        header_lock: &HeaderGuard<'_>,
    ) -> Result<bool> {
        let unrepaired = Self::read_file_header(&self.storage, self.page_size, header_lock)
            .map_err(DatabaseError::into_storage_error_or_corrupted)?;
        let (header, _) = unrepaired.finalize(self.storage.raw_file_len()?)?;
        // Every writer holds the writer byte while it reloads, so transaction ids only move
        // forward, and comparing them says whether the file has moved on. A latched repair
        // syncs whatever the ids say: the pages it leaked are this handle's alone, and the
        // allocator state the file's latest commit recorded does not include them.
        if header.primary_slot().transaction_id == self.get_last_durable_transaction_id()?
            && !self.needs_repair()
        {
            return Ok(false);
        }

        // Discarding the write buffer cannot lose a commit: only a non-durable commit leaves
        // its pages buffered, and the multi-process modes refuse `Durability::None`. What can
        // be buffered here are the writes of a transaction that was dropped while a panic
        // unwound, which never committed, over pages the other process may have reused since.
        assert!(!self.storage.has_committed_pages_buffered());
        self.storage.discard_write_buffer();
        // The other process may have rewritten any page this handle has cached
        self.storage.invalidate_cache_all();
        {
            let mut state = self.state.lock().unwrap();
            state.header = header;
            state.read_from_secondary = false;
            state.allocators = AllocatorState::None;
        }
        // Mirrors the allocator state, which was just dropped, so it is rebuilt along with it
        #[cfg(debug_assertions)]
        self.allocated_pages.lock().unwrap().clear();

        Ok(true)
    }

    pub(crate) fn begin_writable(&self) -> Result {
        let header_lock = self.lock_header_exclusive()?;
        let mut state = self.state.lock().unwrap();
        assert!(!state.header.recovery_required);
        state.header.recovery_required = true;
        self.write_header(&state.header, &header_lock)?;
        self.storage.flush()
    }

    /// Whether another process may have this database open while one process writes.
    pub(crate) fn concurrency_mode(&self) -> ConcurrencyMode {
        self.concurrency_mode
    }

    pub(crate) fn used_two_phase_commit(&self) -> bool {
        self.state.lock().unwrap().header.two_phase_commit
    }

    pub(crate) fn allocator_hash(&self) -> u128 {
        self.state.lock().unwrap().allocators().xxh3_hash()
    }

    // Reports whether the backend has seen an I/O failure in this process.
    // Callers use this to skip cleanup that would do further I/O after a
    // previous storage error (e.g. WriteTransaction::drop).
    pub(crate) fn storage_failure(&self) -> bool {
        self.storage.check_io_errors().is_err()
    }

    pub(crate) fn repair_primary_corrupted(&self) {
        let mut state = self.state.lock().unwrap();
        state.header.swap_primary_slot();
    }

    // Replaces the in-memory allocator state with a fresh, empty one sized to the current
    // layout. The caller is responsible for repopulating it by marking reachable pages allocated.
    pub(crate) fn reset_allocator_state(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.allocators = AllocatorState::Loaded(Allocators::new(state.header.layout()));
        #[cfg(debug_assertions)]
        self.allocated_pages.lock().unwrap().clear();

        Ok(())
    }

    // Whether no transaction has ever committed to this file: the header is the one its
    // initialization wrote
    pub(crate) fn never_committed(&self) -> bool {
        self.state.lock().unwrap().header.never_committed()
    }

    // The allocator state of a database in which no transaction has ever committed: that of the
    // empty layout, which is what rebuilding it from the empty trees would produce. There is
    // nothing to recover, so the recovery flag is cleared as loading a saved state clears it
    pub(crate) fn load_empty_allocator_state(&self) -> Result<()> {
        self.reset_allocator_state()?;
        self.state.lock().unwrap().header.recovery_required = false;

        Ok(())
    }

    // Discards an allocator state that no longer describes the file. Callers that allocate or free
    // must check for one first, since those paths have no way to work without it.
    //
    // Runs during panic unwinding, so it must tolerate poisoned locks rather than double-panic.
    // The poison is deliberately left set: subsequent lock users fail rather than trusting state
    // touched by a panicking thread.
    pub(crate) fn invalidate_allocator_state(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(crate::sync::PoisonError::into_inner);
        state.allocators = AllocatorState::Invalidated;
        drop(state);
        #[cfg(debug_assertions)]
        self.allocated_pages
            .lock()
            .unwrap_or_else(crate::sync::PoisonError::into_inner)
            .clear();
    }

    #[cfg(test)]
    pub(crate) fn allocator_state_loaded(&self) -> bool {
        self.state.lock().unwrap().allocators.is_loaded()
    }

    /// Whether an interrupted operation invalidated the allocator state.
    /// Unlike `None` after a header reload, this requires reopening before writing.
    pub(crate) fn allocator_state_invalidated(&self) -> bool {
        self.state.lock().unwrap().allocators.is_invalidated()
    }

    pub(crate) fn mark_needs_repair(&self) {
        self.needs_repair.store(true, Ordering::Release);
    }

    pub(crate) fn needs_repair(&self) -> bool {
        self.needs_repair.load(Ordering::Acquire)
    }

    // The in-memory allocator state has been rebuilt from the committed trees, so it can be
    // trusted again
    pub(crate) fn clear_needs_repair(&self) {
        self.needs_repair.store(false, Ordering::Release);
    }

    // The freed tables name pages that no page walk ever reads, so this is the only place those
    // page numbers are validated.
    pub(crate) fn mark_page_allocated(&self, page_number: PageNumber) -> Result<()> {
        Self::check_page_order(page_number)?;
        let mut state = self.state.lock()?;
        // Unlike the read path, this is only reached while rebuilding the allocator state, and the
        // state lock is already held, so validating against the layout costs nothing here
        let layout = state.header.layout();
        match Self::page_number_region_failure(page_number, &layout) {
            Some(NativePageNumberRegionFailure::MissingRegion { regions }) => {
                return Err(StorageError::Corrupted(format!(
                    "Page {page_number:?} is in region {}, but the database has {} region(s)",
                    page_number.region, regions
                )));
            }
            Some(NativePageNumberRegionFailure::OutsideRegion { region_pages }) => {
                return Err(StorageError::Corrupted(format!(
                    "Page {page_number:?} extends past the end of its region, which has {region_pages} pages"
                )));
            }
            None => {}
        }

        let allocator = state.get_region_mut(page_number.region);
        if !allocator.record_alloc(page_number.page_index, page_number.page_order) {
            return Err(StorageError::Corrupted(format!(
                "Page {page_number:?} overlaps a page that is already allocated"
            )));
        }
        #[cfg(debug_assertions)]
        assert!(self.allocated_pages.lock().unwrap().insert(page_number));

        Ok(())
    }

    /// Writes the header under the caller's exclusive header hold, which is what keeps another
    /// process from reading it torn where the file is shared.
    fn write_header(&self, header: &DatabaseHeader, _header_lock: &HeaderGuard<'_>) -> Result {
        if self.concurrency_mode.is_multi_process_writable() {
            // Written directly rather than through the write buffer, so that the hold covers the
            // bytes reaching the file
            return self.storage.write_direct(0, &header.to_bytes(true));
        }
        self.storage
            .write(0, DB_HEADER_SIZE, true)?
            .mem_mut()
            .copy_from_slice(&header.to_bytes(true));

        Ok(())
    }

    // Sets the recovery flag that a live writer keeps in its header. Loading the allocator
    // state clears that flag, so a handle that has just synced must set it again.
    pub(crate) fn mark_recovery_required(&self) {
        self.state.lock().unwrap().header.recovery_required = true;
    }

    // Durably clears the recovery flag, marking the repair as complete.
    pub(crate) fn clear_recovery_required(&self) -> Result<()> {
        let header_lock = self.lock_header_exclusive()?;
        let mut state = self.state.lock().unwrap();
        state.header.recovery_required = false;
        self.write_header(&state.header, &header_lock)?;
        self.storage.flush()?;
        Ok(())
    }

    pub(crate) fn reserve_allocator_state(
        &self,
        tree: &mut AllocatorStateTreeMut,
        transaction_id: TransactionId,
    ) -> Result<u32> {
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let num_regions = layout.num_regions();
        let allocators = state.allocators();
        let region_tracker_len = allocators.region_tracker.to_vec().len();
        let region_lens: Vec<usize> = allocators
            .region_allocators
            .iter()
            .map(|x| x.to_vec().len())
            .collect();
        drop(state);

        for i in 0..num_regions {
            let region_bytes_len = region_lens[i as usize];
            tree.insert(
                &AllocatorStateKey::Region(i),
                &vec![0; region_bytes_len].as_ref(),
            )?;
        }

        tree.insert(
            &AllocatorStateKey::RegionTracker,
            &vec![0; region_tracker_len].as_ref(),
        )?;

        tree.insert(
            &AllocatorStateKey::TransactionId,
            &transaction_id.raw_id().to_le_bytes().as_ref(),
        )?;

        Ok(num_regions)
    }

    // Returns true on success, or false if the number of regions has changed
    pub(crate) fn try_save_allocator_state(
        &self,
        tree: &mut AllocatorStateTreeMut,
        num_regions: u32,
    ) -> Result<bool> {
        // Has the number of regions changed since reserve_allocator_state() was called?
        let state = self.state.lock().unwrap();
        if num_regions != state.header.layout().num_regions() {
            return Ok(false);
        }

        let allocators = state.allocators();
        for i in 0..num_regions {
            let region_bytes = &allocators.region_allocators[i as usize].to_vec();
            if tree
                .get(&AllocatorStateKey::Region(i))?
                .unwrap()
                .value()
                .len()
                < region_bytes.len()
            {
                // The allocator state grew too much since we reserved space
                return Ok(false);
            }
            tree.insert_inplace(&AllocatorStateKey::Region(i), &region_bytes.as_ref())?;
        }

        let region_tracker_bytes = allocators.region_tracker.to_vec();
        if tree
            .get(&AllocatorStateKey::RegionTracker)?
            .unwrap()
            .value()
            .len()
            < region_tracker_bytes.len()
        {
            // The allocator state grew too much since we reserved space
            return Ok(false);
        }
        tree.insert_inplace(
            &AllocatorStateKey::RegionTracker,
            &region_tracker_bytes.as_ref(),
        )?;

        Ok(true)
    }

    // Returns true if the allocator state table is up to date, or false if it's stale
    #[cfg(not(redb_no_std))]
    pub(crate) fn is_prepaid_read_construction(&self) -> bool {
        self.prepaid_read_resources.is_some()
    }
    #[cfg(not(redb_no_std))]
    fn prepare_prepaid_definition(
        &self,
        data: &[u8],
        name: &str,
        key: usize,
        value: usize,
    ) -> Result {
        let resources = self
            .prepaid_read_resources
            .as_ref()
            .ok_or_else(super::page_resources::refused)?;
        resources
            .seal_fixed_observation_admission()?
            .require_current()?;
        let mut slot = self.prepaid_definition_work.lock()?;
        // This private construction reads exactly one allocator definition. The
        // actual memory retains its work until all success/error values retire.
        if slot.is_some() {
            return Err(super::page_resources::refused());
        }
        let bytes = crate::tree_store::InternalTableDefinition::prepaid_decode_work_bytes(
            data, name, key, value,
        )?;
        *slot = Some(resources.admit_control(bytes)?);
        Ok(())
    }
    pub(crate) fn is_valid_allocator_state(&self, tree: &AllocatorStateTree) -> Result<bool> {
        // See if this is stale allocator state left over from a previous transaction. That won't
        // happen during normal operation, since WriteTransaction::commit() always updates the
        // allocator state table before calling TransactionalMemory::commit(), but there are also
        // a few places where TransactionalMemory::commit() is called directly without using a
        // WriteTransaction. When that happens, any existing allocator state table will be left
        // in place but is no longer valid. (And even if there were no such calls today, it would
        // be an easy mistake to make! So it's good that we check.)
        let Some(value) = tree.get(&AllocatorStateKey::TransactionId)? else {
            return Ok(false);
        };
        let transaction_id =
            TransactionId::new(u64::from_le_bytes(value.value().try_into().unwrap()));

        Ok(transaction_id == self.get_last_committed_transaction_id()?)
    }

    pub(crate) fn load_allocator_state(&self, tree: &AllocatorStateTree) -> Result {
        #[cfg(not(redb_no_std))]
        let result = self.load_allocator_state_inner(tree, None);
        #[cfg(redb_no_std)]
        let result = self.load_allocator_state_inner(tree);
        result
    }
    // Guarded allocator materialization only. This does not admit the earlier
    // cold header/cache/backend/tree-reader construction, or mint Source access.
    #[cfg(not(redb_no_std))]
    pub(crate) fn load_allocator_state_source(
        &self,
        tree: &AllocatorStateTree,
        resources: &super::page_resources::NativePageResources,
    ) -> Result {
        self.load_allocator_state_inner(tree, Some(resources))
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn inspect_allocator_state_source(
        &self,
        tree: &AllocatorStateTree,
    ) -> Result<(usize, u64)> {
        use super::buddy_allocator::BuddyAllocatorEncodedLayout;
        use super::container_bounds;
        use super::region::RegionTrackerEncodedLayout;
        let mut count = 0usize;
        let mut retained = 0u64;
        for region in
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))?
        {
            let region = region?;
            count = count
                .checked_add(1)
                .ok_or_else(super::page_resources::refused)?;
            let bound = BuddyAllocatorEncodedLayout::inspect(region.value())
                .and_then(|layout| layout.source_capacity_bound())
                .map_err(|_| super::page_resources::refused())?;
            retained = retained
                .checked_add(bound)
                .ok_or_else(super::page_resources::refused)?;
        }
        let tracker = tree
            .get(&AllocatorStateKey::RegionTracker)?
            .ok_or_else(super::page_resources::refused)?;
        let tracker = RegionTrackerEncodedLayout::inspect(tracker.value())
            .and_then(|layout| layout.source_capacity_bound())
            .map_err(|_| super::page_resources::refused())?;
        let outer = container_bounds::vector_storage::<BuddyAllocator>(count)
            .map_err(|_| super::page_resources::refused())?
            .allocator_bytes;
        let peak = retained
            .checked_add(outer)
            .and_then(|n| n.checked_add(tracker.simultaneous_peak_bytes))
            .ok_or_else(super::page_resources::refused)?;
        Ok((count, peak))
    }
    // Prospective allocator-VEC heap only, derived from this actual native
    // header target and the same persisted table. No Source/cold-open authority.
    #[cfg(not(redb_no_std))]
    pub(crate) fn inspect_allocator_state_source_reopen(
        &self,
        tree: &AllocatorStateTree,
    ) -> Result<(usize, u64, u64)> {
        let target = self.state.lock().unwrap().header.layout();
        let (count, quote) = self.inspect_allocator_state_source_resize(tree, target)?;
        // Copy numerical bounds only; the private container Growth type and
        // native owner stay inside page_store. These scalars grant no authority.
        Ok((
            count,
            quote.retained_after_bytes,
            quote.simultaneous_peak_bytes,
        ))
    }
    #[cfg(not(redb_no_std))]
    fn inspect_allocator_state_source_resize(
        &self,
        tree: &AllocatorStateTree,
        target: DatabaseLayout,
    ) -> Result<(usize, super::allocator_growth::Growth)> {
        use super::buddy_allocator::BuddyAllocatorEncodedLayout;
        use super::region::RegionTrackerEncodedLayout;
        use super::{allocator_growth as g, container_bounds as b};
        let (count, parser_peak) = self.inspect_allocator_state_source(tree)?;
        if count == 0 || target.num_regions() == 0 {
            return Err(super::page_resources::refused());
        }
        let target_count = target.num_regions() as usize;
        let mut prepared =
            g::parsed_source_vec_preparation::<BuddyAllocator>(count, count.max(target_count))
                .map_err(|_| super::page_resources::refused())?;
        let mut removed = 0u64;
        let mut index = 0usize;
        for region in
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))?
        {
            let region = region?;
            let layout = BuddyAllocatorEncodedLayout::inspect(region.value())
                .map_err(|_| super::page_resources::refused())?;
            let bound = if index < target_count {
                layout
                    .source_resize_capacity_bound(target.region_layout(index as u32).num_pages())
                    .map_err(|_| super::page_resources::refused())?
            } else {
                let old = layout
                    .source_capacity_bound()
                    .map_err(|_| super::page_resources::refused())?;
                removed = removed
                    .checked_add(old)
                    .ok_or_else(super::page_resources::refused)?;
                g::unchanged(old)
            };
            prepared = g::monotone_preparation(prepared, bound)
                .map_err(|_| super::page_resources::refused())?;
            index = index
                .checked_add(1)
                .ok_or_else(super::page_resources::refused)?;
        }
        if index != count {
            return Err(super::page_resources::refused());
        }
        let tracker = tree
            .get(&AllocatorStateKey::RegionTracker)?
            .ok_or_else(super::page_resources::refused)?;
        let tracker = RegionTrackerEncodedLayout::inspect(tracker.value())
            .and_then(|layout| layout.source_resize_capacity_bound(target.num_regions()))
            .map_err(|_| super::page_resources::refused())?;
        prepared = g::monotone_preparation(prepared, tracker)
            .map_err(|_| super::page_resources::refused())?;
        let added = target_count.saturating_sub(count);
        let parking = b::vector_storage::<BuddyAllocator>(added)
            .map_err(|_| super::page_resources::refused())?
            .allocator_bytes;
        prepared = g::monotone_preparation(prepared, g::unchanged(parking))
            .map_err(|_| super::page_resources::refused())?;
        for index in count..target_count {
            // This sole existing normal-constructor bound conservatively covers
            // Source's exact fresh Vec capacities. Retained is an UPPER bound.
            let new_region = BuddyAllocator::new_quote(
                target.region_layout(index as u32).num_pages(),
                target.full_region_layout().num_pages(),
            )
            .map_err(|_| super::page_resources::refused())?;
            prepared =
                g::monotone_preparation(prepared, g::unchanged(new_region.retained_after_bytes))
                    .map_err(|_| super::page_resources::refused())?;
        }
        let final_retained_upper = prepared
            .retained_after_bytes
            .checked_sub(removed)
            .and_then(|n| n.checked_sub(parking))
            .ok_or_else(super::page_resources::refused)?;
        Ok((
            count,
            g::Growth {
                retained_after_bytes: final_retained_upper,
                simultaneous_peak_bytes: parser_peak.max(prepared.simultaneous_peak_bytes),
            },
        ))
    }

    fn load_allocator_state_inner(
        &self,
        tree: &AllocatorStateTree,
        #[cfg(not(redb_no_std))] resources: Option<&super::page_resources::NativePageResources>,
    ) -> Result {
        let valid = self.is_valid_allocator_state(tree)?;
        #[cfg(not(redb_no_std))]
        if resources.is_some() && !valid {
            return Err(super::page_resources::refused());
        }
        assert!(valid);

        // Observe the ORIGINAL parser while maintenance already owns its work.
        // This is not pre-allocation admission. The future reopen factory must
        // reserve the complete resource plan before this construction begins.
        let initial_stamp = allocator_resource_stamp(&self.state.lock().unwrap().header);
        let mut facts = AllocatorParseRecorder::new();
        #[cfg(not(redb_no_std))]
        let inspected = if resources.is_some() {
            Some(self.inspect_allocator_state_source_reopen(tree)?)
        } else {
            None
        };
        #[cfg(not(redb_no_std))]
        let mut region_allocators = if let Some(resources) = resources {
            let count = inspected.as_ref().unwrap().0;
            super::owned_allocator_vec::NativeOwnedAllocatorVec::source(count, resources)?
        } else {
            super::owned_allocator_vec::NativeOwnedAllocatorVec::default()
        };
        #[cfg(redb_no_std)]
        let mut region_allocators = super::owned_allocator_vec::NativeOwnedAllocatorVec::default();
        #[cfg(not(redb_no_std))]
        if resources.is_some() {
            facts.replace_vec::<BuddyAllocator>(0, region_allocators.capacity());
        }
        for region in
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))?
        {
            let region = region?;
            facts.region_record(region.value().len());
            #[cfg(not(redb_no_std))]
            let allocator = if let Some(resources) = resources {
                if region_allocators.len() >= inspected.as_ref().unwrap().0 {
                    return Err(super::page_resources::refused());
                }
                BuddyAllocator::from_bytes_source_recording(
                    region.value(),
                    resources,
                    Some(&mut facts),
                )?
            } else {
                BuddyAllocator::from_bytes_recording(region.value(), Some(&mut facts))
            };
            #[cfg(redb_no_std)]
            let allocator = BuddyAllocator::from_bytes_recording(region.value(), Some(&mut facts));
            let old_capacity = region_allocators.capacity();
            region_allocators.push(allocator);
            facts.replace_vec::<BuddyAllocator>(old_capacity, region_allocators.capacity());
        }

        #[cfg(not(redb_no_std))]
        if resources.is_some() && region_allocators.len() != inspected.as_ref().unwrap().0 {
            return Err(super::page_resources::refused());
        }
        let tracker_value = tree.get(&AllocatorStateKey::RegionTracker)?;
        #[cfg(not(redb_no_std))]
        if resources.is_some() && tracker_value.is_none() {
            return Err(super::page_resources::refused());
        }
        let tracker_value = tracker_value.unwrap();
        facts.tracker_record(tracker_value.value().len());
        #[cfg(not(redb_no_std))]
        let region_tracker = if let Some(resources) = resources {
            RegionTracker::from_bytes_source_recording(
                tracker_value.value(),
                resources,
                Some(&mut facts),
            )?
        } else {
            RegionTracker::from_bytes_recording(tracker_value.value(), Some(&mut facts))
        };
        #[cfg(redb_no_std)]
        let region_tracker =
            RegionTracker::from_bytes_recording(tracker_value.value(), Some(&mut facts));
        drop(tracker_value);
        #[cfg(not(redb_no_std))]
        let mut allocators =
            Allocators::from_parsed_parts(region_tracker, region_allocators, resources.is_some());
        #[cfg(redb_no_std)]
        let allocators = Allocators::from_parsed_parts(region_tracker, region_allocators);
        let parsed_capacity = allocators.reopen_capacity_facts();
        let parse_facts = facts.finish().and_then(|facts| {
            if facts.retained() != parsed_capacity? {
                return Err(AllocatorResourceError::Incomplete);
            }
            Ok(facts)
        });

        let mut state = self.state.lock().unwrap();
        // Keep the BEFORE-resize parse facts: resize may destroy persisted
        // regions, and changing a logical bitmap len need not free its Vec.
        let layout = state.header.layout();
        #[cfg(not(redb_no_std))]
        if let Some(resources) = resources {
            if allocator_resource_stamp(&state.header) != initial_stamp {
                return Err(super::page_resources::refused());
            }
            let prepared = allocators.prepare_source_reopen_resize(layout, resources)?;
            prepared.claim(&mut allocators, layout)?.apply();
            // Every fallible parse/backing preparation finished before replacing
            // the actual state's prior allocator owner. No partial tagging.
            state.allocators = AllocatorState::Loaded(allocators);
        } else {
            state.allocators = AllocatorState::Loaded(allocators);
            state.allocators_mut().resize_to(layout);
        }
        #[cfg(redb_no_std)]
        {
            state.allocators = AllocatorState::Loaded(allocators);
            state.allocators_mut().resize_to(layout);
        }
        state.header.recovery_required = false;
        let stamp = allocator_resource_stamp(&state.header);
        let recorded = parse_facts
            .and_then(|facts| facts.with_resize_after(state.allocators().reopen_capacity_facts()?))
            .and_then(|facts| {
                if stamp != initial_stamp {
                    return Err(AllocatorResourceError::Stale);
                }
                Ok(RecordedAllocatorReopenComponent {
                    stamp,
                    validated_layout_len: layout.len(),
                    facts,
                })
            });
        *self.allocator_reopen_component_facts.lock().unwrap() = Some(recorded);

        Ok(())
    }

    #[cfg_attr(not(debug_assertions), expect(unused_variables))]
    pub(crate) fn is_allocated(&self, page: PageNumber) -> bool {
        #[cfg(debug_assertions)]
        {
            let allocated = self.allocated_pages.lock().unwrap();
            allocated.contains(&page)
        }
        #[cfg(not(debug_assertions))]
        {
            unreachable!()
        }
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(crate) fn commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        two_phase: bool,
        shrink_policy: ShrinkPolicy,
        header_lock: Option<&HeaderGuard<'_>>,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        self.debug_assert_no_dirty_pages();
        self.storage.check_io_errors()?;

        let mut state = self.state.lock().unwrap();
        // Trim surplus file space, before finalizing the commit
        let shrunk = if !matches!(shrink_policy, ShrinkPolicy::Never) {
            Self::try_shrink(&mut state, matches!(shrink_policy, ShrinkPolicy::Maximum))?
        } else {
            false
        };
        // Copy the header so that we can release the state lock, while we flush the file
        let mut header = state.header.clone();
        drop(state);

        // Crash recovery orders the slots by transaction id, so every commit must be newer than
        // the primary it supersedes; the secondary is overwritten below
        assert!(
            transaction_id > header.primary_slot().transaction_id,
            "commit transaction id not newer than the primary slot's"
        );

        let old_transaction_id = header.secondary_slot().transaction_id;
        header.write_secondary_slot(transaction_id, data_root, system_root);

        // A hold of its own covers one write and not the flush between them, where the caller
        // lends none
        {
            let hold = self.header_hold(header_lock)?;
            self.write_header(&header, &hold)?;
        }

        // Use 2-phase commit, if checksums are disabled
        if two_phase {
            self.storage.flush()?;
        }

        // Make our new commit the primary, and record whether it was a 2-phase commit.
        // These two bits need to be written atomically
        header.swap_primary_slot();
        header.two_phase_commit = two_phase;

        // Write the new header to disk
        {
            let hold = self.header_hold(header_lock)?;
            self.write_header(&header, &hold)?;
            if self.concurrency_mode == ConcurrencyMode::MultiWriter {
                // Writes invalidate the pages they replace, so retained pages remain valid.
                self.storage.record_local_transaction_id(transaction_id);
            }
        }
        self.storage.flush()?;

        if shrunk {
            self.storage.resize(header.layout().len())?;
        }
        // Everything this stood in for is now durable: durable_commit() flushed the allocation
        // records to DATA_ALLOCATED_TABLE before reaching here.
        self.unpersisted.lock().unwrap().clear();

        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.header.secondary_slot().transaction_id,
            old_transaction_id
        );
        state.header = header;
        state.read_from_secondary = false;
        drop(state);

        Ok(())
    }

    // Make changes visible, without a durability guarantee. `newly_unpersisted` is the set of
    // pages allocated by this transaction; they become part of the unpersisted-page tracking so
    // they can be reclaimed if a subsequent durable commit fails.
    pub(crate) fn non_durable_commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        newly_unpersisted: NativeOwnedPageBatch,
        post_commit: bool,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        self.debug_assert_no_dirty_pages();
        self.storage.check_io_errors()?;

        self.unpersisted
            .lock()
            .unwrap()
            .extend(newly_unpersisted, post_commit)?;
        self.storage.write_barrier();

        let mut state = self.state.lock().unwrap();
        state
            .header
            .write_secondary_slot(transaction_id, data_root, system_root);
        state.read_from_secondary = true;

        Ok(())
    }

    pub(crate) fn observe_table_definition(&self, bytes: usize) -> Result {
        if let Some(profile) = self.read_resource_profile {
            profile.before_definition(bytes)?;
        }
        self.read_observations.definition(bytes);
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn planned_read_capture_inline_bytes() -> usize {
        core::mem::size_of::<HeaderGuard<'static>>()
            + core::mem::size_of::<crate::sync::MutexGuard<'static, InMemoryState>>()
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn read_context_limits(
        &self,
    ) -> core::result::Result<super::read_only_resource::NativeReadLimits, AllocatorResourceError>
    {
        self.read_observations
            .capture_read_limits(self.get_page_size())
    }
    #[cfg(all(test, debug_assertions, not(redb_no_std)))]
    pub(crate) fn debug_read_capacity_for_test(&self) -> (usize, usize) {
        let counts = self.read_page_ref_counts.lock().unwrap();
        (counts.len(), counts.capacity())
    }
    pub(crate) fn readonly_resource_profile(
        &self,
    ) -> core::result::Result<
        super::read_only_resource::ReadOnlyResourceProfile,
        AllocatorResourceError,
    > {
        let facts = self.allocator_reopen_component()?.facts();
        self.read_observations.capture(facts, self.get_page_size())
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn readonly_alias_drain(&self) -> ReadOnlyAliasDrain {
        ReadOnlyAliasDrain {
            #[cfg(debug_assertions)]
            read_pages: self.read_page_ref_counts.clone(),
        }
    }
    pub(crate) fn readonly_policy_is_empty(
        &self,
    ) -> core::result::Result<(), AllocatorResourceError> {
        if self.read_resource_profile.is_none() {
            return Err(AllocatorResourceError::Unsupported);
        }
        self.storage.readonly_policy_is_empty()
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepaid_read_memory_capacity_bytes(
        profile: Option<super::read_only_resource::ReadOnlyResourceProfile>,
    ) -> core::result::Result<u64, AllocatorResourceError> {
        use super::read_only_resource::{ReadOnlyResourceProfile as P, map_bound};
        let mut total = P::arc_allocation::<Self>()?;
        #[cfg(debug_assertions)]
        {
            let slots = profile
                .map(|p| p.max_live_pages)
                .unwrap_or(crate::tree_store::btree_base::MAX_BTREE_DEPTH + 3);
            let requested = slots
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(AllocatorResourceError::Overflow)?;
            let map =
                super::container_bounds::hash_fresh_reserve_storage::<(PageNumber, u64)>(requested)
                    .map_err(map_bound)?
                    .allocator_bytes;
            for bytes in [
                P::arc_allocation::<Mutex<NativeDebugPageSet>>()?,
                P::arc_allocation::<Mutex<PageNumberHashMap<u64>>>()?,
                P::arc_allocation::<Mutex<NativeDebugPageSet>>()?,
                map,
            ] {
                total = total
                    .checked_add(bytes)
                    .ok_or(AllocatorResourceError::Overflow)?;
            }
        }
        Ok(total)
    }
    pub(crate) fn readonly_fixed_capacity_bytes(
        profile: super::read_only_resource::ReadOnlyResourceProfile,
    ) -> core::result::Result<u64, AllocatorResourceError> {
        use super::read_only_resource::{ReadOnlyResourceProfile as P, map_bound};
        let mut total = P::arc_allocation::<Self>()?
            .checked_add(PagedCachedFile::readonly_fixed_capacity_bytes()?)
            .ok_or(AllocatorResourceError::Overflow)?;
        #[cfg(debug_assertions)]
        {
            let reserve = profile
                .max_live_pages
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(AllocatorResourceError::Overflow)?;
            let map = super::container_bounds::hash_storage_from_historical_fresh_capacity::<(
                PageNumber,
                u64,
            )>(reserve)
            .map_err(map_bound)?
            .allocator_bytes;
            for n in [
                P::arc_allocation::<Mutex<NativeDebugPageSet>>()?,
                P::arc_allocation::<Mutex<PageNumberHashMap<u64>>>()?,
                P::arc_allocation::<Mutex<NativeDebugPageSet>>()?,
                map,
            ] {
                total = total
                    .checked_add(n)
                    .ok_or(AllocatorResourceError::Overflow)?;
            }
        }
        Ok(total)
    }
    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        Self::check_page_order(page_number)?;
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (range.end - range.start).try_into().unwrap();
        if let Some(profile) = self.read_resource_profile {
            profile.before_page(len)?;
        }
        let mem = self.storage.read(range.start, len, hint)?;
        if let Some(profile) = self.read_resource_profile {
            profile.after_page(&mem)?;
        }
        self.read_observations.page(&mem);

        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(!dirty_pages.contains(&page_number), "{page_number:?}");
            let mut counts = self.read_page_ref_counts.lock().unwrap();
            let live_slots = self
                .read_resource_profile
                .map(|profile| profile.max_live_pages)
                .or_else(|| {
                    #[cfg(not(redb_no_std))]
                    {
                        self.storage.bootstrap_live_page_slots()
                    }
                    #[cfg(redb_no_std)]
                    {
                        None
                    }
                });
            if let Some(max_live_pages) = live_slots {
                // Pinned hashbrown reserve_rehash_inner rehashes IN PLACE
                // when new_items <= full_capacity/2. The fresh reserve is
                // 2*max_live+1; this BEFORE-entry limit proves that condition
                // even after arbitrarily many tombstone-producing erases.
                if !counts.contains_key(&page_number) && counts.len() >= max_live_pages {
                    return Err(super::page_resources::refused());
                }
                debug_assert!(self.readonly_debug_hash_capacity_upper >= 2 * max_live_pages + 1);
            }
            *counts.entry(page_number).or_default() += 1;
            drop(dirty_pages);
        }

        Ok(PageImpl {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            read_tracking: super::base::PageReadTracking::Primary(
                self.read_page_ref_counts.clone(),
            ),
        })
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn point_payload_first_cache_control_capacity_for_test(&self) -> Result<usize> {
        self.storage
            .point_payload_first_cache_control_capacity_for_test()
    }

    #[cfg(not(redb_no_std))]
    fn get_page_for_context(
        &self,
        page_number: PageNumber,
        hint: PageHint,
        context: &NativeReadContext,
    ) -> Result<PageImpl> {
        Self::check_page_order(page_number)?;
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (range.end - range.start).try_into().unwrap();
        #[cfg(debug_assertions)]
        context.copied_open_pages.require_available()?;
        context.limits.before_page(len).inspect_err(|_| {
            #[cfg(all(test, not(redb_no_std)))]
            context.resources.record_read_refusal_for_test(
                1,
                len,
                context.limits.branch,
                context.limits.leaf,
            );
        })?;
        let mem = self
            .storage
            .read_for_context(range.start, len, hint, &context.resources)?;
        context.limits.after_page(&mem).inspect_err(|_| {
            #[cfg(all(test, not(redb_no_std)))]
            context.resources.record_read_refusal_for_test(
                2,
                len,
                context.limits.branch,
                context.limits.leaf,
            );
        })?;
        self.read_observations.page(&mem);

        // This path ALWAYS receives a fresh private copy from read_for_context,
        // never an alias of primary cached/mutable payload. Keep the unchanged
        // dirty-page assertion; USER pin/SYSTEM inspection remain mandatory.
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(!dirty_pages.contains(&page_number), "{page_number:?}");
            context.copied_open_pages.register(page_number)?;
            drop(dirty_pages);
        }

        Ok(PageImpl {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            read_tracking: super::base::PageReadTracking::Copied(context.copied_open_pages.clone()),
        })
    }

    // NOTE: the caller must ensure that the read cache has been invalidated or stale reads my occur
    pub(crate) fn get_page_mut<'txn>(&self, page_number: PageNumber) -> Result<PageMut<'txn>> {
        self.get_page_mut_with_resources(page_number, None)
    }

    fn resource_page_length(&self, page_number: PageNumber) -> Result<usize> {
        Self::check_page_order(page_number)?;
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        usize::try_from(range.end - range.start).map_err(|_| super::page_resources::refused())
    }

    fn get_page_mut_with_resources<'txn>(
        &self,
        page_number: PageNumber,
        resources: Option<&super::page_resources::NativePageResources>,
    ) -> Result<PageMut<'txn>> {
        #[cfg(not(redb_no_std))]
        self.before_debug_page_mutation(resources, false, false)?;
        Self::check_page_order(page_number)?;
        #[cfg(debug_assertions)]
        {
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page_number)
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
        }

        let address_range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();
        let admitted = resources
            .map(|resources| resources.admit_page(len))
            .transpose()?;
        let metadata = self.storage.prepare_write_metadata(resources)?;
        let mem = self.storage.write_with_prepared_metadata(
            address_range.start,
            len,
            false,
            admitted,
            metadata,
        )?;

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));
        }

        Ok(PageMut {
            mem,
            page_number,
            _lifetime: ScopedBorrow::default(),
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    pub(crate) fn get_version(&self) -> u8 {
        let state = self.state.lock().unwrap();
        state.latest_slot().version
    }

    pub(crate) fn get_data_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        state.latest_slot().user_root
    }

    pub(crate) fn get_system_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        state.latest_slot().system_root
    }

    pub(crate) fn get_last_committed_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock()?;
        Ok(state.latest_slot().transaction_id)
    }

    pub(crate) fn get_last_durable_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock()?;
        Ok(state.header.primary_slot().transaction_id)
    }

    // True when a non-durable commit has been made visible to readers but not yet flushed to the
    // durable primary slot.
    pub(crate) fn pending_non_durable_commit(&self) -> bool {
        self.state.lock().unwrap().read_from_secondary
    }

    // True if the backing file is exactly the size the in-memory layout expects. redb only ever
    // sizes the file to a layout length, so an external truncation or extension makes them differ;
    // a pending non-durable commit must not be promoted then, since committing the layout would
    // leave it inconsistent with the file.
    pub(crate) fn file_len_matches_layout(&self) -> Result<bool> {
        let file_len = self.storage.raw_file_len()?;
        let state = self.state.lock().unwrap();
        Ok(file_len == state.header.layout().len())
    }

    // True if the on-disk durable primary slot's checksum is corrupt. Read from disk, since the
    // in-memory copy of an originally-clean slot wouldn't show external/failed-commit corruption.
    pub(crate) fn durable_primary_slot_corrupt(&self) -> Result<bool, DatabaseError> {
        let disk_header = {
            let header_lock = self.lock_header_shared()?;
            Self::read_file_header(&self.storage, self.page_size, &header_lock)?
        };
        Ok(disk_header.primary_corrupted())
    }

    // The durable (primary slot) roots, regardless of any pending non-durable commit served from
    // the secondary slot.
    pub(crate) fn get_durable_data_root(&self) -> Option<BtreeHeader> {
        self.state.lock().unwrap().header.primary_slot().user_root
    }

    pub(crate) fn get_durable_system_root(&self) -> Option<BtreeHeader> {
        self.state.lock().unwrap().header.primary_slot().system_root
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &PageTracker) {
        self.free_helper(page, allocated);
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn native_page_has_read_alias_for_test(&self, page: PageNumber) -> bool {
        self.read_page_ref_counts
            .lock()
            .unwrap()
            .contains_key(&page)
    }

    fn free_helper(&self, page: PageNumber, allocated: &PageTracker) {
        #[cfg(debug_assertions)]
        {
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page)
            );
            assert!(self.allocated_pages.lock().unwrap().remove(&page));
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page));
        }
        allocated.remove(page);
        let mut state = self.state.lock().unwrap();
        Self::release_region_allocation(&mut state, page);

        let address_range = page.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();
        self.storage.invalidate_cache(address_range.start, len);
        self.storage.cancel_pending_write(address_range.start, len);
    }

    // Sole regional free/merged-tracker update, including an allocation that
    // failed before any PageMut or transaction allocation membership escaped.
    fn release_region_allocation(state: &mut InMemoryState, page: PageNumber) {
        let region_index = page.region;
        let freed_order = state
            .get_region_mut(region_index)
            .free(page.page_index, page.page_order);
        state
            .get_region_tracker_mut()
            .mark_free(freed_order, region_index);
    }

    // Drops the page from the unpersisted set without freeing it. Returns whether it was there.
    pub(crate) fn claim_unpersisted(&self, page: PageNumber) -> bool {
        self.unpersisted.lock().unwrap().claim(page)
    }

    // Frees the page if no durable commit has occurred, since it was allocated. Returns true, if the page was freed
    pub(crate) fn free_if_unpersisted(&self, page: PageNumber, allocated: &PageTracker) -> bool {
        if self.unpersisted.lock().unwrap().claim(page) {
            self.free_helper(page, allocated);
            true
        } else {
            false
        }
    }

    // Record pages allocated in the data tree by a non-durable transaction. These are tracked in
    // memory instead of being written to DATA_ALLOCATED_TABLE so that `free_if_unpersisted` can
    // efficiently update the allocation list when it reclaims pages.
    pub(crate) fn record_unpersisted_allocations(
        &self,
        transaction_id: TransactionId,
        pages: impl IntoIterator<Item = PageNumber>,
    ) {
        self.unpersisted
            .lock()
            .unwrap()
            .record_allocations(transaction_id, pages);
    }

    pub(crate) fn take_unpersisted_allocations(&self) -> NativeOwnedTxnPageBatches {
        self.unpersisted.lock().unwrap().take_allocations()
    }

    #[cfg(not(redb_no_std))]
    fn require_allocated_page_number_layout(
        &self,
        pages: &crate::transactions::PageList<'_>,
    ) -> core::result::Result<(), crate::tree_store::NativeKeyPathError> {
        use crate::tree_store::NativeKeyPathError as E;
        let bytes = <crate::transactions::PageList<'_> as crate::Value>::as_bytes(pages);
        // Length before len/get: the sole PageList decoder then walks at most
        // 400 actual entries. Reserved PN bits retain native ignored semantics.
        if bytes.len() != crate::transactions::PageList::allocated_capacity_bytes() {
            return Err(E::Malformed);
        }
        let count = pages.len();
        if count > crate::transactions::PageList::allocated_capacity_count() {
            return Err(E::Malformed);
        }
        let state = self.state.lock().map_err(StorageError::from)?;
        let layout = state.header.layout();
        for index in 0..count {
            let page = pages.get(index); // existing PageNumber::from_le_bytes
            if !Self::supported_page_order(page)
                || Self::page_number_region_failure(page, &layout).is_some()
            {
                return Err(E::Malformed);
            }
        }
        Ok(())
    }

    // Actual post-only eligibility, checked under the existing owner mutex.
    // This does not inspect historical DATA_ALLOCATED rows or mint a plan.
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_allocated_prefix_post_only(&self) -> Result<()> {
        let state = self.unpersisted.lock()?;
        if !state.allocations.is_empty()
            || !state.allocation_txn.is_empty()
            || !state.data_freed.is_empty()
            // Existing stable post-only membership has post as a subset of
            // pages. Equal cardinality proves equality without scanning either
            // potentially large set; inspection/Live writer excludes transfer.
            || state.pages.len() != state.post_commit_allocations.len()
            || state.source_pages.as_ref().is_some_and(|pages| {
                !pages.is_empty() && !state.source_pages_are_post_commit
            })
        {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }

    // Test-only scalar census over a real bounded fixture. Iterating this set
    // is not a preadmitted bounded production census or a resource plan.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn reclamation_quote_snapshot_for_test(&self) -> ReclamationUnpersistedSnapshot {
        let state = self.unpersisted.lock().unwrap();
        let source = state.source_pages.as_ref();
        let source_len = source.map_or(0, |pages| pages.len());
        let source_capacity = source.map_or(0, |pages| pages.capacity());
        let post_only = state.allocations.is_empty()
            && state.allocation_txn.is_empty()
            && state.data_freed.is_empty()
            && state.pages == state.post_commit_allocations
            && (source_len == 0 || state.source_pages_are_post_commit);
        let weighted = state
            .pages
            .iter()
            .chain(source.into_iter().flat_map(|pages| pages.iter()))
            .try_fold(0_u64, |sum, page| sum.checked_add(page.page_size_bytes(1)));
        ReclamationUnpersistedSnapshot {
            pages: state.pages.len() + source_len,
            pages_capacity: state.pages.capacity() + source_capacity,
            post_pages: state.post_commit_allocations.len()
                + if state.source_pages_are_post_commit {
                    source_len
                } else {
                    0
                },
            post_capacity: state.post_commit_allocations.capacity()
                + if state.source_pages_are_post_commit {
                    source_capacity
                } else {
                    0
                },
            allocation_index_capacity: state.allocation_txn.capacity(),
            post_only,
            weighted_pages: weighted,
        }
    }

    // Preserve the ordinary epilogue's existing pre-I/O-check recording
    // boundary. Source post-only membership is moved atomically with its owner
    // by non_durable_commit; duplicating it here would require a new hash array.
    pub(crate) fn record_post_commit_allocations(&self, pages: &NativeOwnedPageBatch) {
        #[cfg(not(redb_no_std))]
        if pages.is_source() {
            return;
        }
        self.unpersisted
            .lock()
            .unwrap()
            .post_commit_allocations
            .extend(pages.iter().copied());
    }

    // Returns all unpersisted data-tree pages allocated strictly after `transaction_id`. Used
    // during savepoint restore to queue pages that need to be freed.
    pub(crate) fn unpersisted_allocations_after(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<PageNumber> {
        self.unpersisted
            .lock()
            .unwrap()
            .allocations_after(transaction_id)
    }

    pub(crate) fn unpersisted(&self, page: PageNumber) -> bool {
        self.unpersisted.lock().unwrap().contains(page)
    }

    // Record the data-tree pages a non-durable commit freed, for the next durable commit to write
    // to DATA_FREED_TABLE.
    pub(crate) fn record_unpersisted_data_freed(
        &self,
        transaction_id: TransactionId,
        pages: NativeOwnedPageVec,
    ) -> Result<()> {
        let pages = pages.into_ordinary()?;
        self.unpersisted
            .lock()
            .unwrap()
            .record_data_freed(transaction_id, pages);
        Ok(())
    }

    // Offers every page freed by a transaction in `start..end` to `free_page`, dropping the ones
    // it reports having reclaimed. Returns the transactions considered.
    pub(crate) fn process_unpersisted_data_freed(
        &self,
        start: TransactionId,
        end: TransactionId,
        mut free_page: impl FnMut(PageNumber) -> bool,
    ) -> Vec<TransactionId> {
        // The records are copied out before `free_page` runs, because reclaiming a page locks this
        // same state. Only the committing write transaction mutates these records, so nothing can
        // add to a transaction's record in between.
        let snapshot = self
            .unpersisted
            .lock()
            .unwrap()
            .data_freed_in_range(start, end);
        let mut transaction_ids = Vec::with_capacity(snapshot.len());
        for (transaction_id, pages) in snapshot {
            let kept: Vec<PageNumber> = pages.into_iter().filter(|p| !free_page(*p)).collect();
            self.unpersisted
                .lock()
                .unwrap()
                .replace_data_freed(transaction_id, kept);
            transaction_ids.push(transaction_id);
        }
        transaction_ids
    }

    pub(crate) fn take_unpersisted_data_freed(&self) -> BTreeMap<TransactionId, Vec<PageNumber>> {
        self.unpersisted.lock().unwrap().take_data_freed()
    }

    pub(crate) fn drop_unpersisted_data_freed_after(&self, transaction_id: TransactionId) {
        self.unpersisted
            .lock()
            .unwrap()
            .drop_data_freed_after(transaction_id);
    }

    pub(crate) fn unpersisted_data_freed_pages(&self) -> Vec<PageNumber> {
        self.unpersisted.lock().unwrap().pages_pending_free()
    }

    pub(crate) fn allocate_helper<'txn>(
        &self,
        allocation_size: usize,
        lowest: bool,
    ) -> Result<PageMut<'txn>> {
        self.allocate_helper_with_resources(allocation_size, lowest, None)
    }

    fn allocate_helper_with_resources<'txn>(
        &self,
        allocation_size: usize,
        lowest: bool,
        resources: Option<&super::page_resources::NativePageResources>,
    ) -> Result<PageMut<'txn>> {
        #[cfg(not(redb_no_std))]
        self.before_debug_page_mutation(resources, true, false)?;
        let admitted = if let Some(resources) = resources {
            let physical = allocation_size
                .div_ceil(self.get_page_size())
                .checked_next_power_of_two()
                .and_then(|pages| pages.checked_mul(self.get_page_size()))
                .ok_or_else(super::page_resources::refused)?;
            Some(resources.admit_page(physical)?)
        } else {
            None
        };
        // Complete cache-container admission/construction precedes both real
        // allocator mutation and debug bookkeeping. No new quota failure may
        // be introduced after a reserved page number but before batch tracking.
        let metadata = self.storage.prepare_write_metadata(resources)?;
        let required_pages = allocation_size.div_ceil(self.get_page_size());
        let required_order = ceil_log2(required_pages);

        let mut state = self.state.lock().unwrap();

        let page_number = if let Some(page_number) =
            Self::allocate_helper_retry(&mut state, required_order, lowest)?
        {
            page_number
        } else {
            self.grow(&mut state, required_order)?;
            Self::allocate_helper_retry(&mut state, required_order, lowest)?.unwrap()
        };

        #[cfg(debug_assertions)]
        {
            assert!(self.allocated_pages.lock().unwrap().insert(page_number));
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page_number),
                "Allocated a page that is still referenced! {page_number:?}"
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
        }

        let address_range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();

        #[allow(unused_mut)]
        let mut mem = match self.storage.write_with_prepared_metadata(
            address_range.start,
            len,
            true,
            admitted,
            metadata,
        ) {
            Ok(mem) => mem,
            Err(error) => {
                // Every fallible cached-write step precedes new-key insert /
                // take_value. No WritablePage or new PN slot has escaped, and
                // allocated_since_commit has not recorded this PN yet. Undo
                // exactly that reservation while the original state lock is
                // still held. Preserve original I/O poison/fail-closed state.
                #[cfg(debug_assertions)]
                {
                    assert!(self.allocated_pages.lock().unwrap().remove(&page_number));
                    assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
                }
                Self::release_region_allocation(&mut state, page_number);
                return Err(error);
            }
        };
        debug_assert!(mem.mem().len() >= allocation_size);

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));

            // Poison the memory in debug mode to help detect uninitialized reads
            mem.mem_mut().fill(0xFF);
        }

        Ok(PageMut {
            mem,
            page_number,
            _lifetime: ScopedBorrow::default(),
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    fn allocate_helper_retry(
        state: &mut InMemoryState,
        required_order: u8,
        lowest: bool,
    ) -> Result<Option<PageNumber>> {
        loop {
            let Some(candidate_region) = state.get_region_tracker_mut().find_free(required_order)
            else {
                return Ok(None);
            };
            let region = state.get_region_mut(candidate_region);
            let r = if lowest {
                region.alloc_lowest(required_order)
            } else {
                region.alloc(required_order)
            };
            if let Some(page) = r {
                return Ok(Some(PageNumber::new(
                    candidate_region,
                    page,
                    required_order,
                )));
            }
            // Mark the region, if it's full
            state
                .get_region_tracker_mut()
                .mark_full(required_order, candidate_region);
        }
    }

    pub(crate) fn get_layout(&self) -> DatabaseLayout {
        self.state.lock().unwrap().header.layout()
    }

    // Free all pages allocated by the aborted transaction before rolling back growth. Freeing pages
    // also cancels their buffered writes so a later flush cannot re-extend the file.
    pub(crate) fn rollback_growth(&self, initial_layout: DatabaseLayout) -> Result {
        let mut state = self.state.lock().unwrap();
        if state.header.layout().len() > initial_layout.len() {
            // Growth is not published in the header until commit. Restoring the starting layout
            // preserves all earlier commits, including non-durable ones, without a header write.
            self.storage.resize(initial_layout.len())?;
            state.allocators_mut().resize_to(initial_layout);
            state.header.set_layout(initial_layout);
        }
        Ok(())
    }

    // Trims the free pages at the end of the file. With `force`, all of them: every free trailing
    // region and the free tail of the last region left. Otherwise one step, of the last region
    fn try_shrink(state: &mut InMemoryState, force: bool) -> Result<bool> {
        let mut shrunk = false;
        loop {
            let layout = state.header.layout();
            let last_region_index = layout.num_regions() - 1;
            let last_allocator = state.get_region(last_region_index);
            let trailing_free = last_allocator.trailing_free_pages();
            let last_allocator_len = last_allocator.len();
            if trailing_free == 0 {
                break;
            }
            if trailing_free < last_allocator_len / 2 && !force {
                break;
            }
            let reduce_by = if layout.num_regions() > 1 && trailing_free == last_allocator_len {
                trailing_free
            } else if force {
                // Do not shrink the database to zero size
                min(last_allocator_len - 1, trailing_free)
            } else {
                trailing_free / 2
            };
            if reduce_by == 0 {
                break;
            }

            let mut new_layout = layout;
            new_layout.reduce_last_region(reduce_by);
            state.allocators_mut().resize_to(new_layout);
            assert!(new_layout.len() < layout.len());
            state.header.set_layout(new_layout);
            shrunk = true;
            if !force {
                break;
            }
        }

        Ok(shrunk)
    }

    fn grow(&self, state: &mut InMemoryState, required_order_allocation: u8) -> Result<()> {
        let new_layout = state
            .header
            .layout()
            .growth_for(required_order_allocation)
            .map_err(|_| crate::StorageError::Unsupported)?;
        self.storage.resize(new_layout.len())?;
        // Make the larger file durable before its layout can reach the on-disk header. A
        // subsequent commit writes this layout into the header, whose layout fields are shared by
        // both commit slots; if a crash persisted that header but not the file extension, every
        // open would fail with "File truncated below stored layout" even though the previous
        // durable state was intact. This mirrors the shrink path, which reduces the file only
        // after the smaller layout is durable.
        self.storage.sync_file()?;

        state.allocators_mut().resize_to(new_layout);
        state.header.set_layout(new_layout);
        Ok(())
    }

    fn allocate<'txn>(
        &self,
        allocation_size: usize,
        allocated: &PageTracker,
    ) -> Result<PageMut<'txn>> {
        let result = self.allocate_helper(allocation_size, false);
        if let Ok(ref page) = result {
            allocated.insert(page.get_page_number());
        }
        result
    }

    fn allocate_lowest<'txn>(
        &self,
        allocation_size: usize,
        allocated: &PageTracker,
    ) -> Result<PageMut<'txn>> {
        let result = self.allocate_helper(allocation_size, true);
        if let Ok(ref page) = result {
            allocated.insert(page.get_page_number());
        }
        result
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_allocated_pages());
        }

        Ok(count)
    }

    pub(crate) fn count_free_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_free_pages());
        }

        Ok(count)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.page_size.try_into().unwrap()
    }

    /// Asserts that the file is consistent and this handle a live writer: see `CONSISTENT_BYTE`.
    /// Shared, since a multi-writer cohort has several, and released with every other range by
    /// `close()`.
    pub(crate) fn mark_consistent(&self) -> Result {
        if self.concurrency_mode.is_multi_process_writable() {
            self.storage
                .lock_shared_range(byte_range(CONSISTENT_BYTE))?;
        }
        Ok(())
    }

    // Writes the shutdown header, under `writer_lock` where the database is shared, and closes
    // the storage. Without the lock the header is not written: it would overwrite a commit
    // another process makes meanwhile
    pub(crate) fn close(&self, writer_lock: Option<&Arc<WriterLock>>) -> Result {
        let shutdown_result = match writer_lock {
            Some(_held) => self.flush_shutdown_header(),
            None => Ok(()),
        };
        // The backend's close() contract guarantees it is called exactly once, so it must be
        // called even if the shutdown writes above failed
        let close_result = self.storage.close();
        shutdown_result.and(close_result)
    }

    fn flush_shutdown_header(&self) -> Result {
        if self.storage.check_io_errors().is_ok() && !crate::panicking() {
            let header_lock = self.lock_header_exclusive()?;
            let mut state = self.state.lock()?;
            // Clearing the flag asserts that this process left the file consistent, which requires
            // an allocator state describing what it wrote, and one not marked for repair.
            if state.allocators.is_loaded() && !self.needs_repair() && self.storage.flush().is_ok()
            {
                state.header.recovery_required = false;
                self.write_header(&state.header, &header_lock)?;
                self.storage.flush()?;
            }
        }

        Ok(())
    }
}

#[cfg(all(test, not(redb_no_std)))]
mod unreturned_allocation_tests {
    use super::TransactionalMemory;
    use crate::tree_store::{InMemoryBackend, Page, PageTracker};
    use crate::{StorageBackend, StorageError};
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct FailingWriteBackend {
        inner: InMemoryBackend,
        fail: Arc<AtomicBool>,
        failed: Arc<AtomicUsize>,
    }
    impl StorageBackend for FailingWriteBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> Result<(), std::io::Error> {
            self.inner.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            if self.fail.load(Ordering::Acquire) {
                self.failed.fetch_add(1, Ordering::AcqRel);
                return Err(std::io::Error::other(
                    "injected native allocation writeback failure",
                ));
            }
            self.inner.write(offset, data)
        }
    }
    fn memory() -> (
        TransactionalMemory,
        Option<Arc<super::WriterLock>>,
        Arc<AtomicBool>,
        Arc<AtomicUsize>,
    ) {
        let fail = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicUsize::new(0));
        let backend = FailingWriteBackend {
            inner: InMemoryBackend::new(),
            fail: fail.clone(),
            failed: failed.clone(),
        };
        let (mem, lock) = TransactionalMemory::new(
            Box::new(backend),
            true,
            512,
            Some(64 * 512),
            1024,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();
        (mem, lock, fail, failed)
    }
    fn pool() -> super::super::NativePageResources {
        use super::super::{
            SharedPageBytes, lru_cache::LRUCache, page_resources::NativePageResources,
        };
        // Two concrete 512-byte attempts, one resident entry per stripe and
        // old/new metadata overlap. This is a unit fixture envelope only.
        let page = NativePageResources::page_allocation_bound(512).unwrap();
        let metadata = usize::try_from(
            LRUCache::<Option<SharedPageBytes>>::prepared_storage_bytes(2, 2).unwrap(),
        )
        .unwrap();
        NativePageResources::from_retained_pool_data(2 * page + 2 * metadata, ()).unwrap()
    }
    // req: STO-020, REC-001
    #[test]
    fn ordinary_failed_writeback_releases_only_never_returned_native_page() {
        let (mem, _lock, fail, failed) = memory();
        let seed = mem.allocate_helper(512, false).unwrap();
        let seed_page = seed.get_page_number();
        drop(seed);
        let before = mem.count_allocated_pages().unwrap();
        assert_eq!(before, 1);
        fail.store(true, Ordering::Release);
        assert!(matches!(
            mem.allocate_helper(512, false),
            Err(StorageError::Io(_))
        ));
        assert_eq!(failed.load(Ordering::Acquire), 1);
        assert!(
            mem.storage_failure(),
            "original failure must remain latched"
        );
        assert_eq!(
            mem.count_allocated_pages().unwrap(),
            before,
            "never_returned_page_reservation_must_retire"
        );
        #[cfg(debug_assertions)]
        assert_eq!(mem.allocated_pages.lock().unwrap().len(), 1);
        mem.free(seed_page, &PageTracker::ignore());
        assert_eq!(mem.count_allocated_pages().unwrap(), 0);
    }
    // req: STO-020, PRJ-004, REC-001
    #[test]
    fn source_failed_writeback_keeps_real_old_owners_and_releases_unreturned_page() {
        let (mem, _lock, fail, failed) = memory();
        let resources = pool();
        let seed = mem
            .allocate_helper_with_resources(512, false, Some(&resources))
            .unwrap();
        let seed_page = seed.get_page_number();
        drop(seed);
        let before = mem.count_allocated_pages().unwrap();
        let physical = resources.current();
        assert!(physical > 0);
        fail.store(true, Ordering::Release);
        assert!(matches!(
            mem.allocate_helper_with_resources(512, false, Some(&resources)),
            Err(StorageError::Io(_))
        ));
        assert_eq!(failed.load(Ordering::Acquire), 1);
        assert!(mem.storage_failure());
        assert_eq!(
            mem.count_allocated_pages().unwrap(),
            before,
            "never_returned_page_reservation_must_retire"
        );
        assert_eq!(
            resources.current(),
            physical,
            "actual old payload/container owners survive failed I/O"
        );
        mem.free(seed_page, &PageTracker::ignore());
        assert_eq!(resources.current(), 0);
        assert_eq!(mem.count_allocated_pages().unwrap(), 0);
    }
    // req: STO-020, PRJ-004, REC-001
    #[test]
    fn source_affinity_error_preserves_actual_page_and_retains_installed_metadata_until_free() {
        let (mem, _lock, _fail, _failed) = memory();
        let first = pool();
        let foreign = pool();
        let seed = mem
            .allocate_helper_with_resources(512, false, Some(&first))
            .unwrap();
        let page = seed.get_page_number();
        drop(seed);
        // Exact private representation transition, preserving the genuine
        // allocated page/value. This cannot mint a new page or writer identity.
        mem.storage.handoff_write_metadata_to_ordinary().unwrap();
        let before = mem.count_allocated_pages().unwrap();
        match mem.get_page_mut_with_resources(page, Some(&foreign)) {
            Err(StorageError::Io(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory)
            }
            _ => panic!("actual foreign-pool affinity must refuse before a mutable page escapes"),
        }
        assert!(
            !mem.storage_failure(),
            "numerical affinity refusal must not latch backend failure"
        );
        assert_eq!(mem.count_allocated_pages().unwrap(), before);
        assert!(
            first.current() > 0,
            "original physical payload remains owned"
        );
        assert!(
            foreign.current() > 0,
            "installed foreign metadata cannot refund before physical retirement"
        );
        mem.free(page, &PageTracker::ignore());
        assert_eq!(first.current(), 0);
        assert_eq!(foreign.current(), 0);
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::page_manager::INITIAL_REGIONS;
    use crate::{Database, TableDefinition};

    // Test that the region tracker expansion code works, by adding more data than fits into the initial max regions
    #[test]
    fn out_of_regions() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u32, &[u8]> = TableDefinition::new("x");
        let page_size = 1024;
        let big_value = vec![0u8; 5 * page_size];

        let db = Database::builder()
            .set_region_size((8 * page_size).try_into().unwrap())
            .set_page_size(page_size)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..=INITIAL_REGIONS {
                table.insert(&i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        drop(db);

        let mut db = Database::builder()
            .set_region_size((8 * page_size).try_into().unwrap())
            .set_page_size(page_size)
            .open(tmpfile.path())
            .unwrap();
        assert!(db.check_integrity().unwrap());
    }

    // Make sure the database remains consistent after a panic
    #[test]
    #[cfg(panic = "unwind")]
    fn panic() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u32, &[u8]> = TableDefinition::new("x");

        let _ = std::panic::catch_unwind(|| {
            let db = Database::create(&tmpfile).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table_definition).unwrap();
            panic!();
        });

        let mut db = Database::open(tmpfile).unwrap();
        assert!(db.check_integrity().unwrap());
    }

    // Missing allocator state is distinct from state invalidated by a failed operation.
    #[test]
    fn an_invalidated_allocator_state_is_not_merely_an_unloaded_one() {
        use super::TransactionalMemory;
        use crate::tree_store::InMemoryBackend;

        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            4096,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();

        // Freshly opened: nothing loaded yet, and nothing invalidated
        assert!(!mem.allocator_state_loaded());
        assert!(!mem.allocator_state_invalidated());

        mem.reset_allocator_state().unwrap();
        assert!(mem.allocator_state_loaded());
        assert!(!mem.allocator_state_invalidated());

        mem.invalidate_allocator_state();
        assert!(!mem.allocator_state_loaded());
        assert!(mem.allocator_state_invalidated());

        // Rebuilding the allocator state clears invalidation.
        mem.reset_allocator_state().unwrap();
        assert!(!mem.allocator_state_invalidated());
    }

    // A panic raised while the state mutex is held (e.g. an allocator assertion) poisons it.
    // invalidate_allocator_state() runs while unwinding from such a panic, so it must recover
    // the lock rather than double-panic; the poison itself is left set.
    #[test]
    #[cfg(panic = "unwind")]
    fn invalidate_allocator_state_tolerates_poison() {
        use super::TransactionalMemory;
        use crate::tree_store::InMemoryBackend;

        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            4096,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        std::thread::scope(|s| {
            let result = s
                .spawn(|| {
                    let _guard = mem.state.lock().unwrap();
                    panic!("poison the state mutex");
                })
                .join();
            assert!(result.is_err());
        });
        assert!(mem.state.is_poisoned());

        // Must not panic, despite the poisoned lock
        mem.invalidate_allocator_state();

        // The poison stays set, so later accesses fail rather than trust the state
        assert!(mem.state.is_poisoned());
        assert!(mem.get_last_committed_transaction_id().is_err());
    }

    // Rebuilding the allocator state feeds mark_page_allocated() page numbers straight out of the
    // freed tables, which no page walk ever reads. A corrupted entry has to be reported rather than
    // indexing the region allocators, tripping the bitmap's bounds assertion, or walking the buddy
    // allocator past its maximum order. See https://github.com/cberner/redb/issues/1333
    #[test]
    fn mark_page_allocated_rejects_corrupt_page_numbers() {
        use super::{MAX_PAGE_INDEX, TransactionalMemory};
        use crate::StorageError;
        use crate::tree_store::page_store::base::MAX_REGIONS;
        use crate::tree_store::{InMemoryBackend, PageNumber};

        let page_size = 4096;
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            page_size,
            Some(64 * page_size as u64),
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        let corrupt = [
            // Past the end of the layout, which would index the region allocators out of bounds
            PageNumber::new(MAX_REGIONS - 1, 0, 0),
            // Past the end of its region, which the bitmap asserts on
            PageNumber::new(0, MAX_PAGE_INDEX, 0),
            // An order no region can have
            PageNumber::from_le_bytes((31u64 << 59).to_le_bytes()),
        ];
        for page in corrupt {
            assert!(
                matches!(
                    mem.mark_page_allocated(page),
                    Err(StorageError::Corrupted(_))
                ),
                "{page:?} was not rejected"
            );
        }

        // Naming the same page twice walks the allocator up past its maximum order looking for a
        // parent to split
        mem.mark_page_allocated(PageNumber::new(0, 0, 0)).unwrap();
        assert!(matches!(
            mem.mark_page_allocated(PageNumber::new(0, 0, 0)),
            Err(StorageError::Corrupted(_))
        ));
    }

    // A page order read from a corrupted file can be far larger than any real page, which the read
    // path would use to size its buffer.
    #[test]
    fn oversized_page_order_is_rejected() {
        use super::TransactionalMemory;
        use crate::StorageError;
        use crate::tree_store::page_store::base::PageHint;
        use crate::tree_store::{InMemoryBackend, Page, PageNumber, PageTracker};

        let page_size = 4096;
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            page_size,
            Some(64 * page_size as u64),
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        let valid = mem.allocate_helper(1, false).unwrap();
        let valid_page = valid.get_page_number();
        drop(valid);

        // order = 31, which would be read as a 2^31 page (8TiB) allocation
        let bad_order = PageNumber::from_le_bytes((31u64 << 59).to_le_bytes());

        assert!(matches!(
            mem.get_page(bad_order, PageHint::None),
            Err(StorageError::Corrupted(_))
        ));
        assert!(matches!(
            mem.mark_page_allocated(bad_order),
            Err(StorageError::Corrupted(_))
        ));
        mem.get_page(valid_page, PageHint::None).unwrap();

        mem.free(valid_page, &PageTracker::ignore());
    }

    // Freeing pages that buddy-merge into a higher order must re-mark the region tracker at the
    // merged order. Otherwise the tracker stays marked full at that order, find_free skips the
    // region even though a free block exists, and the file grows (and compact() stalls) instead of
    // reusing the space.
    #[test]
    fn free_merge_remarks_region_tracker() {
        use super::TransactionalMemory;
        use crate::tree_store::{InMemoryBackend, Page, PageTracker};

        // Small pages and regions keep the reproduction cheap to set up.
        let page_size = 128 * 1024;
        let region_size = 16 * page_size as u64;
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            page_size,
            Some(region_size),
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        let ignore = PageTracker::ignore();

        // Fill region 0 with order-0 pages. The allocation that spills past region 0 fails on it
        // first, which marks region 0 full at every order.
        let mut region0_pages = vec![];
        loop {
            let page = mem.allocate_helper(1, false).unwrap();
            let number = page.get_page_number();
            drop(page);
            if number.region == 0 {
                region0_pages.push(number);
            } else {
                // First page past region 0: it has done its job of forcing region 0 full. Give it
                // back so the spilled-into region is left entirely free.
                mem.free(number, &ignore);
                break;
            }
        }
        assert!(
            region0_pages.len() >= 2,
            "test needs at least two pages in region 0, got {}",
            region0_pages.len()
        );

        // Free everything in region 0. The order-0 pages buddy-merge back into larger blocks, so
        // region 0 regains free space above order 0.
        for page in region0_pages {
            mem.free(page, &ignore);
        }

        // An order-1 allocation must reuse region 0's merged free block. Before the fix the tracker
        // still marked region 0 full above order 0, so find_free skipped it and this landed in a
        // higher region.
        let reused = mem.allocate_helper(2 * page_size, false).unwrap();
        assert_eq!(
            reused.get_page_number().region,
            0,
            "order-1 allocation should reuse the merged free block in region 0"
        );
    }
}

/// The header lock, against real files.
#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod header_lock_test {
    use super::{HeaderGuard, TransactionalMemory};
    use crate::db::ConcurrencyMode;
    use crate::tree_store::PAGE_SIZE;
    use crate::tree_store::page_store::file_backend::FileBackend;
    use crate::{DatabaseError, Result};
    use alloc::sync::Arc;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn hold(mem: &TransactionalMemory, exclusive: bool) -> Result<HeaderGuard<'_>> {
        TransactionalMemory::lock_header(
            &mem.storage,
            &mem.in_process_header_lock,
            mem.concurrency_mode,
            exclusive,
        )
    }

    fn open(path: &Path, mode: ConcurrencyMode) -> Result<Arc<TransactionalMemory>, DatabaseError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        // These stand in for separate processes' handles, so each gives the writer byte back as
        // the end of an open does, by dropping the lock its open held
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(FileBackend::new(file)?),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
            mode,
        )?;

        Ok(Arc::new(mem))
    }

    fn open_read_only(
        path: &Path,
        mode: ConcurrencyMode,
    ) -> Result<Arc<TransactionalMemory>, DatabaseError> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(crate::tree_store::ReadOnlyBackend::new(Box::new(
                FileBackend::new(file)?,
            ))),
            false,
            PAGE_SIZE,
            None,
            0,
            true,
            mode,
        )?;

        Ok(Arc::new(mem))
    }

    /// Through the read-only backend the open wraps its storage in
    #[test]
    fn a_read_only_handle_takes_shared_holds() {
        let tmpfile = crate::create_tempfile();
        open(tmpfile.path(), ConcurrencyMode::ExclusiveWriter).unwrap();

        let reader = open_read_only(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        let held = hold(&reader, false).unwrap();

        let peer = open_read_only(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        drop(hold(&peer, false).unwrap());
        drop(held);
    }

    /// A peer's close clears the recovery flag in the file while this handle is still writing.
    /// The commit that syncs to that close sets the flag again, so that a crash of this handle
    /// is still recovered from.
    #[cfg(feature = "experimental-multiprocess")]
    #[test]
    fn syncing_to_a_peers_close_keeps_the_recovery_flag() {
        use super::DB_HEADER_SIZE;
        use crate::tree_store::page_store::header::UnrepairedDatabaseHeader;
        use crate::{Database, TableDefinition};
        const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

        fn unclean(path: &Path) -> bool {
            let bytes = std::fs::read(path).unwrap();
            UnrepairedDatabaseHeader::from_bytes(
                &bytes[..DB_HEADER_SIZE],
                PAGE_SIZE.try_into().unwrap(),
            )
            .unwrap()
            .unclean()
        }

        let tmpfile = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriter);
        builder.create(tmpfile.path()).unwrap();
        let db = builder.open(tmpfile.path()).unwrap();
        let peer = builder.open(tmpfile.path()).unwrap();
        drop(peer);
        assert!(!unclean(tmpfile.path()), "the peer's close left the flag");

        let txn = db.begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
        txn.commit().unwrap();
        assert!(
            unclean(tmpfile.path()),
            "the commit did not write the flag back"
        );
        drop(db);
        assert!(!unclean(tmpfile.path()), "the close left the flag");
    }

    /// The other half of the hold covering the write: the bytes are on the file when
    /// `write_header()` returns, rather than waiting in the buffer for a flush outside the hold.
    #[test]
    fn the_header_reaches_the_file_before_write_header_returns() {
        use super::DB_HEADER_SIZE;

        let tmpfile = crate::create_tempfile();
        let writer = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

        let mut header = writer.state.lock().unwrap().header.clone();
        header.recovery_required = !header.recovery_required;
        let expected = header.to_bytes(true);
        let header_lock = writer.lock_header_exclusive().unwrap();
        writer.write_header(&header, &header_lock).unwrap();
        drop(header_lock);

        // Read around redb entirely, and with no flush in between
        let on_disk = std::fs::read(tmpfile.path()).unwrap();
        assert_eq!(&on_disk[..DB_HEADER_SIZE], &expected[..]);
    }

    /// The hold has to cover the write reaching the file, which is what `write_header()` does
    /// directly rather than leaving to whenever the buffer is next flushed.
    #[test]
    fn the_hold_covers_the_write_reaching_the_file() {
        let tmpfile = crate::create_tempfile();
        let writer = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        let reader = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

        // Taken before the hold: another one in this process would wait on it
        let header = writer.state.lock().unwrap().header.clone();
        let held = hold(&reader, false).unwrap();

        let (tx, rx) = mpsc::channel();
        let writing = thread::spawn(move || {
            let header_lock = writer.lock_header_exclusive().unwrap();
            writer.write_header(&header, &header_lock).unwrap();
            tx.send(()).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the header reached the file while a reader held the lock"
        );
        drop(held);
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the write never completed");
        writing.join().unwrap();
    }

    /// Which is what keeps a header mid-write from being read in another process
    #[test]
    fn an_exclusive_hold_excludes_a_shared_one() {
        let tmpfile = crate::create_tempfile();
        let writer = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        let reader = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

        let guard = hold(&writer, true).unwrap();
        let (tx, rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            let _held = hold(&reader, false).unwrap();
            tx.send(()).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the reader entered its hold while the writer held one"
        );
        drop(guard);
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the reader never entered its hold");
        waiting.join().unwrap();
    }

    /// Only a write needs the header to itself
    #[test]
    fn shared_holds_admit_each_other() {
        let tmpfile = crate::create_tempfile();
        let first = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        let second = open(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

        let held = hold(&first, false).unwrap();
        let (tx, rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            let _also = hold(&second, false).unwrap();
            tx.send(()).unwrap();
        });

        rx.recv_timeout(Duration::from_secs(10))
            .expect("a second reader was held off");
        waiting.join().unwrap();
        drop(held);
    }

    /// It holds the whole storage, which covers the header bytes: a hold that locked and
    /// released them would punch a hole in that lock.
    #[test]
    fn a_hold_does_not_puncture_the_whole_storage_lock() {
        let tmpfile = crate::create_tempfile();
        let held = open(tmpfile.path(), ConcurrencyMode::ExclusiveWriter).unwrap();

        for exclusive in [false, true] {
            let guard = hold(&held, exclusive).unwrap();
            drop(guard);
        }

        assert!(matches!(
            open(tmpfile.path(), ConcurrencyMode::ExclusiveWriter),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
    }
}

/// The locking protocol an open runs before it reads anything, against real files. Only the
/// platforms with byte-range locks can run it.
#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod lock_protocol_test {
    use super::TransactionalMemory;
    use crate::DatabaseError;
    use crate::db::ConcurrencyMode;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use crate::tree_store::page_store::file_backend::FileBackend;
    use crate::tree_store::page_store::file_backend::range_lock::RangeLock;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    /// The locks an open takes. A failure drops the storage, which is what releases whatever it
    /// had taken.
    fn open_file(
        file: File,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<PagedCachedFile, DatabaseError> {
        let storage = super::NativePageStorage::ordinary(alloc::sync::Arc::new(
            PagedCachedFile::new(Box::new(FileBackend::new(file)?), 4096, 0),
        ));
        TransactionalMemory::lock_for_open(&storage, read_only, concurrency_mode)?;
        let super::NativePageStorage::Ordinary(storage) = storage else {
            unreachable!()
        };
        Ok(alloc::sync::Arc::into_inner(storage).unwrap())
    }

    fn open(
        path: &Path,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<PagedCachedFile, DatabaseError> {
        open_file(reopen(path), read_only, concurrency_mode)
    }

    fn exclusive_writer(path: &Path, read_only: bool) -> Result<PagedCachedFile, DatabaseError> {
        open(path, read_only, ConcurrencyMode::ExclusiveWriter)
    }

    fn refused(result: Result<PagedCachedFile, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::DatabaseAlreadyOpen))
    }

    /// The cohort is another process, which may have been built with the multi-process feature
    /// when this one was not, so the probe is not gated on being able to form one
    #[test]
    fn a_read_only_open_refuses_a_cohort_this_build_cannot_form() {
        use crate::db::{SHARED_WRITER_BYTE, byte_range};

        let tmpfile = crate::create_tempfile();
        let cohort = reopen(tmpfile.path());
        assert!(
            cohort
                .try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))
                .unwrap()
        );

        assert!(refused(exclusive_writer(tmpfile.path(), true)));
    }

    #[test]
    fn exclusive_writer_locks_leave_backend_bytes_and_the_read_only_probe_free() {
        use crate::db::{BACKEND_LOCK_RANGE, SHARED_WRITER_BYTE, byte_range};

        let tmpfile = crate::create_tempfile();
        let observer = reopen(tmpfile.path());
        for read_only in [false, true] {
            let storage = exclusive_writer(tmpfile.path(), read_only).unwrap();
            for offset in [
                0,
                SHARED_WRITER_BYTE - 1,
                SHARED_WRITER_BYTE + 1,
                BACKEND_LOCK_RANGE.start - 1,
                BACKEND_LOCK_RANGE.end,
                i64::MAX as u64,
            ] {
                assert!(observer.query_lock(byte_range(offset)).unwrap());
            }
            assert_eq!(
                observer.query_lock(byte_range(SHARED_WRITER_BYTE)).unwrap(),
                !read_only
            );
            assert!(observer.try_lock_range(BACKEND_LOCK_RANGE).unwrap());
            observer.unlock_range(BACKEND_LOCK_RANGE).unwrap();
            storage.close().unwrap();
            assert!(observer.try_lock_range(..).unwrap());
            observer.unlock_range(..).unwrap();
        }
    }

    #[test]
    fn a_refused_single_process_open_releases_ranges_on_a_retained_file() {
        use crate::db::{SHARED_WRITER_BYTE, byte_range};

        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        // The tail is acquired before the conflict on the prefix.
        for read_only in [false, true] {
            assert!(
                holder
                    .try_lock_range(byte_range(SHARED_WRITER_BYTE - 1))
                    .unwrap()
            );
            let file = reopen(tmpfile.path());
            let retained = file.try_clone().unwrap();
            assert!(refused(open_file(
                file,
                read_only,
                ConcurrencyMode::ExclusiveWriter
            )));
            assert!(
                holder
                    .try_lock_range(byte_range(SHARED_WRITER_BYTE + 1))
                    .unwrap()
            );
            holder
                .unlock_range(byte_range(SHARED_WRITER_BYTE + 1))
                .unwrap();
            holder
                .unlock_range(byte_range(SHARED_WRITER_BYTE - 1))
                .unwrap();
            holder.try_lock().unwrap();
            holder.unlock().unwrap();
            drop(retained);
        }
    }

    #[cfg(not(feature = "experimental-api-5"))]
    #[test]
    fn exclusive_writer_opens_exclude_older_whole_file_writers() {
        let tmpfile = crate::create_tempfile();
        let older = reopen(tmpfile.path());
        for read_only in [false, true] {
            let storage = exclusive_writer(tmpfile.path(), read_only).unwrap();
            assert!(matches!(
                older.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            storage.close().unwrap();

            older.try_lock().unwrap();
            #[cfg(not(windows))]
            assert!(refused(exclusive_writer(tmpfile.path(), read_only)));
            #[cfg(windows)]
            {
                // Older whole-file locks cover the query mutex, so retrying waits for them.
                use std::sync::mpsc;
                use std::time::Duration;

                let path = tmpfile.path().to_owned();
                let (result_tx, result_rx) = mpsc::channel();
                let worker = std::thread::spawn(move || {
                    result_tx.send(exclusive_writer(&path, read_only)).unwrap();
                });
                let while_locked = result_rx.recv_timeout(Duration::from_millis(100));
                older.unlock().unwrap();
                assert!(matches!(while_locked, Err(mpsc::RecvTimeoutError::Timeout)));
                result_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .close()
                    .unwrap();
                worker.join().unwrap();
            }
            #[cfg(not(windows))]
            older.unlock().unwrap();
        }
    }

    #[cfg(not(feature = "experimental-api-5"))]
    #[test]
    fn each_whole_file_reader_excludes_older_whole_file_writers() {
        let tmpfile = crate::create_tempfile();
        let older = reopen(tmpfile.path());
        let first = exclusive_writer(tmpfile.path(), true).unwrap();
        let second = exclusive_writer(tmpfile.path(), true).unwrap();
        first.close().unwrap();
        assert!(matches!(
            older.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        second.close().unwrap();
        older.try_lock().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn multi_process_opens_do_not_take_legacy_locks() {
        let tmpfile = crate::create_tempfile();
        let older = reopen(tmpfile.path());
        older.try_lock().unwrap();
        // On filesystems that emulate flock with range locks, exclusion is unavoidable.
        if older.query_lock(0..=0).unwrap() {
            return;
        }
        for mode in [ConcurrencyMode::SingleWriter, ConcurrencyMode::MultiWriter] {
            for read_only in [false, true] {
                open(tmpfile.path(), read_only, mode)
                    .unwrap()
                    .close()
                    .unwrap();
            }
        }
    }

    /// Only a multi-writer cohort admits a second writer, and every mode byte is free again once
    /// its holder closes
    #[test]
    fn the_modes_exclude_each_other() {
        use ConcurrencyMode::{MultiWriter, SingleWriter};

        let tmpfile = crate::create_tempfile();
        for (held, joining, admitted) in [
            (SingleWriter, SingleWriter, false),
            (SingleWriter, MultiWriter, false),
            (MultiWriter, SingleWriter, false),
            (MultiWriter, MultiWriter, true),
        ] {
            let first = open(tmpfile.path(), false, held).unwrap();
            let second = open(tmpfile.path(), false, joining);
            assert_eq!(second.is_ok(), admitted, "{held:?} then {joining:?}");

            let reader = open(tmpfile.path(), true, MultiWriter).unwrap();
            reader.close().unwrap();
            if let Ok(second) = second {
                second.close().unwrap();
            }
            first.close().unwrap();
        }
    }

    /// A exclusive-writer handle joins nothing, so it and a multi-process one refuse each other --
    /// unless both are read-only, since neither holds a byte the other could find
    #[test]
    fn an_exclusive_writer_handle_and_a_multi_process_one_refuse_each_other() {
        let tmpfile = crate::create_tempfile();
        for mode in [ConcurrencyMode::SingleWriter, ConcurrencyMode::MultiWriter] {
            for read_only in [false, true] {
                let plain = exclusive_writer(tmpfile.path(), read_only).unwrap();
                assert!(refused(open(tmpfile.path(), false, mode)));
                assert_eq!(refused(open(tmpfile.path(), true, mode)), !read_only);
                plain.close().unwrap();

                let held = open(tmpfile.path(), read_only, mode).unwrap();
                assert!(refused(exclusive_writer(tmpfile.path(), false)));
                assert_eq!(refused(exclusive_writer(tmpfile.path(), true)), !read_only);
                held.close().unwrap();
            }
        }
    }

    /// Dropping the file does not suffice: a caller holding a `try_clone()` of the file it
    /// handed over keeps the open file description, and so the locks, alive
    #[test]
    fn a_refused_open_releases_the_bytes_it_took() {
        use crate::db::{SHARED_WRITER_BYTE, WRITER_BYTE, byte_range};

        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(byte_range(WRITER_BYTE)).unwrap());

        // The shared writer byte is taken before the conflict on the writer byte
        let file = reopen(tmpfile.path());
        let kept_by_the_caller = file.try_clone().unwrap();
        assert!(refused(open_file(
            file,
            false,
            ConcurrencyMode::SingleWriter
        )));

        let observer = reopen(tmpfile.path());
        let byte = byte_range(SHARED_WRITER_BYTE);
        assert!(observer.try_lock_range(byte).unwrap());
        observer.unlock_range(byte).unwrap();
        drop(kept_by_the_caller);

        holder.unlock_range(byte_range(WRITER_BYTE)).unwrap();
        open(tmpfile.path(), false, ConcurrencyMode::SingleWriter)
            .unwrap()
            .close()
            .unwrap();
    }
}

#[cfg(test)]
mod lock_failure_test {
    use super::TransactionalMemory;
    use crate::db::{BACKEND_LOCK_RANGE, ConcurrencyMode, FULL_RANGE, SHARED_WRITER_BYTE};
    use crate::io;
    use crate::sync::Mutex;
    use crate::tree_store::InMemoryBackend;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use crate::{BackendError, DatabaseError, StorageBackend, StorageError};
    use alloc::boxed::Box;
    use alloc::collections::VecDeque;
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::ops::{Bound, RangeBounds};
    use core::sync::atomic::{AtomicBool, Ordering};

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum Answer {
        Acquired,
        Refused,
        Unsupported,
        Failed,
    }

    impl Answer {
        fn result(self) -> Result<bool, BackendError> {
            match self {
                Answer::Acquired => Ok(true),
                Answer::Refused => Ok(false),
                Answer::Unsupported => Err(BackendError::Unsupported),
                Answer::Failed => Err(io::invalid_input("the lock could not be taken").into()),
            }
        }
    }

    #[derive(Debug)]
    struct AnsweringBackend {
        inner: InMemoryBackend,
        taking: Answer,
        partial_answers: Mutex<VecDeque<Answer>>,
        expected_shared: Option<bool>,
        fail_unlock: bool,
        querying: Answer,
        held: LockRanges,
        released: LockRanges,
        closed: AtomicBool,
    }

    impl AnsweringBackend {
        fn take(
            &self,
            range: (Bound<u64>, Bound<u64>),
            shared: bool,
        ) -> Result<bool, BackendError> {
            if let Some(expected) = self.expected_shared {
                assert_eq!(shared, expected);
            }
            assert!(
                self.held
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|held| !overlaps(*held, range))
            );
            let answer = if range == FULL_RANGE {
                self.taking
            } else {
                assert!(!overlaps(
                    range,
                    (
                        Bound::Included(BACKEND_LOCK_RANGE.start),
                        Bound::Excluded(BACKEND_LOCK_RANGE.end),
                    ),
                ));
                self.partial_answers
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(self.taking)
            };
            let acquired = answer.result();
            if matches!(acquired, Ok(true)) {
                self.held.lock().unwrap().push(range);
            }

            acquired
        }
    }

    impl StorageBackend for AnsweringBackend {
        fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
            self.take((start, end), false)
        }

        fn try_lock_shared_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> Result<bool, BackendError> {
            self.take((start, end), true)
        }

        fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
            self.take((start, end), false).map(|_| ())
        }

        fn lock_shared_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> Result<(), BackendError> {
            self.take((start, end), true).map(|_| ())
        }

        fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
            assert!(!self.closed.load(Ordering::Acquire), "unlock after close");
            if self.fail_unlock {
                return Err(io::invalid_input("unlock failed").into());
            }
            let mut held = self.held.lock().unwrap();
            let index = held
                .iter()
                .position(|range| *range == (start, end))
                .expect("unlock must exactly match an acquired range");
            held.remove(index);
            self.released.lock().unwrap().push((start, end));
            Ok(())
        }

        fn query_lock_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> Result<bool, BackendError> {
            assert!(!overlaps(
                (start, end),
                (
                    Bound::Included(BACKEND_LOCK_RANGE.start),
                    Bound::Excluded(BACKEND_LOCK_RANGE.end),
                ),
            ));
            assert!(
                self.held
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|held| !overlaps(*held, (start, end)))
            );
            self.querying.result()
        }

        fn close(&self) -> Result<(), io::Error> {
            assert!(!self.closed.swap(true, Ordering::AcqRel), "closed twice");
            for range in self.held.lock().unwrap().drain(..) {
                self.released.lock().unwrap().push(range);
            }

            Ok(())
        }

        fn len(&self) -> Result<u64, io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), io::Error> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
            self.inner.write(offset, data)
        }
    }

    fn overlaps(a: (Bound<u64>, Bound<u64>), b: (Bound<u64>, Bound<u64>)) -> bool {
        let first = |range: (Bound<u64>, Bound<u64>)| match range.0 {
            Bound::Unbounded => 0,
            Bound::Included(start) => start,
            Bound::Excluded(start) => start + 1,
        };
        a.contains(&first(b)) || b.contains(&first(a))
    }

    type LockRanges = Arc<Mutex<Vec<(Bound<u64>, Bound<u64>)>>>;

    fn backend(taking: Answer, querying: Answer) -> (AnsweringBackend, LockRanges) {
        let released: LockRanges = Arc::new(Mutex::new(Vec::new()));
        (
            AnsweringBackend {
                inner: InMemoryBackend::new(),
                taking,
                partial_answers: Mutex::new(VecDeque::new()),
                expected_shared: None,
                fail_unlock: false,
                querying,
                held: Arc::new(Mutex::new(Vec::new())),
                released: released.clone(),
                closed: AtomicBool::new(false),
            },
            released,
        )
    }

    fn storage(taking: Answer, querying: Answer) -> (super::NativePageStorage, LockRanges) {
        let (backend, released) = backend(taking, querying);
        (
            super::NativePageStorage::ordinary(Arc::new(PagedCachedFile::new(
                Box::new(backend),
                4096,
                0,
            ))),
            released,
        )
    }

    #[test]
    fn caller_supplied_lock_results_control_the_open() {
        for taking in [
            Answer::Acquired,
            Answer::Refused,
            Answer::Unsupported,
            Answer::Failed,
        ] {
            let (backend, released) = backend(taking, Answer::Unsupported);
            let result = crate::Database::builder().create_with_backend(backend);
            match taking {
                Answer::Acquired | Answer::Unsupported => drop(result.unwrap()),
                Answer::Refused => {
                    assert!(matches!(result, Err(DatabaseError::DatabaseAlreadyOpen)));
                }
                Answer::Failed => assert!(matches!(
                    result,
                    Err(DatabaseError::Storage(StorageError::Io(err)))
                        if err.kind() == std::io::ErrorKind::InvalidInput
                )),
            }
            let expected = if taking == Answer::Acquired {
                vec![
                    (Bound::Included(BACKEND_LOCK_RANGE.end), Bound::Unbounded),
                    (
                        Bound::Excluded(SHARED_WRITER_BYTE),
                        Bound::Excluded(BACKEND_LOCK_RANGE.start),
                    ),
                    (Bound::Unbounded, Bound::Included(SHARED_WRITER_BYTE)),
                ]
            } else {
                vec![]
            };
            assert_eq!(*released.lock().unwrap(), expected);
        }
    }

    fn open_as(
        concurrency_mode: ConcurrencyMode,
        read_only: bool,
        taking: Answer,
        querying: Answer,
    ) -> Result<(), DatabaseError> {
        TransactionalMemory::lock_for_open(
            &storage(taking, querying).0,
            read_only,
            concurrency_mode,
        )
    }

    fn open(read_only: bool, taking: Answer, querying: Answer) -> Result<(), DatabaseError> {
        open_as(
            ConcurrencyMode::ExclusiveWriter,
            read_only,
            taking,
            querying,
        )
    }

    fn failed(result: Result<(), DatabaseError>) -> bool {
        matches!(
            result,
            Err(DatabaseError::Storage(
                StorageError::Io(_) | StorageError::Unsupported
            ))
        )
    }

    #[test]
    fn a_platform_without_locks_opens_anyway() {
        open(false, Answer::Unsupported, Answer::Unsupported).unwrap();
        open(true, Answer::Unsupported, Answer::Unsupported).unwrap();

        // A granular backend must support queries to safely exclude idle multi-writers.
        assert!(failed(open(true, Answer::Acquired, Answer::Unsupported)));
    }

    #[test]
    fn a_conflicting_lock_reports_the_database_already_open() {
        for read_only in [false, true] {
            assert!(matches!(
                open(read_only, Answer::Refused, Answer::Acquired),
                Err(DatabaseError::DatabaseAlreadyOpen)
            ));
        }
    }

    #[test]
    fn a_lock_that_fails_for_any_other_reason_fails_the_open() {
        assert!(failed(open(false, Answer::Failed, Answer::Acquired)));
        assert!(failed(open(true, Answer::Failed, Answer::Acquired)));

        // ... and so does a probe that fails, which only a read-only open makes
        assert!(failed(open(true, Answer::Acquired, Answer::Failed)));
        open(false, Answer::Acquired, Answer::Failed).unwrap();
    }

    // A read-only open included: its mode still declares writers that need the locks
    #[test]
    fn a_shared_mode_without_locks_fails_the_open() {
        for mode in [ConcurrencyMode::SingleWriter, ConcurrencyMode::MultiWriter] {
            for read_only in [false, true] {
                assert!(failed(open_as(
                    mode,
                    read_only,
                    Answer::Unsupported,
                    Answer::Unsupported
                )));
            }
        }
    }

    #[test]
    fn the_lock_is_released_at_close() {
        for taking in [Answer::Acquired, Answer::Unsupported] {
            let (storage, released) = storage(taking, Answer::Unsupported);
            TransactionalMemory::lock_for_open(&storage, false, ConcurrencyMode::ExclusiveWriter)
                .unwrap();
            storage.close().unwrap();

            let expected: Vec<(Bound<u64>, Bound<u64>)> = if taking == Answer::Acquired {
                vec![
                    (Bound::Included(BACKEND_LOCK_RANGE.end), Bound::Unbounded),
                    (
                        Bound::Excluded(SHARED_WRITER_BYTE),
                        Bound::Excluded(BACKEND_LOCK_RANGE.start),
                    ),
                    (Bound::Unbounded, Bound::Included(SHARED_WRITER_BYTE)),
                ]
            } else {
                vec![]
            };
            assert_eq!(*released.lock().unwrap(), expected);
        }
    }

    #[test]
    fn whole_storage_fallback_releases_partial_locks_and_skips_queries() {
        for read_only in [false, true] {
            for partial_count in 0..=2 {
                for answer in [
                    Answer::Acquired,
                    Answer::Refused,
                    Answer::Unsupported,
                    Answer::Failed,
                ] {
                    let (mut backend, released) = backend(answer, Answer::Failed);
                    backend.expected_shared = Some(read_only);
                    backend.partial_answers = Mutex::new(
                        core::iter::repeat_n(Answer::Acquired, partial_count)
                            .chain([Answer::Unsupported])
                            .collect(),
                    );
                    let held = backend.held.clone();
                    let storage = super::NativePageStorage::ordinary(Arc::new(
                        PagedCachedFile::new(Box::new(backend), 4096, 0),
                    ));
                    let result = TransactionalMemory::lock_for_open(
                        &storage,
                        read_only,
                        ConcurrencyMode::ExclusiveWriter,
                    );
                    match answer {
                        Answer::Acquired => {
                            result.unwrap();
                            assert_eq!(*held.lock().unwrap(), [FULL_RANGE]);
                        }
                        Answer::Refused => {
                            assert!(matches!(result, Err(DatabaseError::DatabaseAlreadyOpen)));
                        }
                        Answer::Unsupported => result.unwrap(),
                        Answer::Failed => assert!(failed(result)),
                    }
                    assert_eq!(released.lock().unwrap().len(), partial_count);
                    if answer != Answer::Acquired {
                        assert!(held.lock().unwrap().is_empty());
                    }
                    storage.close().unwrap();
                    assert!(held.lock().unwrap().is_empty());
                }
            }
        }
    }

    #[test]
    fn a_partial_conflict_or_io_error_does_not_fall_back() {
        for read_only in [false, true] {
            for answer in [Answer::Refused, Answer::Failed] {
                let (mut backend, _) = backend(Answer::Acquired, Answer::Refused);
                backend.partial_answers = Mutex::new([Answer::Acquired, answer].into());
                let held = backend.held.clone();
                let storage = super::NativePageStorage::ordinary(Arc::new(PagedCachedFile::new(
                    Box::new(backend),
                    4096,
                    0,
                )));
                let result = TransactionalMemory::lock_for_open(
                    &storage,
                    read_only,
                    ConcurrencyMode::ExclusiveWriter,
                );
                if answer == Answer::Refused {
                    assert!(matches!(result, Err(DatabaseError::DatabaseAlreadyOpen)));
                } else {
                    assert!(failed(result));
                }
                assert_ne!(*held.lock().unwrap(), [FULL_RANGE]);
                storage.close().unwrap();
                assert!(held.lock().unwrap().is_empty());
            }
        }
    }

    #[test]
    fn a_failed_unlock_prevents_whole_storage_fallback() {
        let (mut backend, _) = backend(Answer::Acquired, Answer::Refused);
        backend.partial_answers = Mutex::new([Answer::Acquired, Answer::Unsupported].into());
        backend.fail_unlock = true;
        let held = backend.held.clone();
        let storage = super::NativePageStorage::ordinary(Arc::new(PagedCachedFile::new(
            Box::new(backend),
            4096,
            0,
        )));
        assert!(failed(TransactionalMemory::lock_for_open(
            &storage,
            false,
            ConcurrencyMode::ExclusiveWriter
        )));
        assert_eq!(held.lock().unwrap().len(), 1);
        storage.close().unwrap();
        assert!(held.lock().unwrap().is_empty());
    }

    #[test]
    fn granular_read_only_open_queries_outside_its_locks() {
        open(true, Answer::Acquired, Answer::Refused).unwrap();
        assert!(matches!(
            open(true, Answer::Acquired, Answer::Acquired),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
    }
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "owned_tracking_tests.rs"]
mod owned_tracking_tests;

#[cfg(all(test, not(redb_no_std)))]
mod allocator_handoff_retry_tests {
    use super::super::page_resources::NativePageResources;
    use super::*;
    use crate::{Database, StorageError};

    fn database() -> (tempfile::NamedTempFile, Database) {
        let file = crate::create_tempfile();
        let mut builder = Database::builder();
        builder.set_page_size(512).set_region_size(512 * 8192);
        let db = builder.create(file.path()).unwrap();
        (file, db)
    }
    fn install_backing(
        db: &Database,
        write: &crate::WriteTransaction,
        seed_cache: bool,
    ) -> NativePageResources {
        let mem = db.get_memory();
        let guard = write.transaction_guard();
        guard
            .tracker()
            .check_source_no_savepoint_writer(&mem, &guard)
            .unwrap();
        let layout = mem.state.lock().unwrap().header.layout();
        let next = layout.growth_for(8).unwrap();
        let allocator_bytes = {
            let state = mem.state.lock().unwrap();
            state
                .allocators()
                .source_resize_storage_quote(layout, next)
                .unwrap()
                .simultaneous_peak_bytes
        };
        let cache = mem
            .point_payload_first_cache_control_capacity_for_test()
            .unwrap();
        let page = NativePageResources::page_allocation_bound(512).unwrap();
        let total = usize::try_from(allocator_bytes)
            .unwrap()
            .checked_add(cache)
            .unwrap()
            .checked_add(page)
            .unwrap();
        let resources = NativePageResources::from_retained_pool_data(total, ()).unwrap();
        if seed_cache {
            // Genuine reserved/native page, immediately returned/freed. Its
            // physical owner retires; only the actual sticky flag remains.
            let page = mem
                .allocate_helper_with_resources(512, false, Some(&resources))
                .unwrap();
            let number = page.get_page_number();
            drop(page);
            mem.free(number, &PageTracker::ignore());
            assert!(mem.storage.has_source_write_metadata());
        } else {
            assert!(!mem.storage.has_source_write_metadata());
        }
        let mut state = mem.state.lock().unwrap();
        let prepared = state
            .allocators_mut()
            .prepare_source_resize(layout, next, &resources)
            .unwrap();
        drop(prepared); // unused new regions die; installed changed buffers stay
        assert!(state.allocators().has_source_storage());
        assert!(resources.current() > 0);
        resources
    }
    // req: STO-020, PRJ-004
    #[test]
    fn native_ordinary_writer_handoffs_allocator_storage_without_source_cache_metadata() {
        let (_file, db) = database();
        let first = db.begin_write().unwrap();
        let resources = install_backing(&db, &first, false);
        first.abort().unwrap();
        assert!(!db.get_memory().storage.has_source_write_metadata());
        assert!(resources.current() > 0);
        let retry = db.begin_write().unwrap();
        assert_eq!(
            resources.current(),
            0,
            "ordinary constructor must inspect independent allocator owner"
        );
        retry.abort().unwrap();
    }
    // req: STO-020, PRJ-004, REC-001
    #[test]
    fn native_ordinary_writer_retries_partial_allocator_handoff_after_cache_flag_clears() {
        let (_file, db) = database();
        let first = db.begin_write().unwrap();
        let resources = install_backing(&db, &first, true);
        first.abort().unwrap();
        let mem = db.get_memory();
        let before = resources.current();
        mem.state
            .lock()
            .unwrap()
            .allocators_mut()
            .fail_ordinary_handoff_after_for_test(1);
        let failed = db.begin_write();
        assert!(
            matches!(failed,Err(crate::TransactionError::Storage(StorageError::Io(ref error))) if error.kind()==std::io::ErrorKind::OutOfMemory)
        );
        drop(failed);
        assert!(
            !mem.storage.has_source_write_metadata(),
            "actual cache handoff finished before injected next ordinary reserve failure"
        );
        assert!(
            resources.current() > 0 && resources.current() < before,
            "one physical array retired, remaining actual owners stay charged"
        );
        assert!(mem.state.lock().unwrap().allocators().has_source_storage());
        // A real retry also proves the failed constructor released Live writer
        // ownership. No manual tracker release or synthetic transaction ID.
        let retry = db.begin_write().unwrap();
        assert_eq!(resources.current(), 0);
        assert!(!mem.state.lock().unwrap().allocators().has_source_storage());
        retry.abort().unwrap();
    }
}

#[cfg(all(test, debug_assertions, not(redb_no_std)))]
#[path = "debug_page_set_tests.rs"]
mod debug_page_set_tests;
