mod btree;
mod btree_base;
mod btree_cursor;
mod fixed_scratch;
pub(crate) use btree_cursor::readonly_cursor_capacity_bytes;
pub(crate) use page_store::{
    NativePageResources, ReadOnlyResourceProfile, readonly_lock_entry_capacity_bytes,
    readonly_tracker_node_capacity_bytes,
};
mod btree_cursor_range;
mod btree_iters;
mod btree_mutator;
mod extract_if;
mod multimap_btree;
pub(crate) mod page_store;
#[cfg(all(test, not(redb_no_std)))]
pub(crate) use page_store::NativeFixedPhasePage;
#[cfg(all(test, not(redb_no_std)))]
pub(crate) use page_store::NativePointMutatorTrace;
mod table_tree;
mod table_tree_base;

pub(crate) use btree::{Btree, BtreeMut, BtreeStats, NativeFreedEntryRead, RawBtree};
pub(crate) use btree::{
    CompiledPathVisits, NativeEpochKeyBatch, NativeEpochKeyPath, NativeKeyPathError,
    NativePreparedNormalKey,
};
pub(crate) use btree::{NativeAllocatorPurgeFacts, NativeAllocatorPurgeWork};
pub(crate) use btree::{NativeSystemNamespaceFacts, NativeSystemNamespaceProfile};
pub(crate) use btree_base::BtreeHeader;
pub use btree_base::{AccessGuard, AccessGuardMut, AccessGuardMutInPlace};
pub(crate) use btree_base::{BRANCH, LEAF, LeafAccessor, RawLeafBuilder};
pub(crate) use btree_cursor::{BtreeCursor, BtreeCursorMut};
pub(crate) use btree_cursor_range::BtreeCursorRange;
pub(crate) use btree_iters::{AllPageNumbersBtreeIter, encode_bounds};
pub(crate) use extract_if::BtreeExtractIf;
pub(crate) use multimap_btree::{DynamicCollection, DynamicCollectionType, multimap_btree_stats};
#[cfg(all(test, feature = "experimental-multiprocess"))]
pub(crate) use page_store::HEADER_LOCK;
#[cfg(not(redb_no_std))]
pub(crate) use page_store::ReadOnlyBackend;
pub(crate) use page_store::allocator_reopen_facts::AllocatorResourceError;
#[cfg(not(redb_no_std))]
pub use page_store::file_backend;
pub(crate) use page_store::{
    AllocationPolicy, DB_HEADER_SIZE, DatabaseLayout, FILE_FORMAT_VERSION3, HeaderGuard,
    MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, PAGE_SIZE, Page, PageAllocator, PageHint, PageNumber,
    PageNumberHashMap, PageNumberHashSet, PageResolver, PageTracker, ScopedBorrow,
    SerializedSavepoint, ShrinkPolicy, TransactionalMemory, WriterLock,
};
pub use page_store::{InMemoryBackend, Savepoint};
pub(crate) use page_store::{NativePurgeVectorQuote, native_purge_vector_quote};
pub(crate) use table_tree::{PageListMut, TableTree, TableTreeMut};
pub(crate) use table_tree_base::NativeDefinitionResourceError;
pub(crate) use table_tree_base::{InternalTableDefinition, TableType};

pub(crate) use page_store::SharedPageBytes;

#[cfg(not(redb_no_std))]
pub(crate) use page_store::{NativeReadContext, NativeReadLimits};

pub(crate) use btree::{
    NativeOwnedPointFrontierFacts, NativeOwnedPointFrontierPreparation, NativePointFrontierFacts,
    NativePointFrontierPreparation, NativePointKey, NativePointMutation, NativePointRecipe,
    NativePointRecipeClass, point_frontier_capture_bytes,
};

pub(crate) use page_store::{NativeLocalPageVec, NativeOwnedPageBatch, NativeOwnedPageVec};

pub(crate) use page_store::NativeOwnedShared;

#[cfg(not(redb_no_std))]
pub(crate) use page_store::NativeOwnedTransactionMarkers;

#[cfg(not(redb_no_std))]
pub(crate) use page_store::source_owner_refused;

#[cfg(not(redb_no_std))]
pub(crate) use page_store::NativePreparedDebugFamilies;

pub(crate) use page_store::{NativeTableName, NativeTxnNameMap};

pub(crate) use page_store::NativeOwnedTxnIds;

#[cfg(not(redb_no_std))]
pub(crate) use page_store::{NativeClosedNumericalEpoch, NativeNumericalEpoch};

pub(crate) use page_store::NativeOwnedValueBytes;

#[cfg(not(redb_no_std))]
pub(crate) use page_store::{NativeFixedPoint, NativeFixedTableViews, NativeOwnedFixedRecipe};

#[cfg(not(redb_no_std))]
pub(crate) use page_store::NativeControlAllocation;

#[cfg(not(redb_no_std))]
pub(crate) use btree::NativeFixedPointCells;

#[cfg(not(redb_no_std))]
pub(crate) use btree::{NativeFreedPointFrontierShape, freed_point_selection_capacity_bytes};

#[cfg(not(redb_no_std))]
pub(crate) use table_tree::NORMAL_NAMESPACE_OBSERVATION;
#[cfg(not(redb_no_std))]
pub(crate) use table_tree::{
    NativeFixedNamespaceCensus, NativeNamespacePutPhase, NativeNormalNamespaceError,
    NativeNormalNamespaceRecipe,
};

#[cfg(not(redb_no_std))]
pub(crate) use page_store::NativeFixedObservationAdmission;

#[cfg(not(redb_no_std))]
pub(crate) use btree::NativeFreedAppendGapShape;

#[cfg(not(redb_no_std))]
pub(crate) use btree::{NativeFreedPhaseForest, NativeFreedPhaseGeometry};

#[cfg(not(redb_no_std))]
pub(crate) use btree::NativeChecksumPool;

#[cfg(not(redb_no_std))]
pub(crate) use btree_base::NativeEntryBufferPool;

pub(crate) use btree_cursor::FixedCursorKey;
#[cfg(not(redb_no_std))]
pub(crate) use btree_cursor::NativeFixedCursorPool;

#[cfg(not(redb_no_std))]
pub(crate) use btree_mutator::NativeFixedPackingPool;

#[cfg(not(redb_no_std))]
mod owned_branch_children_source;
#[cfg(not(redb_no_std))]
pub(crate) use owned_branch_children_source::NativeBranchChildrenPool;

mod owned_branch_keys;
#[cfg(not(redb_no_std))]
pub(crate) use owned_branch_keys::NativeBranchKeyPool;
