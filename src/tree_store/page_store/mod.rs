mod point_frontier_capacity;
pub(crate) use point_frontier_capacity::{
    point_byte_forest_bytes, point_frontier_capacity, point_vector_bytes,
};
#[cfg(all(test, not(redb_no_std)))]
mod point_mutator_trace;
#[cfg(all(test, not(redb_no_std)))]
pub(crate) use point_mutator_trace::{NativePointMutatorTrace, NativePointTraceScope};
mod active_transactions;
mod allocator_growth;
mod allocator_purge_capacity;
pub(crate) use allocator_purge_capacity::{NativePurgeVectorQuote, native_purge_vector_quote};
pub(crate) mod allocator_reopen_facts;
mod backends;
mod base;
mod bitmap;
mod buddy_allocator;
mod cached_file;
mod container_bounds;
#[cfg(all(debug_assertions, not(redb_no_std)))]
mod copied_page_metadata;

mod fast_hash;
#[cfg(not(redb_no_std))]
pub mod file_backend;
mod header;
mod layout;
mod lru_cache;
mod page_bytes;
mod page_manager;
mod page_resources;
mod read_only_resource;
pub(crate) use page_resources::NativePageResources;
pub(crate) use read_only_resource::{
    ReadOnlyResourceProfile, readonly_lock_entry_capacity_bytes,
    readonly_tracker_node_capacity_bytes,
};
mod resource_build_identity;
mod resource_build_profile;
pub(crate) use page_bytes::SharedPageBytes;
mod region;
mod savepoint;
#[allow(clippy::pedantic, dead_code)]
mod xxh3;

pub use backends::InMemoryBackend;
#[cfg(not(redb_no_std))]
pub(crate) use backends::ReadOnlyBackend;
pub(crate) use base::{
    MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, Page, PageHint, PageNumber, PageTracker, ScopedBorrow,
};
pub(crate) use fast_hash::{PageNumberHashMap, PageNumberHashSet};
pub(crate) use header::{DB_HEADER_SIZE, PAGE_SIZE};
pub(crate) use layout::DatabaseLayout;
#[cfg(all(test, feature = "experimental-multiprocess"))]
pub(crate) use page_manager::HEADER_LOCK;
pub(crate) use page_manager::{
    AllocationPolicy, FILE_FORMAT_VERSION3, HeaderGuard, PageAllocator, PageResolver, ShrinkPolicy,
    TransactionalMemory, WriterLock, xxh3_checksum,
};
pub use savepoint::Savepoint;
pub(crate) use savepoint::SerializedSavepoint;

#[cfg(all(test, not(redb_no_std)))]
pub(crate) use base::PageImpl as NativeFixedPhasePage;
pub(super) use base::{PageImpl, PageMut};
pub(super) use xxh3::hash128_with_seed;

#[cfg(not(redb_no_std))]
pub(crate) use page_manager::NativeReadContext;
#[cfg(not(redb_no_std))]
pub(crate) use read_only_resource::NativeReadLimits;

mod owned_shared;
pub(crate) use owned_shared::NativeOwnedShared;
mod owned_page_batch;
pub(crate) use owned_page_batch::{NativeLocalPageVec, NativeOwnedPageBatch, NativeOwnedPageVec};

#[cfg(not(redb_no_std))]
mod owned_transaction_markers;
#[cfg(not(redb_no_std))]
pub(crate) use owned_transaction_markers::NativeOwnedTransactionMarkers;

#[cfg(not(redb_no_std))]
pub(crate) fn source_owner_refused() -> crate::StorageError {
    page_resources::refused()
}

mod owned_allocator_vec;

#[cfg(debug_assertions)]
mod debug_page_set;

#[cfg(not(redb_no_std))]
pub(crate) use page_manager::NativePreparedDebugFamilies;

mod txn_name_storage;
pub(crate) use txn_name_storage::{NativeTableName, NativeTxnNameMap};

mod txn_id_storage;
pub(crate) use txn_id_storage::NativeOwnedTxnIds;

#[cfg(not(redb_no_std))]
pub(crate) use page_resources::{NativeClosedNumericalEpoch, NativeNumericalEpoch};

mod owned_value_bytes;
pub(crate) use owned_value_bytes::NativeOwnedValueBytes;

#[cfg(not(redb_no_std))]
mod fixed_recipe;
#[cfg(not(redb_no_std))]
pub(crate) use fixed_recipe::{NativeFixedPoint, NativeFixedTableViews, NativeOwnedFixedRecipe};

#[cfg(not(redb_no_std))]
pub(crate) use page_resources::NativeControlAllocation;

#[cfg(not(redb_no_std))]
pub(crate) use page_resources::NativeFixedObservationAdmission;

#[cfg(not(redb_no_std))]
mod cold_read_quote;
#[cfg(not(redb_no_std))]
pub(crate) use cold_read_quote::{
    cold_read_fixed_capacity_bytes, native_cold_fixed_error_work_bytes,
    native_cold_format_work_bytes, prepare_cold_read_admission, prepare_cold_read_error_work,
    prepare_cold_read_fixed,
};
