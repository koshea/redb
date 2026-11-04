//! Closed native readonly profile. Numerical bounds confer no database authority.
use super::allocator_reopen_facts::{AllocatorReopenFacts, AllocatorResourceError};
use super::container_bounds::{self, ContainerBoundError};
use super::page_resources::NativePageResources;
use core::mem::{align_of, size_of};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
pub(crate) struct ReadOnlyReadObservations {
    branch: AtomicUsize,
    leaf: AtomicUsize,
    definition: AtomicUsize,
    unknown: AtomicBool,
}
impl ReadOnlyReadObservations {
    pub(super) fn page(&self, bytes: &[u8]) {
        match bytes.first() {
            Some(2) => {
                self.branch.fetch_max(bytes.len(), Ordering::AcqRel);
            }
            Some(1) => {
                self.leaf.fetch_max(bytes.len(), Ordering::AcqRel);
            }
            _ => {
                self.unknown.store(true, Ordering::Release);
            }
        }
    }
    pub(super) fn definition(&self, len: usize) {
        self.definition.fetch_max(len, Ordering::AcqRel);
    }
    // Numerical read limits from this parsed native engine; no reopen/parser facts.
    // Unobserved larger pages remain a typed refusal at the read boundary.
    pub(super) fn capture_read_limits(
        &self,
        page_size: usize,
    ) -> Result<NativeReadLimits, AllocatorResourceError> {
        // Retain the compiled native layout-profile guard for the context,
        // cursor and control quotes. Fresh copied pages now have separately
        // preadmitted metadata; their reads never grow the primary debug map.
        super::resource_build_profile::NativeResourceBuildProfile::require().map_err(map_bound)?;
        if self.unknown.load(Ordering::Acquire) {
            return Err(AllocatorResourceError::Unsupported);
        }
        let branch = self.branch.load(Ordering::Acquire).max(page_size);
        let leaf = self.leaf.load(Ordering::Acquire);
        let definition = self.definition.load(Ordering::Acquire);
        if leaf == 0 || definition == 0 {
            return Err(AllocatorResourceError::Incomplete);
        }
        let branch_pages = crate::tree_store::btree_base::MAX_BTREE_DEPTH
            .checked_add(4)
            .ok_or(AllocatorResourceError::Overflow)?;
        let page_bytes = NativePageResources::page_allocation_bound(branch)
            .map_err(|_| AllocatorResourceError::Overflow)?
            .checked_mul(branch_pages)
            .and_then(|v| {
                NativePageResources::page_allocation_bound(leaf)
                    .ok()
                    .and_then(|n| n.checked_mul(4))
                    .and_then(|n| v.checked_add(n))
            })
            .ok_or(AllocatorResourceError::Overflow)?;
        Ok(NativeReadLimits {
            branch,
            leaf,
            definition,
            page_bytes,
            base_page: page_size,
        })
    }
    pub(super) fn capture(
        &self,
        allocator: AllocatorReopenFacts,
        page_size: usize,
    ) -> Result<ReadOnlyResourceProfile, AllocatorResourceError> {
        if self.unknown.load(Ordering::Acquire) {
            return Err(AllocatorResourceError::Unsupported);
        }
        let branch = self.branch.load(Ordering::Acquire);
        let leaf = self.leaf.load(Ordering::Acquire);
        let definition = self.definition.load(Ordering::Acquire);
        if leaf == 0 || definition == 0 {
            return Err(AllocatorResourceError::Incomplete);
        }
        // A single forward range retains at most MAX_BTREE_DEPTH branch pages;
        // four additional branches cover master/system lookup and transition.
        // Four leaves cover the cursor, yielded row and a transient next page.
        let branch = branch.max(page_size);
        let branch_bytes = NativePageResources::page_allocation_bound(branch)
            .map_err(|_| AllocatorResourceError::Overflow)?;
        let leaf_bytes = NativePageResources::page_allocation_bound(leaf)
            .map_err(|_| AllocatorResourceError::Overflow)?;
        let branch_pages = crate::tree_store::btree_base::MAX_BTREE_DEPTH
            .checked_add(4)
            .ok_or(AllocatorResourceError::Overflow)?;
        let page_bytes = branch_bytes
            .checked_mul(branch_pages)
            .and_then(|n| leaf_bytes.checked_mul(4).and_then(|v| n.checked_add(v)))
            .ok_or(AllocatorResourceError::Overflow)?;
        Ok(ReadOnlyResourceProfile {
            allocator,
            branch,
            leaf,
            definition,
            page_bytes,
            max_live_pages: branch_pages
                .checked_add(4)
                .ok_or(AllocatorResourceError::Overflow)?,
        })
    }
}
#[derive(Clone, Copy)]
pub(crate) struct ReadOnlyResourceProfile {
    pub(super) allocator: AllocatorReopenFacts,
    pub(super) branch: usize,
    pub(super) leaf: usize,
    pub(super) definition: usize,
    pub(super) page_bytes: usize,
    pub(super) max_live_pages: usize,
}
impl ReadOnlyResourceProfile {
    // Capture is heap-free, but its fixed working graph must be prepaid.
    // This union deliberately counts both completed limits and their construction
    // intermediates together. Repeated allocation-bound calls are sequential;
    // one nested invocation's arguments, results and checked arithmetic suffice.
    pub(crate) fn read_context_capture_control_bytes() -> Result<u64, AllocatorResourceError> {
        use super::container_bounds::{AllocationBound, ContainerBoundError};
        use super::resource_build_profile::NativeResourceBuildProfile;
        NativeResourceBuildProfile::require().map_err(map_bound)?;
        let controls = size_of::<(
            // page size, branch, leaf, definition, branch-page count, page bytes
            [usize; 6],
            NativeReadLimits,
            Result<NativeReadLimits, AllocatorResourceError>,
            Result<NativeResourceBuildProfile, ContainerBoundError>,
            // page_allocation_bound: bytes, payload, owner and checked result
            [usize; 3],
            Result<usize, crate::StorageError>,
            // container allocation: size/alignment, Layout and complete result
            [usize; 2],
            core::alloc::Layout,
            Result<core::alloc::Layout, core::alloc::LayoutError>,
            AllocationBound,
            Result<AllocationBound, ContainerBoundError>,
            Result<u64, AllocatorResourceError>,
            // checked add/multiply/conversion temporaries in the nested owners
            Option<usize>,
            Option<u64>,
            u64,
        )>();
        u64::try_from(controls).map_err(|_| AllocatorResourceError::Overflow)
    }
    pub(crate) fn page_bytes(self) -> usize {
        self.page_bytes
    }
    pub(crate) fn allocator_facts(self) -> AllocatorReopenFacts {
        self.allocator
    }
    pub(crate) fn maximum_observed_leaf_bytes(self) -> usize {
        self.leaf
    }
    pub(crate) fn metadata_bytes(self) -> usize {
        self.definition
    }
    pub(super) fn before_page(self, len: usize) -> crate::Result {
        if len > self.branch.max(self.maximum_observed_leaf_bytes()) {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    pub(super) fn after_page(self, bytes: &[u8]) -> crate::Result {
        let bound = match bytes.first() {
            Some(1) => self.maximum_observed_leaf_bytes(),
            Some(2) => self.branch,
            _ => return Err(super::page_resources::refused()),
        };
        if bytes.len() > bound {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    pub(super) fn before_definition(self, len: usize) -> crate::Result {
        if len > self.definition {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    pub(crate) fn allocation(
        bytes: usize,
        alignment: usize,
    ) -> Result<u64, AllocatorResourceError> {
        container_bounds::allocation(bytes, alignment)
            .map(|b| b.allocator_bytes)
            .map_err(map_bound)
    }
    pub(crate) fn typed_allocation<T>() -> Result<u64, AllocatorResourceError> {
        Self::allocation(size_of::<T>(), align_of::<T>())
    }
    pub(crate) fn arc_allocation<T>() -> Result<u64, AllocatorResourceError> {
        Self::allocation(
            size_of::<T>()
                .checked_add(2 * size_of::<usize>())
                .ok_or(AllocatorResourceError::Overflow)?,
            align_of::<T>().max(align_of::<usize>()),
        )
    }
    pub(crate) fn allocator_bound(self, peak: bool) -> Result<u64, AllocatorResourceError> {
        let (bytes, count) = if peak {
            // During resize/new child construction, every Vec's capacity grows
            // monotonically to its final capacity. At most one old/new buffer
            // replacement executes at a time, so one complete extra old/final
            // union covers this transient. The sole parser's own temporary
            // buffers and peak were already observed separately.
            (
                self.allocator
                    .peak_capacity_bytes()
                    .checked_mul(2)
                    .ok_or(AllocatorResourceError::Overflow)?,
                self.allocator
                    .peak_buffer_count()
                    .checked_mul(2)
                    .ok_or(AllocatorResourceError::Overflow)?,
            )
        } else {
            let f = self.allocator.final_retained();
            (f.capacity_bytes(), f.buffer_count())
        };
        // All parser allocations are Vec of u8/u64/usize or native allocator
        // structs, whose alignment is no larger than this actual type union.
        let alignment = align_of::<super::buddy_allocator::BuddyAllocator>()
            .max(align_of::<super::region::RegionTracker>())
            .max(align_of::<usize>());
        let overhead = alignment
            .checked_mul(2)
            .and_then(|v| v.checked_add(256))
            .ok_or(AllocatorResourceError::Overflow)?;
        let bytes = bytes
            .checked_mul(2)
            .and_then(|v| count.checked_mul(overhead).and_then(|n| n.checked_add(v)))
            .ok_or(AllocatorResourceError::Overflow)?;
        u64::try_from(bytes).map_err(|_| AllocatorResourceError::Overflow)
    }
}
pub(super) fn map_bound(error: ContainerBoundError) -> AllocatorResourceError {
    match error {
        ContainerBoundError::Overflow | ContainerBoundError::InvalidLayout => {
            AllocatorResourceError::Overflow
        }
        _ => AllocatorResourceError::Unsupported,
    }
}

pub(crate) fn readonly_tracker_node_capacity_bytes() -> Result<u64, AllocatorResourceError> {
    use crate::transaction_tracker::TransactionId;
    container_bounds::btree_storage::<TransactionId, u64>(1).map_err(map_bound)
}
pub(crate) fn readonly_lock_entry_capacity_bytes() -> Result<u64, AllocatorResourceError> {
    use core::ops::Bound;
    container_bounds::hash_storage_from_historical_fresh_capacity::<(Bound<u64>, Bound<u64>)>(1)
        .map(|b| b.allocator_bytes)
        .map_err(map_bound)
}

// Shared numerical page/type-name bounds; contains no allocator or Source facts.
#[derive(Clone, Copy)]
pub(crate) struct NativeReadLimits {
    // Captured from SAME native memory owner, never caller-selected geometry.
    pub(crate) base_page: usize,
    pub(crate) branch: usize,
    pub(crate) leaf: usize,
    pub(crate) definition: usize,
    pub(crate) page_bytes: usize,
}
impl NativeReadLimits {
    pub(super) fn before_page(self, len: usize) -> crate::Result {
        if len > self.branch.max(self.leaf) {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    pub(super) fn after_page(self, bytes: &[u8]) -> crate::Result {
        let bound = match bytes.first() {
            Some(1) => self.leaf,
            Some(2) => self.branch,
            _ => return Err(super::page_resources::refused()),
        };
        if bytes.len() > bound {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    pub(super) fn before_definition(self, len: usize) -> crate::Result {
        if len > self.definition {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
}
