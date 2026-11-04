//! Actual empty native pagination interval; no prospective mutation authority.
//! This retains original structural records. It does not certify a physical
//! multi-version peak or an evolved pre/post insertion/deletion union.
use super::*;
use crate::transactions::{NativeFreedAppendGapRecipe, PageList, TransactionIdWithPagination};
use crate::tree_store::{NativeControlAllocation, NativeFixedObservationAdmission};
use core::mem::size_of;

type FreedTree = Btree<TransactionIdWithPagination, PageList<'static>>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Edge {
    node: Node,
    parent: Option<Node>,
    edge: Option<usize>,
    depth: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Endpoints {
    lower: Option<Edge>,
    upper: Option<Edge>,
    // Captured by the SAME native descent and upper-leaf scan, repeated under
    // the same original read/epoch. Empty root is a genuine right-end case.
    right_end: bool,
}
fn last_edge(path: &Path) -> Result<Edge, E> {
    let depth = path.length.checked_sub(1).ok_or(E::Malformed)?;
    Ok(Edge {
        node: path.nodes[depth].ok_or(E::Malformed)?,
        parent: depth
            .checked_sub(1)
            .map(|i| path.nodes[i].ok_or(E::Malformed))
            .transpose()?,
        edge: depth.checked_sub(1).map(|i| path.edges[i]),
        depth,
    })
}
fn same_path(left: &Path, right: &Path) -> bool {
    left.length == right.length
        && left.nodes[..left.length] == right.nodes[..right.length]
        && left.edges[..left.length.saturating_sub(1)]
            == right.edges[..right.length.saturating_sub(1)]
}
struct GapScratch {
    paths: [Path; 3],
    work: Work,
    endpoints: Endpoints,
    parser: Scratch,
    page: PageImpl,
    accessor: LeafAccessor<'static>,
}
struct GapCapture<'tree, 'recipe, 'epoch, 'tracker> {
    prepared: NativeFreedAppendGapPreparation<'tree, 'recipe, 'epoch, 'tracker>,
    shape: NativeFreedAppendGapShape,
    result: Result<NativeFreedAppendGapShape, E>,
    recipe: &'recipe NativeFreedAppendGapRecipe<'tree, 'epoch, 'tracker>,
    parts: crate::db::CheckedPinnedReadParts<'tree>,
    admission: NativeFixedObservationAdmission,
    control: NativeControlAllocation,
}
// Structural backing FIRST, all its retained numerical allowances LAST.
// Original read/root/generation seals remain in the enclosing prepared owner.
pub(crate) struct NativeFreedAppendGapShape {
    records: Vec<Record>,
    root: Option<BtreeHeader>,
    memory: *const crate::tree_store::TransactionalMemory,
    read_id: crate::transaction_tracker::TransactionId,
    lower: [u8; 2 * size_of::<u64>()],
    upper: [u8; 2 * size_of::<u64>()],
    endpoints: Endpoints,
    old_physical_bytes: u64,
    leaves: usize,
    branches: usize,
    boundary_children: usize,
    observed_fetches: usize,
    admission: NativeFixedObservationAdmission,
    _storage: NativeControlAllocation,
    _capture: NativeControlAllocation,
}
impl NativeFreedAppendGapShape {
    // Only the sibling phase module consumes this borrowed original structure;
    // access is not a current-state or independent epoch authority.
    pub(super) fn phase_records(&self) -> &[Record] {
        &self.records
    }
    pub(super) fn phase_binding(
        &self,
    ) -> (
        Option<BtreeHeader>,
        *const crate::tree_store::TransactionalMemory,
        crate::transaction_tracker::TransactionId,
    ) {
        (self.root, self.memory, self.read_id)
    }
    pub(super) fn require_phase_pool(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> Result<(), E> {
        self.admission.require_current()?;
        if !self._storage.belongs_to(resources) || !self._capture.belongs_to(resources) {
            return Err(E::ForeignOwner);
        }
        // The old interval component may describe an interior gap. Only the
        // new ordered prefix/right-end phase class requires this stronger fact.
        if !self.endpoints.right_end {
            return Err(E::UnsupportedState);
        }
        Ok(())
    }
    pub(crate) fn old_pages(&self) -> usize {
        self.records.len()
    }
    pub(crate) fn old_physical_bytes(&self) -> u64 {
        self.old_physical_bytes
    }
    pub(crate) fn old_leaves(&self) -> usize {
        self.leaves
    }
    pub(crate) fn old_branches(&self) -> usize {
        self.branches
    }
    pub(crate) fn boundary_children(&self) -> usize {
        self.boundary_children
    }
    // Constructor-local consistency only. This method is private to the
    // owning module and cannot validate a later epoch/generation by itself.
    // Production geometry stays inseparable from the original prepared or
    // detached SYSTEM owner, whose existing original binding MUST revalidate
    // before any consuming recipe/plan/executor operation.
    fn require_captured_parts(
        &self,
        actual: &FreedTree,
        epoch: &NativeResourceEpoch<'_>,
    ) -> Result<(), E> {
        actual.require_freed_point_epoch(epoch)?;
        self.admission.require_current()?;
        actual
            .mem
            .require_fixed_observation_allocation(&self._storage)?;
        let parts = actual
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        if self.memory != Arc::as_ptr(parts.actual_memory())
            || self.root != actual.root
            || self.read_id != parts.transaction_id()
        {
            return Err(E::ForeignOwner);
        }
        self.admission.require_current()?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn require_interval_for_test(
        &self,
        lower: &TransactionIdWithPagination,
        upper: &TransactionIdWithPagination,
    ) -> Result<(), E> {
        self.admission.require_current()?;
        if self.lower != TransactionIdWithPagination::as_bytes(lower)
            || self.upper != TransactionIdWithPagination::as_bytes(upper)
        {
            return Err(E::ForeignOwner);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn edge_witness_for_test(
        &self,
    ) -> Option<(PageNumber, Checksum, usize, PageNumber, Checksum, usize)> {
        let left = self.endpoints.lower?;
        let right = self.endpoints.upper?;
        Some((
            left.parent?.page,
            left.parent?.checksum,
            left.edge?,
            right.parent?.page,
            right.parent?.checksum,
            right.edge?,
        ))
    }
    #[cfg(test)]
    pub(crate) fn parent_checksums_for_test(&self) -> bool {
        self.endpoints
            .lower
            .and_then(|edge| edge.parent)
            .is_some_and(|node| node.checksum != DEFERRED)
            && self
                .endpoints
                .upper
                .and_then(|edge| edge.parent)
                .is_some_and(|node| node.checksum != DEFERRED)
    }
    #[cfg(test)]
    pub(crate) fn observed_fetches_for_test(&self) -> usize {
        self.observed_fetches
    }
}
pub(crate) struct NativeFreedAppendGapPreparation<'tree, 'recipe, 'epoch, 'tracker> {
    actual: &'tree FreedTree,
    recipe: &'recipe NativeFreedAppendGapRecipe<'tree, 'epoch, 'tracker>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    visits: usize,
    endpoints: Endpoints,
    record_bytes: u64,
    capture: NativeControlAllocation,
}
impl NativeFreedAppendGapPreparation<'_, '_, '_, '_> {
    pub(crate) fn collect(self) -> Result<NativeFreedAppendGapShape, E> {
        self.recipe.require_for(self.actual, self.epoch)?;
        let storage = self.actual.mem.admit_fixed_observation(
            usize::try_from(self.record_bytes).map_err(|_| E::Overflow)?,
        )?;
        let mut records = Vec::new();
        // Supported pinned RawVec::grow_exact on this fresh non-ZST Vec stores
        // capacity=visits and requests its exact Record array layout. The sole
        // container helper above prices that request+allocator allowance;
        // point_fetch refuses before any push could grow this backing.
        records
            .try_reserve_exact(self.visits)
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        let (visits, endpoints) = self
            .actual
            .walk_freed_append_gap(self.recipe, Some(&mut records))?;
        if visits != self.visits || endpoints != self.endpoints {
            return Err(E::Malformed);
        }
        self.recipe.require_for(self.actual, self.epoch)?;
        let parts = self
            .actual
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        let mut physical = 0_u64;
        let mut leaves = 0_usize;
        let mut branches = 0_usize;
        let mut children = 0_usize;
        for record in &records {
            physical = add64(
                physical,
                u64::try_from(record.physical).map_err(|_| E::Overflow)?,
            )?;
            if record.branch {
                branches = add(branches, 1)?;
                children = add(children, add(record.count, 1)?)?;
            } else {
                leaves = add(leaves, 1)?;
            }
        }
        let boundary_children = if records.is_empty() {
            0
        } else {
            children
                .checked_sub(records.len() - 1)
                .ok_or(E::Malformed)?
        };
        let admission = self.actual.mem.seal_fixed_observation_admission()?;
        let shape = NativeFreedAppendGapShape {
            records,
            root: self.actual.root,
            memory: Arc::as_ptr(parts.actual_memory()),
            read_id: parts.transaction_id(),
            lower: *self.recipe.lower_bytes(),
            upper: *self.recipe.upper_bytes(),
            endpoints,
            old_physical_bytes: physical,
            leaves,
            branches,
            boundary_children,
            observed_fetches: visits,
            admission,
            _storage: storage,
            _capture: self.capture,
        };
        shape.require_captured_parts(self.actual, self.epoch)?;
        Ok(shape)
    }
}
impl FreedTree {
    fn gap_work() -> Result<Work, E> {
        // Two root-to-leaf paths: <=2*DEPTH fetches. One forward neighbor:
        // <=(DEPTH-1) pivot + (DEPTH-1) descent fetches. Two endpoint checks.
        // Total <=4*DEPTH, independently of future append row count.
        Ok(Work {
            remaining: DEPTH.checked_mul(4).ok_or(E::Overflow)?,
            visited: 0,
            leaf_depth: None,
        })
    }
    fn require_gap_leaf_empty(
        &self,
        path: &Path,
        recipe: &NativeFreedAppendGapRecipe<'_, '_, '_>,
        work: &mut Work,
        records: &mut Option<&mut Vec<Record>>,
    ) -> Result<bool, E> {
        let edge = last_edge(path)?;
        let page = self.point_fetch(
            edge.node,
            edge.parent.map(|n| n.page),
            edge.edge,
            edge.depth,
            work,
            records,
        )?;
        if page.memory()[0] != LEAF {
            return Err(E::Malformed);
        }
        let leaf = LeafAccessor::new(
            page.memory(),
            TransactionIdWithPagination::fixed_width(),
            PageList::fixed_width(),
        );
        if leaf.num_pairs() == 0 {
            return Err(E::UnsupportedState);
        }
        for index in 0..leaf.num_pairs() {
            let key = leaf.entry(index).ok_or(E::Malformed)?.key();
            if !TransactionIdWithPagination::compare(key, recipe.lower_bytes()).is_lt()
                && TransactionIdWithPagination::compare(key, recipe.upper_bytes()).is_lt()
            {
                return Err(E::UnsupportedState);
            }
        }
        let right_end = path.rightmost
            && TransactionIdWithPagination::compare(leaf.last_entry().key(), recipe.upper_bytes())
                .is_lt();
        drop(page); // No PageImpl survives the next fetch.
        Ok(right_end)
    }
    fn walk_freed_append_gap(
        &self,
        recipe: &NativeFreedAppendGapRecipe<'_, '_, '_>,
        mut records: Option<&mut Vec<Record>>,
    ) -> Result<(usize, Endpoints), E> {
        let mut work = Self::gap_work()?;
        if self.root.is_none() {
            return Ok((
                0,
                Endpoints {
                    lower: None,
                    upper: None,
                    right_end: true,
                },
            ));
        }
        let lower = self.point_path(recipe.lower_bytes(), &mut work, &mut records)?;
        let upper = self.point_path(recipe.upper_bytes(), &mut work, &mut records)?;
        let mut endpoints = Endpoints {
            lower: Some(last_edge(&lower)?),
            upper: Some(last_edge(&upper)?),
            right_end: false,
        };
        if lower.length != upper.length {
            return Err(E::Malformed);
        }
        if last_edge(&lower)?.node == last_edge(&upper)?.node {
            // Equal PN alone is insufficient, including a malformed repeated edge.
            if !same_path(&lower, &upper) {
                return Err(E::Malformed);
            }
        } else {
            let mut neighbor = lower;
            if !self.point_neighbor(
                &mut neighbor,
                lower.length - 1,
                true,
                &mut work,
                &mut records,
            )? || !same_path(&neighbor, &upper)
            {
                return Err(E::UnsupportedState);
            }
        }
        self.require_gap_leaf_empty(&lower, recipe, &mut work, &mut records)?;
        endpoints.right_end =
            self.require_gap_leaf_empty(&upper, recipe, &mut work, &mut records)?;
        Ok((work.visited, endpoints))
    }
    pub(crate) fn prepare_freed_append_gap<'tree, 'recipe, 'epoch, 'tracker>(
        &'tree self,
        recipe: &'recipe NativeFreedAppendGapRecipe<'tree, 'epoch, 'tracker>,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
    ) -> Result<NativeFreedAppendGapPreparation<'tree, 'recipe, 'epoch, 'tracker>, E> {
        recipe.require_for(self, epoch)?;
        let capture_bytes = crate::tree_store::page_store::point_frontier_capacity::<
            Record,
            GapCapture<'_, '_, '_, '_>,
            GapScratch,
        >(0)?;
        let capture = self
            .mem
            .admit_fixed_observation(usize::try_from(capture_bytes).map_err(|_| E::Overflow)?)?;
        // No Record Vec allocation in the first census. Existing Source context
        // still admits every actual copied page BEFORE its physical allocation.
        let (visits, endpoints) = self.walk_freed_append_gap(recipe, None)?;
        recipe.require_for(self, epoch)?;
        let record_bytes = crate::tree_store::page_store::point_frontier_capacity::<
            Record,
            GapCapture<'_, '_, '_, '_>,
            GapScratch,
        >(visits)?;
        Ok(NativeFreedAppendGapPreparation {
            actual: self,
            recipe,
            epoch,
            visits,
            endpoints,
            record_bytes,
            capture,
        })
    }
}
