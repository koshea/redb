//! Actual point-recipe DATA closure. This is not the full native write plan.
use super::resource_key_paths::{
    NativeKeyPathError as E, SUPPORTED_PATH_DEPTH as DEPTH, check_layout,
};
use super::resource_point_recipe::NativePointRecipe;
use super::{
    BRANCH, BranchAccessor, Btree, BtreeHeader, Checksum, DEFERRED, LEAF, LeafAccessor, Page,
    PageImpl, PageNumber, branch_checksum, leaf_checksum,
};
use crate::transaction_tracker::NativeResourceEpoch;
use crate::{Key, Value};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::TypeId;

// Closed numerical recipe view. The ordinary compiler remains byte/str only;
// native freed recipes enter only through their distinct typed owner below.
trait PointRecipeView {
    fn points(&self) -> usize;
    fn key_at(&self, index: usize) -> Result<&[u8], E>;
    fn puts(&self) -> usize;
    fn deletes(&self) -> usize;
    fn incoming_bytes(&self) -> usize;
    fn maximum_key(&self) -> usize;
    fn maximum_value(&self) -> usize;
    fn class(&self) -> super::resource_point_recipe::NativePointRecipeClass;
}
impl PointRecipeView for NativePointRecipe<'_> {
    fn points(&self) -> usize {
        self.operations.len()
    }
    fn key_at(&self, index: usize) -> Result<&[u8], E> {
        match self.operations.get(index).ok_or(E::Malformed)? {
            super::resource_point_recipe::NativePointMutation::Put { key, .. }
            | super::resource_point_recipe::NativePointMutation::Delete { key } => Ok(key.bytes()),
        }
    }
    fn puts(&self) -> usize {
        self.puts
    }
    fn deletes(&self) -> usize {
        self.deletes
    }
    fn incoming_bytes(&self) -> usize {
        self.incoming_bytes
    }
    fn maximum_key(&self) -> usize {
        self.maximum_key
    }
    fn maximum_value(&self) -> usize {
        self.operations
            .iter()
            .filter_map(|op| match op {
                super::resource_point_recipe::NativePointMutation::Put { value, .. } => {
                    Some(value.len())
                }
                super::resource_point_recipe::NativePointMutation::Delete { .. } => None,
            })
            .max()
            .unwrap_or(0)
    }
    fn class(&self) -> super::resource_point_recipe::NativePointRecipeClass {
        self.class()
    }
}
#[cfg(not(redb_no_std))]
impl PointRecipeView for crate::transactions::NativeFreedPointRecipe<'_, '_, '_> {
    fn points(&self) -> usize {
        self.points()
    }
    fn key_at(&self, index: usize) -> Result<&[u8], E> {
        self.key_at(index)
    }
    fn puts(&self) -> usize {
        0
    }
    fn deletes(&self) -> usize {
        self.points()
    }
    fn incoming_bytes(&self) -> usize {
        0
    }
    fn maximum_key(&self) -> usize {
        2 * size_of::<u64>()
    }
    fn maximum_value(&self) -> usize {
        0
    }
    fn class(&self) -> super::resource_point_recipe::NativePointRecipeClass {
        super::resource_point_recipe::NativePointRecipeClass::DeleteOnly
    }
}

#[cfg(not(redb_no_std))]
impl PointRecipeView for crate::tree_store::NativeNormalNamespaceRecipe<'_, '_, '_, '_> {
    fn points(&self) -> usize {
        self.points()
    }
    fn key_at(&self, index: usize) -> Result<&[u8], E> {
        self.key_at(index)
    }
    fn puts(&self) -> usize {
        self.points()
    }
    fn deletes(&self) -> usize {
        0
    }
    fn incoming_bytes(&self) -> usize {
        self.incoming_bytes()
    }
    fn maximum_key(&self) -> usize {
        self.maximum_key()
    }
    fn maximum_value(&self) -> usize {
        self.maximum_value()
    }
    fn class(&self) -> super::resource_point_recipe::NativePointRecipeClass {
        super::resource_point_recipe::NativePointRecipeClass::PutOnly
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Node {
    page: PageNumber,
    checksum: Checksum,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Record {
    node: Node,
    parent: Option<PageNumber>,
    parent_edge: Option<usize>,
    depth: usize,
    physical: usize,
    branch: bool,
    count: usize,
    payload: usize,
    key_bytes: usize,
    maximum_key: usize,
    maximum_value: usize,
}
#[derive(Clone, Copy)]
struct Path {
    nodes: [Option<Node>; DEPTH],
    edges: [usize; DEPTH],
    length: usize,
    // Proof only immediately after the owning point_path descent; neighboring
    // traversal invalidates it. Actual sizeof<Path>/Scratch quotes include it.
    rightmost: bool,
}
impl Path {
    fn empty() -> Self {
        Self {
            nodes: [None; DEPTH],
            edges: [0; DEPTH],
            length: 0,
            rightmost: true,
        }
    }
}
struct Work {
    remaining: usize,
    visited: usize,
    leaf_depth: Option<usize>,
}
impl Work {
    fn new(recipe: &impl PointRecipeView) -> Result<Self, E> {
        let windows = recipe
            .deletes()
            .checked_mul(2)
            .and_then(|v| v.checked_mul(DEPTH))
            .and_then(|v| v.checked_add(1))
            .ok_or(E::Overflow)?;
        let remaining = recipe
            .points()
            .checked_mul(DEPTH)
            .and_then(|v| v.checked_mul(windows))
            .ok_or(E::Overflow)?;
        Ok(Self {
            remaining,
            visited: 0,
            leaf_depth: None,
        })
    }
    fn before_fetch(&mut self) -> Result<(), E> {
        self.remaining = self.remaining.checked_sub(1).ok_or(E::VisitsExceeded)?;
        self.visited = self.visited.checked_add(1).ok_or(E::Overflow)?;
        Ok(())
    }
}
struct IncomingEdgeFrame {
    caller: Option<usize>,
    callee: Option<usize>,
    fetched: Record,
}
struct Scratch {
    incoming_edge_frame: IncomingEdgeFrame,
    path: Path,
    neighbor: Path,
    work: Work,
}
struct CaptureControls {
    recipe: NativePointRecipe<'static>,
    preparation: NativePointFrontierPreparation<
        'static,
        'static,
        'static,
        'static,
        &'static [u8],
        &'static [u8],
    >,
    facts: NativePointFrontierFacts<'static, 'static>,
    work: Work,
    owned_preparation: NativeOwnedPointFrontierPreparation<
        'static,
        'static,
        'static,
        &'static [u8],
        &'static [u8],
    >,
    owned_facts: NativeOwnedPointFrontierFacts<'static, 'static>,
}

// No vector or PageImpl is constructed by the capture-control quote.
pub(crate) fn point_frontier_capture_bytes() -> Result<u64, E> {
    crate::tree_store::page_store::point_frontier_capacity::<Record, CaptureControls, Scratch>(0)
}

pub(crate) struct NativePointFrontierPreparation<
    'tree,
    'recipe,
    'epoch,
    'tracker,
    K: Key + 'static,
    V: Value + 'static,
> {
    actual: &'tree Btree<K, V>,
    actual_name: &'tree str,
    recipe: &'recipe NativePointRecipe<'recipe>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    visits: usize,
    record_bytes: u64,
}
impl<'tree, 'recipe, 'epoch, 'tracker, K: Key, V: Value>
    NativePointFrontierPreparation<'tree, 'recipe, 'epoch, 'tracker, K, V>
{
    pub(crate) fn record_capacity_bytes(&self) -> u64 {
        self.record_bytes
    }
    pub(crate) fn observed_fetches(&self) -> usize {
        self.visits
    }
    // The enclosing closed first-party owner must admit record_capacity_bytes
    // BEFORE this call. This native numerical component cannot mint admission.
    fn collect_records(&self) -> Result<Vec<Record>, E> {
        self.actual.require_point_epoch(self.epoch)?;
        let mut records = Vec::with_capacity(self.visits);
        let mut work = Work::new(self.recipe)?;
        self.actual
            .walk_point_recipe(self.recipe, &mut work, Some(&mut records))?;
        if work.visited != self.visits {
            return Err(E::Malformed);
        }
        self.actual.require_point_epoch(self.epoch)?;
        Ok(records)
    }
    pub(crate) fn collect(self) -> Result<NativePointFrontierFacts<'epoch, 'tracker>, E> {
        let records = self.collect_records()?;
        let facts = NativePointFrontierFacts::from_records(
            self.actual,
            self.actual_name,
            self.recipe,
            self.actual.mem.resource_base_page_size(),
            self.epoch,
            &records,
        )?;
        drop(records);
        Ok(facts)
    }
    // Complete DATA capture calls this immediately on its OWN immutable
    // recipe view. No caller-supplied facts can be rebound to another recipe.
    // The actual resolver, not an independently paired pool, admits record and
    // retained name storage before the sole existing collect parser runs.
    #[cfg(not(redb_no_std))]
    pub(crate) fn collect_fixed_cells(self) -> Result<NativeFixedPointCells, E> {
        let allocation = self.actual.mem.admit_fixed_observation(
            usize::try_from(self.record_bytes).map_err(|_| E::Overflow)?,
        )?;
        let actual = self.actual;
        let name = self.actual_name;
        let mut facts = self.collect()?;
        facts.validate_same_current_tree(actual, name)?;
        let result = NativeFixedPointCells {
            table_name: core::mem::take(&mut facts.table_name),
            root: facts.root,
            memory: facts.memory,
            read_id: facts.read_id,
            old_pages: facts.old_pages,
            old_physical_bytes: facts.old_bytes,
            scratch: facts.scratch,
            maximum_key: facts.shape.maximum_key,
            final_live_physical_upper: facts.final_physical_upper,
            checksum_depth: facts
                .maximum_depth
                .checked_add(facts.shape.incoming_pairs)
                .ok_or(E::Overflow)?
                .max(1),
            _records_and_name: allocation,
        };
        drop(facts);
        Ok(result)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn collect_pages_for_test(
        self,
    ) -> Result<(NativePointFrontierFacts<'epoch, 'tracker>, Vec<PageNumber>), E> {
        let records = self.collect_records()?;
        let facts = NativePointFrontierFacts::from_records(
            self.actual,
            self.actual_name,
            self.recipe,
            self.actual.mem.resource_base_page_size(),
            self.epoch,
            &records,
        )?;
        let pages = records.iter().map(|r| r.node.page).collect();
        drop(records);
        Ok((facts, pages))
    }
}
impl<K: Key, V: Value> Drop for NativePointFrontierPreparation<'_, '_, '_, '_, K, V> {
    fn drop(&mut self) {}
}

// These values cannot provide write/Source authority or stand in for the other
// native allocation families. The epoch borrows the actual tracker, not a DTO.
pub(crate) struct NativePointFrontierFacts<'epoch, 'tracker> {
    root: Option<BtreeHeader>,
    memory: *const crate::tree_store::TransactionalMemory,
    read_id: crate::transaction_tracker::TransactionId,
    table_name: Vec<u8>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    old_pages: usize,
    old_bytes: u64,
    leaves: usize,
    branches: usize,
    boundary_children: usize,
    future_leaves: usize,
    future_branches: usize,
    final_physical_upper: u64,
    maximum_depth: usize,
    scratch: crate::tree_store::btree_base::NativePointScratchUpper,
    shape: NativePointShape,
}
// Actual observed aggregates, never caller-supplied resource/Source authority.
#[derive(Clone, Copy)]
struct NativePointShape {
    old_pairs: usize,
    old_children: usize,
    maximum_old_pairs: usize,
    maximum_old_children: usize,
    maximum_old_leaf_bytes: usize,
    maximum_old_branch_bytes: usize,
    maximum_key: usize,
    maximum_value: usize,
    incoming_pairs: usize,
    incoming_payload_bytes: usize,
    class: super::resource_point_recipe::NativePointRecipeClass,
}
impl<'epoch, 'tracker> NativePointFrontierFacts<'epoch, 'tracker> {
    fn from_records<K: Key, V: Value>(
        actual: &Btree<K, V>,
        actual_name: &str,
        recipe: &impl PointRecipeView,
        base: usize,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
        records: &[Record],
    ) -> Result<Self, E> {
        let root = actual.root;
        let parts = actual
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        let memory = Arc::as_ptr(parts.actual_memory());
        let read_id = parts.transaction_id();
        let mut leaves = 0_usize;
        let mut branches = 0_usize;
        let mut children = 0_usize;
        let mut pairs = 0_usize;
        let mut payload = recipe.incoming_bytes();
        let mut branch_keys = 0_usize;
        let mut max_key = recipe.maximum_key();
        let mut maximum_depth = 0_usize;
        let mut old_bytes = 0_u64;
        let mut maximum_old_pairs = 0_usize;
        let mut maximum_old_children = 0_usize;
        let mut maximum_old_leaf_bytes = 0_usize;
        let mut maximum_old_branch_bytes = 0_usize;
        let mut maximum_value = recipe.maximum_value();
        for record in records {
            maximum_depth = maximum_depth.max(record.depth + 1);
            old_bytes = add64(
                old_bytes,
                u64::try_from(record.physical).map_err(|_| E::Overflow)?,
            )?;
            max_key = max_key.max(record.maximum_key);
            maximum_value = maximum_value.max(record.maximum_value);
            match record.parent {
                None if root.is_some_and(|r| r.root == record.node.page)
                    && record.depth == 0
                    && record.parent_edge.is_none() => {}
                Some(parent) => {
                    let p = records
                        .iter()
                        .find(|r| r.node.page == parent)
                        .ok_or(E::Malformed)?;
                    if !p.branch
                        || p.depth.checked_add(1) != Some(record.depth)
                        || !record.parent_edge.is_some_and(|edge| edge <= p.count)
                    {
                        return Err(E::Malformed);
                    }
                }
                _ => return Err(E::Malformed),
            }
            if record.branch {
                maximum_old_children = maximum_old_children.max(add(record.count, 1)?);
                maximum_old_branch_bytes = maximum_old_branch_bytes.max(record.physical);
                branches = add(branches, 1)?;
                children = add(children, add(record.count, 1)?)?;
                branch_keys = add(branch_keys, record.key_bytes)?;
            } else {
                maximum_old_pairs = maximum_old_pairs.max(record.count);
                maximum_old_leaf_bytes = maximum_old_leaf_bytes.max(record.physical);
                leaves = add(leaves, 1)?;
                pairs = add(pairs, record.count)?;
                payload = add(payload, record.payload)?;
            }
        }
        if root.is_some() != !records.is_empty() {
            return Err(E::Malformed);
        }
        let boundary_children = if records.is_empty() {
            0
        } else {
            children
                .checked_sub(records.len() - 1)
                .ok_or(E::Malformed)?
        };
        let future_leaves = add(leaves, recipe.puts())?;
        let terminals = add(boundary_children, future_leaves)?;
        let future_branches = terminals.saturating_sub(1);
        // Native point insertion creates at most one additional leaf, including
        // the oversized singleton and append fastpaths. Branch nodes have at
        // least two children; no minimum byte occupancy is assumed here.
        let future_pairs = add(pairs, recipe.puts())?;
        let leaf_header =
            crate::tree_store::btree_base::RawLeafBuilder::required_bytes(0, 0, None, None);
        let leaf_pair =
            crate::tree_store::btree_base::RawLeafBuilder::required_bytes(1, 0, None, None)
                - leaf_header;
        let branch_header =
            crate::tree_store::btree_base::RawBranchBuilder::required_bytes(0, 0, None);
        let branch_pair =
            crate::tree_store::btree_base::RawBranchBuilder::required_bytes(1, 0, None)
                - branch_header;
        let leaf_headers = mul(future_leaves, leaf_header)?;
        let leaf_offsets = mul(future_pairs, leaf_pair)?;
        let leaf_required = add(add(leaf_headers, leaf_offsets)?, payload)?;
        let fresh_separators = mul(add(recipe.puts(), recipe.deletes())?, max_key)?;
        let branch_required = if future_branches == 0 {
            0
        } else {
            add(
                add(
                    mul(future_branches, branch_header)?,
                    mul(terminals - 1, branch_pair)?,
                )?,
                add(branch_keys, fresh_separators)?,
            )?
        };
        // Exact RawLeafBuilder/RawBranchBuilder geometry above; native page
        // allocation rounds to base*next_power_of_two(ceil(required/base)).
        // Sum <=2*sum(required)+base*nodes. This is FINAL LIVE geometry only;
        // old read pages and one-operation split/builder overlaps are separate.
        let required = add(leaf_required, branch_required)?;
        let physical = add(
            mul(required, 2)?,
            mul(base, add(future_leaves, future_branches)?)?,
        )?;
        let scratch = crate::tree_store::btree_base::point_mutator_scratch_upper(
            future_pairs,
            terminals,
            add(branch_keys, fresh_separators)?,
            max_key,
            maximum_value,
        )?;
        let shape = NativePointShape {
            old_pairs: pairs,
            old_children: children,
            maximum_old_pairs,
            maximum_old_children,
            maximum_old_leaf_bytes,
            maximum_old_branch_bytes,
            maximum_key: max_key,
            maximum_value,
            incoming_pairs: recipe.puts(),
            incoming_payload_bytes: recipe.incoming_bytes(),
            class: recipe.class(),
        };
        Ok(Self {
            root,
            memory,
            read_id,
            table_name: actual_name.as_bytes().to_vec(),
            epoch,
            old_pages: records.len(),
            old_bytes,
            leaves,
            branches,
            boundary_children,
            future_leaves,
            future_branches,
            final_physical_upper: u64::try_from(physical).map_err(|_| E::Overflow)?,
            maximum_depth,
            scratch,
            shape,
        })
    }
    pub(crate) fn scratch_heap_upper(&self) -> u64 {
        self.scratch.heap_bytes
    }
    pub(crate) fn leaf_pair_capacity_upper(&self) -> usize {
        self.scratch.leaf_pairs
    }
    pub(crate) fn branch_child_capacity_upper(&self) -> usize {
        self.scratch.branch_children
    }
    pub(crate) fn branch_key_capacity_upper(&self) -> usize {
        self.scratch.branch_keys
    }
    pub(crate) fn owned_key_payload_upper(&self) -> usize {
        self.scratch.owned_key_bytes
    }
    pub(crate) fn maximum_value_bytes(&self) -> usize {
        self.shape.maximum_value
    }
    pub(crate) fn maximum_key_bytes(&self) -> usize {
        self.shape.maximum_key
    }
    pub(crate) fn old_pair_count(&self) -> usize {
        self.shape.old_pairs
    }
    pub(crate) fn maximum_old_leaf_bytes(&self) -> usize {
        self.shape.maximum_old_leaf_bytes
    }
    pub(crate) fn maximum_old_branch_bytes(&self) -> usize {
        self.shape.maximum_old_branch_bytes
    }
    pub(crate) fn require_current(&self) -> Result<(), E> {
        self.epoch.require_current().map_err(E::Epoch)
    }
    pub(crate) fn old_pages(&self) -> usize {
        self.old_pages
    }
    pub(crate) fn old_physical_bytes(&self) -> u64 {
        self.old_bytes
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
    pub(crate) fn future_leaf_upper(&self) -> usize {
        self.future_leaves
    }
    pub(crate) fn future_branch_upper(&self) -> usize {
        self.future_branches
    }
    pub(crate) fn final_live_physical_upper(&self) -> u64 {
        self.final_physical_upper
    }
    pub(crate) fn maximum_depth(&self) -> usize {
        self.maximum_depth
    }
    pub(crate) fn data_freed_entry_upper(&self) -> usize {
        self.old_pages.div_ceil(400)
    }
    pub(crate) fn validate_same_current_tree<K: Key, V: Value>(
        &self,
        tree: &Btree<K, V>,
        actual_name: &str,
    ) -> Result<(), E> {
        tree.require_point_epoch(self.epoch)?;
        let parts = tree
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        if self.root != tree.root
            || self.memory != Arc::as_ptr(parts.actual_memory())
            || self.read_id != parts.transaction_id()
            || self.table_name != actual_name.as_bytes()
        {
            return Err(E::UnsupportedState);
        }
        Ok(())
    }
}
impl Drop for NativePointFrontierFacts<'_, '_> {
    fn drop(&mut self) {}
}
// Own the actual native epoch alongside detached numerical cells. No raw
// read/PageImpl/guard survives collect; no self-reference or Send widening.
pub(crate) struct NativeOwnedPointFrontierPreparation<
    'tree,
    'recipe,
    'tracker,
    K: Key + 'static,
    V: Value + 'static,
> {
    actual: &'tree Btree<K, V>,
    actual_name: &'tree str,
    recipe: &'recipe NativePointRecipe<'recipe>,
    epoch: NativeResourceEpoch<'tracker>,
    visits: usize,
    record_bytes: u64,
}
struct DetachedPointFrontier {
    root: Option<BtreeHeader>,
    memory: *const crate::tree_store::TransactionalMemory,
    read_id: crate::transaction_tracker::TransactionId,
    table_name: Vec<u8>,
    old_pages: usize,
    scratch: crate::tree_store::btree_base::NativePointScratchUpper,
    shape: NativePointShape,
    final_physical_upper: u64,
}
pub(crate) struct NativeOwnedPointFrontierFacts<'recipe, 'tracker> {
    recipe: &'recipe NativePointRecipe<'recipe>,
    cells: DetachedPointFrontier,
    epoch: NativeResourceEpoch<'tracker>,
}
impl<'tree, 'recipe, 'tracker, K: Key, V: Value>
    NativeOwnedPointFrontierPreparation<'tree, 'recipe, 'tracker, K, V>
{
    pub(crate) fn record_capacity_bytes(&self) -> u64 {
        self.record_bytes
    }
    // record_capacity_bytes and the fixed capture quote must already be admitted
    // by the real enclosing owner; this component mints no resource allowance.
    pub(crate) fn collect(self) -> Result<NativeOwnedPointFrontierFacts<'recipe, 'tracker>, E> {
        // All borrowed observations refer to this exact epoch; they are consumed
        // and physically destroyed before moving it into the returned owner.
        let cells = {
            let preparation = NativePointFrontierPreparation {
                actual: self.actual,
                actual_name: self.actual_name,
                recipe: self.recipe,
                epoch: &self.epoch,
                visits: self.visits,
                record_bytes: self.record_bytes,
            };
            let mut facts = preparation.collect()?;
            facts.validate_same_current_tree(self.actual, self.actual_name)?;
            if !core::ptr::eq(facts.epoch, &self.epoch) {
                return Err(E::UnsupportedState);
            }
            let cells = DetachedPointFrontier {
                root: facts.root,
                memory: facts.memory,
                read_id: facts.read_id,
                table_name: core::mem::take(&mut facts.table_name),
                old_pages: facts.old_pages,
                scratch: facts.scratch,
                shape: facts.shape,
                final_physical_upper: facts.final_physical_upper,
            };
            drop(facts);
            cells
        };
        self.epoch.require_current().map_err(E::Epoch)?;
        Ok(NativeOwnedPointFrontierFacts {
            recipe: self.recipe,
            cells,
            epoch: self.epoch,
        })
    }
}
impl NativeOwnedPointFrontierFacts<'_, '_> {
    pub(crate) fn require_current(&self) -> Result<(), E> {
        self.epoch.require_current().map_err(E::Epoch)
    }
    pub(crate) fn scratch_heap_upper(&self) -> u64 {
        self.cells.scratch.heap_bytes
    }
    pub(crate) fn old_pages(&self) -> usize {
        self.cells.old_pages
    }
    pub(crate) fn final_live_physical_upper(&self) -> u64 {
        self.cells.final_physical_upper
    }
    pub(crate) fn maximum_value_bytes(&self) -> usize {
        self.cells.shape.maximum_value
    }
    pub(crate) fn validate_same_current_tree<K: Key, V: Value>(
        &self,
        tree: &Btree<K, V>,
        name: &str,
        actual_recipe: &NativePointRecipe<'_>,
    ) -> Result<(), E> {
        if !self.recipe.same_operations(actual_recipe) {
            return Err(E::UnsupportedState);
        }
        tree.require_point_epoch(&self.epoch)?;
        let parts = tree
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        if self.cells.root != tree.root
            || self.cells.memory != Arc::as_ptr(parts.actual_memory())
            || self.cells.read_id != parts.transaction_id()
            || self.cells.table_name != name.as_bytes()
        {
            return Err(E::UnsupportedState);
        }
        Ok(())
    }
}
impl Drop for NativeOwnedPointFrontierFacts<'_, '_> {
    fn drop(&mut self) {}
}

fn add(a: usize, b: usize) -> Result<usize, E> {
    a.checked_add(b).ok_or(E::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, E> {
    a.checked_mul(b).ok_or(E::Overflow)
}
fn add64(a: u64, b: u64) -> Result<u64, E> {
    a.checked_add(b).ok_or(E::Overflow)
}

impl<K: Key, V: Value> Btree<K, V> {
    fn require_point_epoch(&self, epoch: &NativeResourceEpoch<'_>) -> Result<(), E> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        epoch
            .require_tracker(parts.actual_tracker())
            .map_err(E::Epoch)?;
        self.require_current_native_resource_read()?;
        epoch.require_current().map_err(E::Epoch)
    }
    pub(crate) fn prepare_point_frontier<'tree, 'recipe, 'epoch, 'tracker>(
        &'tree self,
        actual_name: &'tree str,
        recipe: &'recipe NativePointRecipe<'recipe>,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
    ) -> Result<NativePointFrontierPreparation<'tree, 'recipe, 'epoch, 'tracker, K, V>, E> {
        if TypeId::of::<V>() != TypeId::of::<&'static [u8]>()
            || (recipe.string_keys && TypeId::of::<K>() != TypeId::of::<&'static str>())
            || (!recipe.string_keys && TypeId::of::<K>() != TypeId::of::<&'static [u8]>())
        {
            return Err(E::UnsupportedKey);
        }
        // Build/profile and fixed stack quote were admitted before observation.
        let _ = point_frontier_capture_bytes()?;
        self.require_point_epoch(epoch)?;
        let mut work = Work::new(recipe)?;
        self.walk_point_recipe(recipe, &mut work, None)?;
        self.require_point_epoch(epoch)?;
        let visits = work.visited;
        let record_bytes = crate::tree_store::page_store::point_frontier_capacity::<
            Record,
            CaptureControls,
            Scratch,
        >(visits)?
        .checked_add(crate::tree_store::page_store::point_frontier_capacity::<
            u8,
            (),
            (),
        >(actual_name.len())?)
        .ok_or(E::Overflow)?;
        Ok(NativePointFrontierPreparation {
            actual: self,
            actual_name,
            recipe,
            epoch,
            visits,
            record_bytes,
        })
    }
    pub(crate) fn prepare_owned_point_frontier<'tree, 'recipe, 'tracker>(
        &'tree self,
        actual_name: &'tree str,
        recipe: &'recipe NativePointRecipe<'recipe>,
        epoch: NativeResourceEpoch<'tracker>,
    ) -> Result<NativeOwnedPointFrontierPreparation<'tree, 'recipe, 'tracker, K, V>, E> {
        let (visits, record_bytes) = {
            let p = self.prepare_point_frontier(actual_name, recipe, &epoch)?;
            (p.visits, p.record_bytes)
        };
        Ok(NativeOwnedPointFrontierPreparation {
            actual: self,
            actual_name,
            recipe,
            epoch,
            visits,
            record_bytes,
        })
    }
    fn walk_point_recipe(
        &self,
        recipe: &impl PointRecipeView,
        work: &mut Work,
        mut records: Option<&mut Vec<Record>>,
    ) -> Result<(), E> {
        if self.root.is_none() {
            return Ok(());
        }
        for index in 0..recipe.points() {
            let path = self.point_path(recipe.key_at(index)?, work, &mut records)?;
            for target in 1..path.length {
                for right in [false, true] {
                    let mut window = path;
                    for _ in 0..recipe.deletes() {
                        if !self.point_neighbor(&mut window, target, right, work, &mut records)? {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn point_path(
        &self,
        key: &[u8],
        work: &mut Work,
        records: &mut Option<&mut Vec<Record>>,
    ) -> Result<Path, E> {
        let header = self.root.ok_or(E::Malformed)?;
        let mut path = Path::empty();
        let mut node = Node {
            page: header.root,
            checksum: header.checksum,
        };
        for depth in 0..DEPTH {
            path.nodes[depth] = Some(node);
            path.length = depth + 1;
            let parent = if depth == 0 {
                None
            } else {
                Some(path.nodes[depth - 1].ok_or(E::Malformed)?.page)
            };
            let parent_edge = depth.checked_sub(1).map(|d| path.edges[d]);
            let page = self.point_fetch(node, parent, parent_edge, depth, work, records)?;
            if page.memory()[0] == LEAF {
                if work.leaf_depth.is_some_and(|d| d != depth) {
                    return Err(E::Malformed);
                }
                work.leaf_depth = Some(depth);
                return Ok(path);
            }
            let branch = BranchAccessor::new(&page, K::fixed_width());
            let (index, child) = branch.child_for_key::<K>(key);
            path.rightmost &= Some(index) == branch.count_children().checked_sub(1);
            let checksum = branch.child_checksum(index).ok_or(E::Malformed)?;
            path.edges[depth] = index;
            node = Node {
                page: child,
                checksum,
            };
            drop(page);
        }
        Err(E::UnsupportedDepth)
    }
    fn point_neighbor(
        &self,
        path: &mut Path,
        target: usize,
        right: bool,
        work: &mut Work,
        records: &mut Option<&mut Vec<Record>>,
    ) -> Result<bool, E> {
        path.rightmost = false;
        for pivot in (0..target).rev() {
            let node = path.nodes[pivot].ok_or(E::Malformed)?;
            let parent = if pivot == 0 {
                None
            } else {
                Some(path.nodes[pivot - 1].ok_or(E::Malformed)?.page)
            };
            let parent_edge = pivot.checked_sub(1).map(|d| path.edges[d]);
            let page = self.point_fetch(node, parent, parent_edge, pivot, work, records)?;
            if page.memory()[0] != BRANCH {
                return Err(E::Malformed);
            }
            let branch = BranchAccessor::new(&page, K::fixed_width());
            let old = path.edges[pivot];
            let index = if right {
                if old >= branch.count_children() - 1 {
                    continue;
                }
                old + 1
            } else {
                if old == 0 {
                    continue;
                }
                old - 1
            };
            let mut next = Node {
                page: branch.child_page(index).ok_or(E::Malformed)?,
                checksum: branch.child_checksum(index).ok_or(E::Malformed)?,
            };
            path.edges[pivot] = index;
            drop(page);
            for depth in pivot + 1..=target {
                path.nodes[depth] = Some(next);
                let parent = Some(path.nodes[depth - 1].ok_or(E::Malformed)?.page);
                let parent_edge = Some(path.edges[depth - 1]);
                let page = self.point_fetch(next, parent, parent_edge, depth, work, records)?;
                let leaf = page.memory()[0] == LEAF;
                if leaf != (Some(depth) == work.leaf_depth) {
                    return Err(E::Malformed);
                }
                if depth == target {
                    return Ok(true);
                }
                let branch = BranchAccessor::new(&page, K::fixed_width());
                let index = if right {
                    0
                } else {
                    (branch.count_children() - 1)
                };
                path.edges[depth] = index;
                next = Node {
                    page: branch.child_page(index).ok_or(E::Malformed)?,
                    checksum: branch.child_checksum(index).ok_or(E::Malformed)?,
                };
                drop(page);
            }
        }
        Ok(false)
    }
    fn point_fetch(
        &self,
        node: Node,
        parent: Option<PageNumber>,
        parent_edge: Option<usize>,
        depth: usize,
        work: &mut Work,
        records: &mut Option<&mut Vec<Record>>,
    ) -> Result<PageImpl, E> {
        if node.checksum == DEFERRED || depth >= DEPTH {
            return Err(E::UnsupportedState);
        }
        work.before_fetch()?;
        let page = self.mem.get_page(node.page, self.hint)?;
        check_layout::<K, V>(&page)?;
        let branch = page.memory()[0] == BRANCH;
        let mut payload = 0_usize;
        let mut key_bytes = 0_usize;
        let mut maximum_key = 0_usize;
        let mut maximum_value = 0_usize;
        let count;
        if branch {
            if branch_checksum(&page, K::fixed_width())? != node.checksum {
                return Err(E::Malformed);
            }
            let a = BranchAccessor::new(&page, K::fixed_width());
            count = a.count_children() - 1;
            for i in 0..count {
                let key = a.key(i).ok_or(E::Malformed)?;
                if i > 0 && !K::compare(a.key(i - 1).ok_or(E::Malformed)?, key).is_lt() {
                    return Err(E::Malformed);
                }
                key_bytes = add(key_bytes, key.len())?;
                maximum_key = maximum_key.max(key.len());
            }
        } else {
            if leaf_checksum(&page, K::fixed_width(), V::fixed_width())? != node.checksum {
                return Err(E::Malformed);
            }
            let a = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            count = a.num_pairs();
            for i in 0..count {
                let entry = a.entry(i).ok_or(E::Malformed)?;
                if i > 0
                    && !K::compare(a.entry(i - 1).ok_or(E::Malformed)?.key(), entry.key()).is_lt()
                {
                    return Err(E::Malformed);
                }
                key_bytes = add(key_bytes, entry.key().len())?;
                payload = add(payload, add(entry.key().len(), entry.value().len())?)?;
                maximum_key = maximum_key.max(entry.key().len());
                maximum_value = maximum_value.max(entry.value().len());
            }
        }
        let record = Record {
            node,
            parent,
            parent_edge,
            depth,
            physical: page.memory().len(),
            branch,
            count,
            payload,
            key_bytes,
            maximum_key,
            maximum_value,
        };
        if let Some(records) = records.as_deref_mut() {
            if let Some(prior) = records.iter().find(|r| r.node.page == node.page) {
                if prior != &record {
                    return Err(E::Malformed);
                }
            } else {
                if records.len() == records.capacity() {
                    return Err(E::VisitsExceeded);
                }
                records.push(record);
            }
        }
        Ok(page)
    }
}

// Inseparable member of NativeFixedDataCensus, not a standalone recipe/epoch
// proof or an authoritative input. Fields cannot be populated from raw counts.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeFixedPointCells {
    table_name: Vec<u8>,
    root: Option<BtreeHeader>,
    memory: *const crate::tree_store::TransactionalMemory,
    read_id: crate::transaction_tracker::TransactionId,
    old_pages: usize,
    old_physical_bytes: u64,
    scratch: crate::tree_store::btree_base::NativePointScratchUpper,
    final_live_physical_upper: u64,
    checksum_depth: usize,
    maximum_key: usize,
    // Conservative record allowance remains with the retained actual name;
    // unused work is finite and dies when this prepared plan physically dies.
    _records_and_name: crate::tree_store::NativeControlAllocation,
}
#[cfg(not(redb_no_std))]
impl NativeFixedPointCells {
    pub(crate) fn maximum_branch_key(&self) -> usize {
        self.maximum_key
    }

    pub(crate) fn checksum_shape(&self) -> (usize, usize) {
        (self.checksum_depth, self.scratch.branch_children)
    }

    pub(crate) fn old_pages(&self) -> usize {
        self.old_pages
    }
    pub(crate) fn old_physical_bytes(&self) -> u64 {
        self.old_physical_bytes
    }
    pub(crate) fn point_scratch_heap_upper(&self) -> u64 {
        self.scratch.heap_bytes
    }
    pub(crate) fn final_live_physical_upper(&self) -> u64 {
        self.final_live_physical_upper
    }
    pub(crate) fn require_for_current_tree<K: Key, V: Value>(
        &self,
        tree: &Btree<K, V>,
        name: &str,
        epoch: &NativeResourceEpoch<'_>,
    ) -> Result<(), E> {
        tree.require_point_epoch(epoch)?;
        let parts = tree
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        if self.root != tree.root
            || self.memory != Arc::as_ptr(parts.actual_memory())
            || self.read_id != parts.transaction_id()
            || self.table_name != name.as_bytes()
        {
            return Err(E::ForeignOwner);
        }
        Ok(())
    }
}

#[cfg(not(redb_no_std))]
pub(crate) fn freed_point_selection_capacity_bytes<T, Owner>(
    data: usize,
    system: usize,
) -> Result<u64, E> {
    // Exact per-family vector capacities plus actual owner/return overlap.
    crate::tree_store::page_store::point_frontier_capacity::<T, Owner, ()>(data)?
        .checked_add(crate::tree_store::page_store::point_vector_bytes::<T>(
            system,
        )?)
        .ok_or(E::Overflow)
}

// Its sole mint consumes the actual typed, checked census; the outer
// NativePreparedReclamation retains genuine read/epoch/root stamps.
// Original-incarnation records and their same-pool control remain owned here;
// no SYSTEM PageImpl or detached mutation/Source authority survives.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeFreedPointFrontierShape {
    // Original-incarnation structure survives into the inseparable mixed plan.
    // Backing dies before its actual same-pool allocation guard (LAST).
    records: Vec<Record>,
    memory: *const crate::tree_store::TransactionalMemory,
    read_id: crate::transaction_tracker::TransactionId,
    root: Option<BtreeHeader>,
    old_pages: usize,
    old_physical_bytes: u64,
    old_leaves: usize,
    old_branches: usize,
    boundary_children: usize,
    future_leaf_upper: usize,
    future_branch_upper: usize,
    final_live_physical_upper: u64,
    maximum_depth: usize,
    maximum_key: usize,
    maximum_value: usize,
    scratch: crate::tree_store::btree_base::NativePointScratchUpper,
    _records: crate::tree_store::NativeControlAllocation,
}
#[cfg(not(redb_no_std))]
impl NativeFreedPointFrontierShape {
    pub(crate) fn root(&self) -> Option<BtreeHeader> {
        self.root
    }
    pub(crate) fn old_pages(&self) -> usize {
        self.old_pages
    }
    pub(crate) fn old_physical_bytes(&self) -> u64 {
        self.old_physical_bytes
    }
    pub(crate) fn old_leaves(&self) -> usize {
        self.old_leaves
    }
    pub(crate) fn old_branches(&self) -> usize {
        self.old_branches
    }
    pub(crate) fn boundary_children(&self) -> usize {
        self.boundary_children
    }
    pub(crate) fn future_leaf_upper(&self) -> usize {
        self.future_leaf_upper
    }
    pub(crate) fn future_branch_upper(&self) -> usize {
        self.future_branch_upper
    }
    pub(crate) fn final_live_physical_upper(&self) -> u64 {
        self.final_live_physical_upper
    }
    pub(crate) fn maximum_depth(&self) -> usize {
        self.maximum_depth
    }
    pub(crate) fn maximum_key_bytes(&self) -> usize {
        self.maximum_key
    }
    pub(crate) fn maximum_value_bytes(&self) -> usize {
        self.maximum_value
    }
    pub(crate) fn scratch_heap_upper(&self) -> u64 {
        self.scratch.heap_bytes
    }
}

#[cfg(not(redb_no_std))]
pub(crate) struct NativeFreedPointFrontierPreparation<'tree, 'recipe, 'epoch, 'tracker> {
    actual: &'tree Btree<
        crate::transactions::TransactionIdWithPagination,
        crate::transactions::PageList<'static>,
    >,
    recipe: &'recipe crate::transactions::NativeFreedPointRecipe<'recipe, 'epoch, 'tracker>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    visits: usize,
    record_bytes: u64,
    _capture: crate::tree_store::NativeControlAllocation,
}
#[cfg(not(redb_no_std))]
impl NativeFreedPointFrontierPreparation<'_, '_, '_, '_> {
    pub(crate) fn record_capacity_bytes(&self) -> u64 {
        self.record_bytes
    }
    pub(crate) fn collect_shape(self) -> Result<NativeFreedPointFrontierShape, E> {
        self.recipe.require_for(self.actual, self.epoch)?;
        // SAME actual Source read context checks managed serial and charges
        // complete records/name/control overlap BEFORE any Vec allocation.
        let allocation = self.actual.mem.admit_fixed_observation(
            usize::try_from(self.record_bytes).map_err(|_| E::Overflow)?,
        )?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(self.visits)
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        let mut work = Work::new(self.recipe)?;
        self.actual
            .walk_point_recipe(self.recipe, &mut work, Some(&mut records))?;
        if work.visited != self.visits {
            return Err(E::Malformed);
        }
        self.recipe.require_for(self.actual, self.epoch)?;
        let facts = NativePointFrontierFacts::from_records(
            self.actual,
            self.recipe.family().name(),
            self.recipe,
            self.actual.mem.resource_base_page_size(),
            self.epoch,
            &records,
        )?;
        // Own all scalar facts first, then destroy its native name Vec before
        // moving the original Record Vec/guard into the retained phase owner.
        let (
            memory,
            read_id,
            root,
            old_pages,
            old_physical_bytes,
            old_leaves,
            old_branches,
            boundary_children,
            future_leaf_upper,
            future_branch_upper,
            final_live_physical_upper,
            maximum_depth,
            maximum_key,
            maximum_value,
            scratch,
        ) = (
            facts.memory,
            facts.read_id,
            facts.root,
            facts.old_pages,
            facts.old_bytes,
            facts.leaves,
            facts.branches,
            facts.boundary_children,
            facts.future_leaves,
            facts.future_branches,
            facts.final_physical_upper,
            facts.maximum_depth,
            facts.shape.maximum_key,
            facts.shape.maximum_value,
            facts.scratch,
        );
        drop(facts); // actual temporary native name BEFORE same-pool guard refund.
        let shape = NativeFreedPointFrontierShape {
            records,
            memory,
            read_id,
            root,
            old_pages,
            old_physical_bytes,
            old_leaves,
            old_branches,
            boundary_children,
            future_leaf_upper,
            future_branch_upper,
            final_live_physical_upper,
            maximum_depth,
            maximum_key,
            maximum_value,
            scratch,
            _records: allocation,
        };
        self.recipe.require_for(self.actual, self.epoch)?;
        Ok(shape)
    }
}
#[cfg(not(redb_no_std))]
impl
    Btree<crate::transactions::TransactionIdWithPagination, crate::transactions::PageList<'static>>
{
    // Closed typed SYSTEM observation: preserve the existing owning Btree
    // read/actual-tracker/epoch checks without exposing its private read parts.
    pub(crate) fn require_freed_point_epoch(
        &self,
        epoch: &NativeResourceEpoch<'_>,
    ) -> Result<(), E> {
        self.require_point_epoch(epoch)
    }

    pub(crate) fn prepare_freed_point_frontier<'tree, 'recipe, 'epoch, 'tracker>(
        &'tree self,
        recipe: &'recipe crate::transactions::NativeFreedPointRecipe<'recipe, 'epoch, 'tracker>,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
    ) -> Result<NativeFreedPointFrontierPreparation<'tree, 'recipe, 'epoch, 'tracker>, E> {
        recipe.require_for(self, epoch)?;
        // No general caller key/value class or independently supplied name.
        let capture_bytes = crate::tree_store::page_store::point_frontier_capacity::<
            Record,
            NativeFreedPointFrontierPreparation<'_, '_, '_, '_>,
            Scratch,
        >(0)?;
        let capture = self
            .mem
            .admit_fixed_observation(usize::try_from(capture_bytes).map_err(|_| E::Overflow)?)?;
        let mut work = Work::new(recipe)?;
        self.walk_point_recipe(recipe, &mut work, None)?;
        recipe.require_for(self, epoch)?;
        let record_bytes = crate::tree_store::page_store::point_frontier_capacity::<
            Record,
            NativeFreedPointFrontierPreparation<'_, '_, '_, '_>,
            Scratch,
        >(work.visited)?
        .checked_add(crate::tree_store::page_store::point_vector_bytes::<u8>(
            recipe.family().name().len(),
        )?)
        .and_then(|n| {
            n.checked_add(
                u64::try_from(size_of::<NativeFreedPointFrontierShape>())
                    .ok()?
                    .checked_mul(2)?,
            )
        })
        .ok_or(E::Overflow)?;
        Ok(NativeFreedPointFrontierPreparation {
            actual: self,
            recipe,
            epoch,
            visits: work.visited,
            record_bytes,
            _capture: capture,
        })
    }
}

// Specialized INTERNAL built-in namespace entry. The ordinary compiler's
// byte-value and unique-key predicates above remain unchanged.
#[cfg(not(redb_no_std))]
impl Btree<&'static str, crate::tree_store::InternalTableDefinition> {
    pub(crate) fn require_normal_namespace_epoch(
        &self,
        epoch: &NativeResourceEpoch<'_>,
    ) -> Result<(), E> {
        self.require_point_epoch(epoch)
    }
    pub(crate) fn fixed_namespace_affinity(
        &self,
    ) -> Result<crate::tree_store::NativeControlAllocation, E> {
        Ok(self.mem.admit_fixed_observation(0)?)
    }
    pub(crate) fn collect_normal_namespace_cells(
        &self,
        recipe: &crate::tree_store::NativeNormalNamespaceRecipe<'_, '_, '_, '_>,
        epoch: &NativeResourceEpoch<'_>,
    ) -> Result<NativeFixedPointCells, E> {
        recipe.require_for(self, epoch)?;
        let capture = self.mem.admit_fixed_observation(
            usize::try_from(point_frontier_capture_bytes()?).map_err(|_| E::Overflow)?,
        )?;
        let mut first = Work::new(recipe)?;
        self.walk_point_recipe(recipe, &mut first, None)?;
        recipe.require_for(self, epoch)?;
        let name = crate::tree_store::NORMAL_NAMESPACE_OBSERVATION;
        let bound = crate::tree_store::page_store::point_frontier_capacity::<
            Record,
            CaptureControls,
            Scratch,
        >(first.visited)?
        .checked_add(crate::tree_store::page_store::point_vector_bytes::<u8>(
            name.len(),
        )?)
        .ok_or(E::Overflow)?;
        let allocation = self
            .mem
            .admit_fixed_observation(usize::try_from(bound).map_err(|_| E::Overflow)?)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(first.visited)
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        let mut observed = Work::new(recipe)?;
        self.walk_point_recipe(recipe, &mut observed, Some(&mut records))?;
        if observed.visited != first.visited {
            return Err(E::Malformed);
        }
        recipe.require_for(self, epoch)?;
        let mut facts = NativePointFrontierFacts::from_records(
            self,
            name,
            recipe,
            self.mem.resource_base_page_size(),
            epoch,
            &records,
        )?;
        facts.validate_same_current_tree(self, name)?;
        let cells = NativeFixedPointCells {
            table_name: core::mem::take(&mut facts.table_name),
            root: facts.root,
            memory: facts.memory,
            read_id: facts.read_id,
            old_pages: facts.old_pages,
            old_physical_bytes: facts.old_bytes,
            scratch: facts.scratch,
            maximum_key: facts.shape.maximum_key,
            final_live_physical_upper: facts.final_physical_upper,
            checksum_depth: facts
                .maximum_depth
                .checked_add(facts.shape.incoming_pairs)
                .ok_or(E::Overflow)?
                .max(1),
            _records_and_name: allocation,
        };
        drop(facts);
        drop(records);
        recipe.require_for(self, epoch)?;
        drop(capture);
        Ok(cells)
    }
}

// Closed typed interval observation only; the ordinary recipe compiler and its
// supported operation classes are unchanged.
#[cfg(not(redb_no_std))]
#[path = "resource_freed_append_gap.rs"]
mod freed_append_gap;
#[cfg(not(redb_no_std))]
pub(crate) use freed_append_gap::{NativeFreedAppendGapPreparation, NativeFreedAppendGapShape};

// This owner is constructor-private below the actual native frontier parser.
// No raw record/PN vector or root/guard pair escapes to a caller.
#[cfg(not(redb_no_std))]
#[path = "resource_fixed_system_phase.rs"]
mod fixed_system_phase;
#[cfg(not(redb_no_std))]
pub(crate) use fixed_system_phase::{NativeFreedPhaseForest, NativeFreedPhaseGeometry};
