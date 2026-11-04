//! Original-incarnation closure for the inseparable fixed SYSTEM phase plan.
//! The enclosing prepared owner still revalidates original epoch/root/serial.
//! These facts alone do not certify a mixed physical peak or authorize mutation.
use super::*;
use crate::tree_store::{NativeFreedAppendGapShape, NativePageResources};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct NativeFreedPhaseForest {
    original_nodes: usize,
    original_bytes: u64,
    leaves: usize,
    boundary_children: usize,
    pairs: usize,
    payload: usize,
    maximum_value: usize,
    maximum_physical: usize,
    maximum_depth: usize,
}
impl NativeFreedPhaseForest {
    // Numerical only: actual parsed owners are inseparable from the existing
    // complete native binding. No raw counts, record constructor or fallback.
    pub(crate) fn from_original_owners(
        selected: Option<&NativeFreedPointFrontierShape>,
        append: Option<&NativeFreedAppendGapShape>,
        resources: &NativePageResources,
    ) -> Result<Self, E> {
        if let Some(selected) = selected {
            if !selected._records.belongs_to(resources) {
                return Err(E::ForeignOwner);
            }
        }
        if let Some(append) = append {
            append.require_phase_pool(resources)?;
        }
        if let (Some(selected), Some(append)) = (selected, append) {
            if (selected.root, selected.memory, selected.read_id) != append.phase_binding() {
                return Err(E::ForeignOwner);
            }
        }
        let left = selected.map_or(&[][..], |owner| owner.records.as_slice());
        let right = append.map_or(&[][..], NativeFreedAppendGapShape::phase_records);
        let root = selected
            .map(|owner| owner.root)
            .or_else(|| append.map(|owner| owner.phase_binding().0))
            .flatten();
        let unique = || {
            left.iter().chain(
                right
                    .iter()
                    .filter(|record| !left.iter().any(|prior| prior.node.page == record.node.page)),
            )
        };
        // Every original page has exactly one original edge/incarnation. A
        // repeated actual path may dedup; a different checksum/edge never does.
        for record in right {
            if let Some(prior) = left
                .iter()
                .find(|prior| prior.node.page == record.node.page)
            {
                if prior != record {
                    return Err(E::Malformed);
                }
            }
        }
        let mut result = Self {
            original_nodes: 0,
            original_bytes: 0,
            leaves: 0,
            boundary_children: 0,
            pairs: 0,
            payload: 0,
            maximum_value: 0,
            maximum_physical: 0,
            maximum_depth: 0,
        };
        let mut children = 0;
        for record in unique() {
            result.original_nodes = add(result.original_nodes, 1)?;
            result.original_bytes = add64(result.original_bytes, record.physical as u64)?;
            result.maximum_physical = result.maximum_physical.max(record.physical);
            result.maximum_depth = result.maximum_depth.max(add(record.depth, 1)?);
            result.maximum_value = result.maximum_value.max(record.maximum_value);
            match record.parent {
                None if root.is_some_and(|root| root.root == record.node.page)
                    && record.depth == 0
                    && record.parent_edge.is_none() => {}
                Some(parent) => {
                    let ancestor = unique()
                        .find(|prior| prior.node.page == parent)
                        .ok_or(E::Malformed)?;
                    if !ancestor.branch
                        || ancestor.depth.checked_add(1) != Some(record.depth)
                        || !record
                            .parent_edge
                            .is_some_and(|edge| edge <= ancestor.count)
                    {
                        return Err(E::Malformed);
                    }
                }
                _ => return Err(E::Malformed),
            }
            if record.branch {
                // Record.count is KEYS, never child count.
                children = add(children, add(record.count, 1)?)?;
            } else {
                result.leaves = add(result.leaves, 1)?;
                result.pairs = add(result.pairs, record.count)?;
                result.payload = add(result.payload, record.payload)?;
            }
        }
        if root.is_some() != (result.original_nodes != 0) {
            return Err(E::Malformed);
        }
        result.boundary_children = if result.original_nodes == 0 {
            0
        } else {
            children
                .checked_sub(result.original_nodes - 1)
                .ok_or(E::Malformed)?
        };
        Ok(result)
    }
    // Entire union evolves together. No independent DeleteOnly/PutOnly max or
    // sum is used. The ordered prefix/right-end closure bounds original nodes.
    // Exact-key RangeMut extraction can repack an oversized multi-pair leaf
    // into MORE leaves. Final live materialized leaves are instead bounded by
    // retained captured pairs plus incoming rows: each leaf is nonempty and
    // packing/deletion never duplicates a pair. Original unopened children
    // remain opaque terminals under the same closed K-window proof.
    pub(crate) fn evolve_right_appends(
        &self,
        rows: usize,
        value_bytes: usize,
        base: usize,
    ) -> Result<NativeFreedPhaseGeometry, E> {
        if base == 0 || !base.is_power_of_two() {
            return Err(E::UnsupportedState);
        }
        let key = <crate::transactions::TransactionIdWithPagination as crate::Value>::fixed_width()
            .ok_or(E::UnsupportedState)?;
        let pairs = add(self.pairs, rows)?;
        let leaves = pairs;
        let terminals = add(leaves, self.boundary_children)?;
        let branches = terminals.saturating_sub(1);
        let payload = add(self.payload, mul(rows, add(key, value_bytes)?)?)?;
        let leaf_header =
            crate::tree_store::btree_base::RawLeafBuilder::required_bytes(0, 0, Some(key), None);
        let leaf_offset =
            crate::tree_store::btree_base::RawLeafBuilder::required_bytes(1, 0, Some(key), None)
                - leaf_header;
        let branch_header =
            crate::tree_store::btree_base::RawBranchBuilder::required_bytes(0, 0, Some(key));
        let branch_key =
            crate::tree_store::btree_base::RawBranchBuilder::required_bytes(1, key, Some(key))
                - branch_header;
        // In the abstract forest, boundary child roots are opaque terminals.
        // Every retained branch has >=2 children, so total branch KEYS <=T-1.
        let leaf_required = add(
            add(mul(leaves, leaf_header)?, mul(pairs, leaf_offset)?)?,
            payload,
        )?;
        let branch_required = add(
            mul(branches, branch_header)?,
            mul(terminals.saturating_sub(1), branch_key)?,
        )?;
        let nodes = add(leaves, branches)?;
        let required = add(leaf_required, branch_required)?;
        // Native base*next_power_of_two(ceil(required/base)) rounding, summed
        // over nodes. This is LIVE geometry, never a physical-peak theorem.
        let physical = add(mul(required, 2)?, mul(base, nodes)?)?;
        let scratch = crate::tree_store::btree_base::point_mutator_scratch_upper(
            pairs,
            terminals,
            mul(terminals.saturating_sub(1), key)?,
            key,
            self.maximum_value.max(value_bytes),
        )?;
        Ok(NativeFreedPhaseGeometry {
            nodes,
            leaves,
            branches,
            physical: physical as u64,
            heap_scratch: scratch.heap_bytes,
            original_bytes: self.original_bytes,
            maximum_original_physical: self.maximum_physical,
            pairs,
            payload,
            children: terminals,
            maximum_depth: self
                .maximum_depth
                .checked_add(rows)
                .ok_or(E::Overflow)?
                .max(1),
        })
    }
    #[cfg(test)]
    pub(crate) fn maximum_original_depth_for_fixture(&self) -> usize {
        self.maximum_depth
    }
    pub(crate) fn original_nodes(&self) -> usize {
        self.original_nodes
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct NativeFreedPhaseGeometry {
    pairs: usize,
    payload: usize,
    children: usize,
    maximum_depth: usize,
    nodes: usize,
    leaves: usize,
    branches: usize,
    physical: u64,
    heap_scratch: u64,
    original_bytes: u64,
    maximum_original_physical: usize,
}
impl NativeFreedPhaseGeometry {
    pub(crate) fn execution_buffers_shape(&self) -> (usize, usize, usize, usize) {
        (self.maximum_depth, self.children, self.pairs, self.payload)
    }

    pub(crate) fn nodes(&self) -> usize {
        self.nodes
    }
}
