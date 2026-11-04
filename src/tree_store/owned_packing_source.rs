//! The one selected sparse run's replacement containers, prepared offgate.
use crate::Result;
use crate::tree_store::fixed_scratch::{PreparedVec, ScratchVec};
use crate::tree_store::page_store::{NativeNumericalEpoch, NativePageResources};
use crate::tree_store::{PageNumber, PageResolver};
use core::ops::Range;

pub(in crate::tree_store) struct FixedPackedLeaf {
    pub(super) page: PageNumber,
    pub(super) upper: [u8; 16],
}
#[derive(Clone)]
pub(crate) struct NativeFixedPackingPool {
    plan: PreparedVec<Range<usize>>,
    leaves: PreparedVec<FixedPackedLeaf>,
    removed_leaves: PreparedVec<PageNumber>,
}
impl NativeFixedPackingPool {
    pub(crate) fn capacity_bytes(pairs: usize) -> Result<usize> {
        PreparedVec::<Range<usize>>::capacity_bytes(pairs)?
            .checked_add(PreparedVec::<FixedPackedLeaf>::capacity_bytes(pairs)?)
            .and_then(|n| n.checked_add(PreparedVec::<PageNumber>::capacity_bytes(2).ok()?))
            .ok_or_else(crate::tree_store::source_owner_refused)
    }
    pub(crate) fn prepare(
        resolver: &PageResolver,
        numerical: &NativeNumericalEpoch,
        pairs: usize,
    ) -> Result<Self> {
        let result = Self {
            plan: PreparedVec::prepare(resolver, numerical, pairs)?,
            leaves: PreparedVec::prepare(resolver, numerical, pairs)?,
            // Exactly one selected leaf plus at most one sparse neighbour.
            // Ancestor merge siblings have separate native builder ownership.
            removed_leaves: PreparedVec::prepare(resolver, numerical, 2)?,
        };
        result.require_available(numerical.resources())?;
        Ok(result)
    }
    pub(crate) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
        self.plan.require_available(resources)?;
        self.leaves.require_available(resources)?;
        self.removed_leaves.require_available(resources)
    }
    pub(in crate::tree_store) fn plan(
        &self,
        resources: &NativePageResources,
    ) -> Result<ScratchVec<Range<usize>>> {
        self.plan.checkout(resources)
    }
    pub(in crate::tree_store) fn leaves(
        &self,
        resources: &NativePageResources,
    ) -> Result<ScratchVec<FixedPackedLeaf>> {
        self.leaves.checkout(resources)
    }
    pub(in crate::tree_store) fn removed(
        &self,
        resources: &NativePageResources,
    ) -> Result<ScratchVec<PageNumber>> {
        self.removed_leaves.checkout(resources)
    }
}
