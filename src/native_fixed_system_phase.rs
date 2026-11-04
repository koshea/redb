//! Closed fixed SYSTEM phase geometry, constructed only in the actual native
//! combined owner. This is NOT a whole resource/physical peak plan.
use super::*;
use crate::transactions::{NativeFixedCommitProfile, PageList};
use crate::tree_store::{NativeFreedPhaseForest, NativeFreedPhaseGeometry};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct NativeFixedSystemPhasePlan {
    original: [NativeFreedPhaseForest; 3],
    main: [NativeFreedPhaseGeometry; 3],
    post: [NativeFreedPhaseGeometry; 3],
    original_namespace_bytes: usize,
    base: usize,
    main_freed_upper: usize,
    post_freed_upper: usize,
    post_rows_upper: usize,
    selected_system_records: usize,
    purge_physical_bytes: u64,
}
impl NativeFixedSystemPhasePlan {
    pub(super) fn capture(
        data: &NativeFixedDataCensus<'_>,
        emission: &NativeFixedDataEmission,
        system: &NativeDetachedProfiledSystemResources<'_>,
        numerical: &NativeNumericalEpoch,
    ) -> R<Self> {
        emission.require_for(data, system, numerical)?;
        let original = system.phase_forests(numerical.resources())?;
        let namespace = system.namespace_facts()?;
        let base = namespace.base_page_bytes();
        if namespace.prospective_union_bytes() > base {
            return Err(E::UnsupportedState);
        }
        let rows = emission.append_rows_upper();
        let main = [
            original[0].evolve_right_appends(rows, PageList::allocated_capacity_bytes(), base)?,
            original[1].evolve_right_appends(0, PageList::system_freed_capacity_bytes(), base)?,
            original[2].evolve_right_appends(0, PageList::allocated_capacity_bytes(), base)?,
        ];
        let add = |a: usize, b: usize| a.checked_add(b).ok_or(E::Overflow);
        // Whole original SYS mutation union, including one checked single-leaf
        // namespace. Purge is the actual complete allocator tree census.
        let old_nodes = original
            .iter()
            .try_fold(1usize, |n, f| add(n, f.original_nodes()))?;
        let purge = system.purge_facts()?;
        let main_freed_upper = add(
            old_nodes,
            usize::try_from(purge.nodes).map_err(|_| E::Overflow)?,
        )?;
        let main_nodes = main.iter().try_fold(1usize, |n, f| add(n, f.nodes()))?;
        // All main freed SYS predecessors are drained AFTER main publication,
        // BEFORE post begins. The initial post queue is zero on checked profile.
        // Post may retire original or main-committed incarnations from this
        // closure once each. Post-created successors are uncommitted and freed
        // directly: append does not recursively create new committed debt.
        let post_freed_upper = add(old_nodes, main_nodes)?;
        let post_rows_upper = post_freed_upper.div_ceil(PageList::system_freed_capacity_count());
        let post = [
            original[0].evolve_right_appends(rows, PageList::allocated_capacity_bytes(), base)?,
            original[1].evolve_right_appends(
                post_rows_upper,
                PageList::system_freed_capacity_bytes(),
                base,
            )?,
            original[2].evolve_right_appends(0, PageList::allocated_capacity_bytes(), base)?,
        ];
        emission.require_for(data, system, numerical)?;
        Ok(Self {
            original,
            main,
            post,
            original_namespace_bytes: namespace.actual_page_bytes(),
            base,
            main_freed_upper,
            post_freed_upper,
            post_rows_upper,
            purge_physical_bytes: purge.page_bytes,
            selected_system_records: system.selected_prefix()?.system().records(),
        })
    }
    pub(super) fn execution_buffers_shape(&self) -> (usize, usize, usize, usize) {
        // The single exact-key extraction owns at most its selected leaf and
        // one absorbed neighbour. Both are within this captured/evolved union.
        // Whole union pairs/payload therefore bound its actual sparse buffers;
        // original opaque children stay unopened under the K-window proof.
        self.main
            .iter()
            .chain(self.post.iter())
            .fold((1, 0, 0, 0), |(d, c, p, b), f| {
                let (fd, fc, fp, fb) = f.execution_buffers_shape();
                (d.max(fd), c.max(fc), p.max(fp), b.max(fb))
            })
    }
    pub(super) fn require_for(
        &self,
        data: &NativeFixedDataCensus<'_>,
        emission: &NativeFixedDataEmission,
        system: &NativeDetachedProfiledSystemResources<'_>,
        numerical: &NativeNumericalEpoch,
    ) -> R<()> {
        // Same original owner is revalidated BEFORE deriving/comparing any
        // geometry; this result is not a detached stamp or mutation grant.
        system.require_current()?;
        if *self != Self::capture(data, emission, system, numerical)? {
            return Err(E::ForeignOwner);
        }
        Ok(())
    }
    pub(super) fn require_for_writer(
        &self,
        emission: &NativeFixedDataEmission,
        profile: &NativeFixedCommitProfile<'_>,
    ) -> R<()> {
        emission.require_for_writer(profile)?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn post_freed_upper_for_fixture(&self) -> usize {
        self.post_freed_upper
    }
    #[cfg(test)]
    pub(super) fn original_system_depth_for_fixture(&self) -> usize {
        self.original[1].maximum_original_depth_for_fixture()
    }
    #[cfg(test)]
    pub(super) fn selected_system_records_for_fixture(&self) -> usize {
        self.selected_system_records
    }
    #[cfg(test)]
    pub(super) fn post_rows_upper_for_fixture(&self) -> usize {
        self.post_rows_upper
    }
    #[cfg(test)]
    pub(super) fn namespace_incarnation_bytes_for_fixture(&self) -> R<usize> {
        // Three physical versions, even if native PN reuse occurs. Additional
        // copied-read/stack aliases are separate whole-factory obligations.
        self.original_namespace_bytes
            .checked_add(self.base.checked_mul(2).ok_or(E::Overflow)?)
            .ok_or(E::Overflow)
    }
    #[cfg(test)]
    pub(super) fn legacy_original_namespace_only_for_fixture(&self) -> usize {
        self.original_namespace_bytes
    }
    pub(super) fn freed_capacity_upper(&self) -> usize {
        self.main_freed_upper.max(self.post_freed_upper)
    }
}
