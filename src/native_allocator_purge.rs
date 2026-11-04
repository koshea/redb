//! Actual SYSTEM-owned allocator purge census. Numerical evidence only.
use super::*;
use crate::tree_store::{BtreeHeader, NativeAllocatorPurgeFacts, NativeAllocatorPurgeWork};

pub(crate) struct NativeAllocatorPurgeSeed<'system, 'read, 'epoch, 'tracker> {
    actual: &'system NativeSystemRead<'read, 'epoch, 'tracker>,
    present: bool,
    root: Option<BtreeHeader>,
    work: NativeAllocatorPurgeWork,
    definition_path_visits_upper: usize,
    definition_path_bytes_upper: u64,
}
pub(crate) struct NativeAllocatorPurgeObservation<'system, 'read, 'epoch, 'tracker> {
    actual: &'system NativeSystemRead<'read, 'epoch, 'tracker>,
    present: bool,
    root: Option<BtreeHeader>,
    facts: NativeAllocatorPurgeFacts,
    definition_path_visits_upper: usize,
    definition_path_bytes_upper: u64,
}
impl<'read, 'epoch, 'tracker> NativeSystemRead<'read, 'epoch, 'tracker> {
    pub(crate) fn allocator_purge_seed<'system>(
        &'system self,
    ) -> PathResult<NativeAllocatorPurgeSeed<'system, 'read, 'epoch, 'tracker>> {
        self.require_current()?;
        let keys = [crate::tree_store::NativePreparedNormalKey::Str(
            ALLOCATOR_STATE_TABLE_NAME,
        )];
        let namespace = self
            .tree
            .inspect_prepared_resource_definitions(&keys, &self.epoch)?;
        // One checked inspection + two identical native typed lookups under
        // the same actual exclusion lease. Initial SYSTEM capture is external.
        let definition_path_visits_upper = namespace
            .visited()
            .checked_mul(3)
            .ok_or(NativeKeyPathError::Overflow)?;
        let definition_path_bytes_upper = namespace
            .observed_page_bytes()
            .checked_mul(3)
            .ok_or(NativeKeyPathError::Overflow)?;
        drop(namespace);
        // The fixed definition is parsed by the normal native typed reader,
        // through this inspector's admitted resolver and exclusive lease.
        let definition = self
            .tree
            .get_table::<AllocatorStateKey, &'static [u8]>(
                ALLOCATOR_STATE_TABLE_NAME,
                TableType::Normal,
            )
            .map_err(|error| {
                NativeKeyPathError::Native(error.into_storage_error_or_corrupted(
                    "Native allocator resource definition mismatch",
                ))
            })?;
        let (present, root) = match definition {
            Some(InternalTableDefinition::Normal { table_root, .. }) => (true, table_root),
            None => (false, None),
            Some(InternalTableDefinition::Multimap { .. }) => {
                return Err(NativeKeyPathError::Malformed);
            }
        };
        if root.is_some_and(|header| header.length == 0) {
            return Err(NativeKeyPathError::Malformed);
        }
        let work = NativeAllocatorPurgeWork::from_actual_header(
            root.map_or(0, |header| header.length),
            u64::try_from(self.read.mem.get_page_size())
                .map_err(|_| NativeKeyPathError::Overflow)?
                .checked_mul(1_u64 << 31)
                .ok_or(NativeKeyPathError::Overflow)?,
        )?;
        self.require_current()?;
        Ok(NativeAllocatorPurgeSeed {
            actual: self,
            present,
            root,
            work,
            definition_path_visits_upper,
            definition_path_bytes_upper,
        })
    }
}
impl<'system, 'read, 'epoch, 'tracker> NativeAllocatorPurgeSeed<'system, 'read, 'epoch, 'tracker> {
    // Actual owning numeric root size, not a decoded/independently paired root.
    // This performs no page fetch or native allocation.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn root_page_bytes_for_test(&self) -> PathResult<Option<usize>> {
        self.actual.require_current()?;
        self.root
            .map(|root| {
                self.actual
                    .read
                    .mem
                    .get_page_size()
                    .checked_shl(u32::from(root.root.page_order))
                    .ok_or(NativeKeyPathError::Overflow)
            })
            .transpose()
    }

    pub(crate) fn inspect_complete(
        mut self,
    ) -> PathResult<NativeAllocatorPurgeObservation<'system, 'read, 'epoch, 'tracker>> {
        self.actual.require_current()?;
        // Consume numerical work BEFORE open_normal caches the allocator root.
        if let Some(header) = self.root {
            self.work
                .before_fetch(header.root, self.actual.read.mem.get_page_size())?;
        }
        let facts = {
            let tree = self
                .actual
                .open_normal::<AllocatorStateKey, &'static [u8]>(ALLOCATOR_STATE_TABLE_NAME)?;
            if tree.is_some() != self.present
                || tree.as_ref().and_then(Btree::get_root) != self.root
            {
                return Err(NativeKeyPathError::Malformed);
            }
            match tree {
                Some(tree) => {
                    tree.inspect_complete_allocator_purge(&self.actual.epoch, &mut self.work)?
                }
                None => NativeAllocatorPurgeFacts::default(),
            }
        }; // Every temporary allocator Btree, cached root and page is destroyed.
        self.actual.require_current()?;
        Ok(NativeAllocatorPurgeObservation {
            actual: self.actual,
            present: self.present,
            root: self.root,
            facts,
            definition_path_visits_upper: self.definition_path_visits_upper,
            definition_path_bytes_upper: self.definition_path_bytes_upper,
        })
    }
}
// Combined numerical preparation, never a complete write/admission factory.
// The only constructor below consumes the actual SYSTEM inspector after both
// observations; no detached census facts or independently paired roots mint it.
pub(crate) struct NativePreparedSystemResources<'read, 'epoch, 'tracker> {
    reclamation: NativePreparedReclamation<'read, 'epoch, 'tracker>,
    purge: NativeAllocatorPurgeFacts,
    allocator_present: bool,
    allocator_root: Option<BtreeHeader>,
    definition_path_upper: (usize, u64),
}
impl NativePreparedSystemResources<'_, '_, '_> {
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn detachment_state_for_test(
        &self,
    ) -> (
        Option<crate::transaction_tracker::TransactionId>,
        (usize, usize, usize, usize),
        usize,
        bool,
    ) {
        self.reclamation.detachment_state_for_test()
    }
    pub(crate) fn require_before_write(&self) -> PathResult<()> {
        self.reclamation.require_before_write()
    }
    pub(crate) fn purge_facts(&self) -> &NativeAllocatorPurgeFacts {
        &self.purge
    }
    pub(crate) fn allocator_state_present(&self) -> bool {
        self.allocator_present
    }
    pub(crate) fn allocator_root(&self) -> Option<BtreeHeader> {
        self.allocator_root
    }
    pub(crate) fn definition_path_upper(&self) -> (usize, u64) {
        self.definition_path_upper
    }
    pub(crate) fn selected_prefix(&self) -> PathResult<&super::NativeSelectedFreedPrefix> {
        self.reclamation.selected_prefix()
    }
    pub(crate) fn selected_records(&self) -> usize {
        self.reclamation.selected_records()
    }
}
#[cfg(not(redb_no_std))]
pub(super) struct NativeDetachedSystemResources<'work> {
    reclamation: super::NativeDetachedReclamation<'work>,
    purge: NativeAllocatorPurgeFacts,
    allocator_present: bool,
    allocator_root: Option<BtreeHeader>,
    definition_path_upper: (usize, u64),
}
#[cfg(not(redb_no_std))]
impl NativeDetachedSystemResources<'_> {
    pub(super) fn phase_forests(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> PathResult<[crate::tree_store::NativeFreedPhaseForest; 3]> {
        self.reclamation.phase_forests(resources)
    }

    pub(super) fn take_for_fixed_writer(
        &mut self,
        claim: &crate::db::NativeFixedWriterClaim<'_>,
    ) -> PathResult<ReclamationBudget> {
        self.reclamation.take_for_fixed_writer(claim)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn detachment_state_for_test(
        &self,
    ) -> (
        Option<crate::transaction_tracker::TransactionId>,
        (usize, usize, usize, usize),
        usize,
        bool,
    ) {
        self.reclamation.detachment_state_for_test()
    }
    pub(super) fn require_current(&self) -> PathResult<()> {
        self.reclamation.require_current()
    }
    pub(super) fn selected_prefix(&self) -> PathResult<&super::NativeSelectedFreedPrefix> {
        self.reclamation.selected_prefix()
    }
    pub(super) fn append_positions(&self) -> PathResult<&super::NativeFreedAppendPositions> {
        self.reclamation.append_positions()
    }
    pub(super) fn purge_facts(&self) -> &NativeAllocatorPurgeFacts {
        &self.purge
    }
}
#[cfg(not(redb_no_std))]
impl<'read, 'epoch, 'work> NativePreparedSystemResources<'read, 'epoch, 'work> {
    pub(super) fn reclamation_resolver(&self) -> crate::tree_store::PageResolver {
        self.reclamation.read.tree.read_page_resolver()
    }
    pub(super) fn detach_parts(
        &mut self,
        actual: &'work crate::Database,
        original: &crate::transaction_tracker::NativeResourceEpoch<'work>,
    ) -> PathResult<NativeDetachedSystemResources<'work>> {
        let reclamation = self.reclamation.detach_parts(actual, original)?;
        Ok(NativeDetachedSystemResources {
            reclamation,
            purge: core::mem::take(&mut self.purge),
            allocator_present: self.allocator_present,
            allocator_root: self.allocator_root,
            definition_path_upper: self.definition_path_upper,
        })
    }
}

impl Drop for NativePreparedSystemResources<'_, '_, '_> {
    fn drop(&mut self) {}
}

// Closed owning choice only: existing ordinary preparation remains unchanged.
enum NativeSystemPreparationMode {
    Existing,
    #[cfg(not(redb_no_std))]
    WithFreedFrontiers,
}
impl<'read, 'epoch, 'tracker> NativeSystemRead<'read, 'epoch, 'tracker> {
    pub(crate) fn prepare_system_resources(
        self,
        budget: &mut ReclamationBudget,
    ) -> PathResult<NativePreparedSystemResources<'read, 'epoch, 'tracker>> {
        self.prepare_system_resources_impl(budget, NativeSystemPreparationMode::Existing)
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_system_resources_with_freed_frontiers(
        self,
        budget: &mut ReclamationBudget,
    ) -> PathResult<NativePreparedSystemResources<'read, 'epoch, 'tracker>> {
        self.prepare_system_resources_impl(budget, NativeSystemPreparationMode::WithFreedFrontiers)
    }
    fn prepare_system_resources_impl(
        self,
        budget: &mut ReclamationBudget,
        mode: NativeSystemPreparationMode,
    ) -> PathResult<NativePreparedSystemResources<'read, 'epoch, 'tracker>> {
        let (purge, allocator_present, allocator_root, definition_path_upper) = {
            let mut observed = self.allocator_purge_seed()?.inspect_complete()?;
            observed.require_current()?;
            // Private owning-family transfer only; the observation remains
            // borrowed until its physical destruction at this scope boundary.
            // These numeric facts have no external completion constructor.
            (
                core::mem::take(&mut observed.facts),
                observed.present,
                observed.root,
                (
                    observed.definition_path_visits_upper,
                    observed.definition_path_bytes_upper,
                ),
            )
        };
        // The EXISTING single budget seals both freed families/tails. Its
        // consuming path destroys all SYSTEM Btrees/PageImpl before lease
        // release, then revalidates genuine original Read/Epoch/root stamps.
        let reclamation = match mode {
            NativeSystemPreparationMode::Existing => self.prepare_reclamation(budget)?,
            #[cfg(not(redb_no_std))]
            NativeSystemPreparationMode::WithFreedFrontiers => {
                self.prepare_reclamation_with_freed_frontiers(budget)?
            }
        };
        let result = NativePreparedSystemResources {
            reclamation,
            purge,
            allocator_present,
            allocator_root,
            definition_path_upper,
        };
        result.require_before_write()?;
        Ok(result)
    }
}

impl NativeAllocatorPurgeObservation<'_, '_, '_, '_> {
    pub(crate) fn require_current(&self) -> PathResult<()> {
        self.actual.require_current()
    }
    pub(crate) fn present(&self) -> bool {
        self.present
    }
    pub(crate) fn root(&self) -> Option<BtreeHeader> {
        self.root
    }
    pub(crate) fn definition_path_upper(&self) -> (usize, u64) {
        (
            self.definition_path_visits_upper,
            self.definition_path_bytes_upper,
        )
    }
    pub(crate) fn facts(&self) -> &NativeAllocatorPurgeFacts {
        &self.facts
    }
}
impl Drop for NativeAllocatorPurgeSeed<'_, '_, '_, '_> {
    fn drop(&mut self) {}
}
impl Drop for NativeAllocatorPurgeObservation<'_, '_, '_, '_> {
    fn drop(&mut self) {}
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_allocator_purge_tests.rs"]
mod tests;
