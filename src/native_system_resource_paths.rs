//! Native current SYSTEM observations. Complete pre-read admission is external;
//! these owners grant neither mutation nor Source publication authority.
use super::bounded_reclamation::{
    NativeReclamationFence, NativeReclamationTail, ReclamationBudget, ReclamationLimits,
};
use super::{
    ALLOCATOR_STATE_TABLE_NAME, AllocatorStateKey, Btree, ConcurrencyMode, DATA_FREED_TABLE,
    InternalTableDefinition, PageHint, PageList, ReadTransaction, SYSTEM_FREED_TABLE, StorageError,
    TableTree, TableType, TransactionId, TransactionIdWithPagination,
};
use crate::transaction_tracker::{NativeResourceEpoch, NativeResourcePlanningRefusal};
use crate::tree_store::{CompiledPathVisits, NativeKeyPathError, NativePreparedNormalKey};
use crate::{Key, TableHandle, Value};

#[path = "native_allocator_purge.rs"]
mod allocator_purge;
pub(crate) use allocator_purge::{
    NativeAllocatorPurgeObservation, NativeAllocatorPurgeSeed, NativePreparedSystemResources,
};

#[path = "native_profiled_system.rs"]
mod profiled_system;
pub(crate) use profiled_system::NativePreparedProfiledSystemResources;
#[cfg(not(redb_no_std))]
pub(crate) use profiled_system::{
    NativeDetachedProfiledSystemResources, NativeSystemDetachmentFailure,
};

type PathResult<T> = core::result::Result<T, NativeKeyPathError>;

#[cfg(not(redb_no_std))]
#[path = "native_allocated_prefix.rs"]
mod allocated_prefix;
#[cfg(not(redb_no_std))]
pub(super) use allocated_prefix::NativeAllocatedPrefix;
#[cfg(not(redb_no_std))]
pub(crate) use allocated_prefix::NativeAllocatedPrefixWork;

#[path = "native_selected_freed_prefix.rs"]
mod selected_prefix;
pub(crate) use selected_prefix::NativeSelectedFreedPrefix;

#[cfg(not(redb_no_std))]
#[path = "native_freed_point_recipe.rs"]
mod freed_point_recipe;
#[cfg(not(redb_no_std))]
pub(crate) use freed_point_recipe::{
    NativeFreedAppendGapRecipe, NativeFreedAppendPositions, NativeFreedPointRecipe,
    NativeSelectedFreedFrontiers,
};

pub(crate) struct NativeSystemRead<'read, 'epoch, 'tracker> {
    read: &'read ReadTransaction,
    original_epoch: &'epoch NativeResourceEpoch<'tracker>,
    epoch: NativeResourceEpoch<'tracker>,
    tree: TableTree,
    next_write: TransactionId,
    horizon: TransactionId,
    // LAST: physical SYSTEM cached roots must die before writers resume.
    _inspection: crate::db::NativeSystemInspectionLease,
}
impl<'read, 'epoch, 'tracker> NativeSystemRead<'read, 'epoch, 'tracker> {
    pub(super) fn capture(
        read: &'read ReadTransaction,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
    ) -> PathResult<Self> {
        if read.mem.concurrency_mode() != ConcurrencyMode::ExclusiveWriter {
            return Err(NativeKeyPathError::UnsupportedState);
        }
        Self::require_actual_read(read, epoch)?;
        let parts = read
            .tree
            .transaction_guard()
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let (inspection, inspection_epoch) = parts
            .actual_tracker()
            .claim_system_inspection(epoch)
            .map_err(NativeKeyPathError::Epoch)?;
        let (next_write, horizon) = Self::require_actual_read(read, &inspection_epoch)?;
        // Capture only from this actual read's memory, never caller root/guard
        // arguments. The actual read pins USER data; the separate inspection
        // lease prevents ordinary native writers from freeing SYSTEM pages.
        let root = read.mem.get_system_root();
        let tree = TableTree::new(
            root,
            PageHint::Clean,
            read.tree.transaction_guard().clone(),
            read.tree.read_page_resolver(),
        )?;
        let result = Self {
            read,
            original_epoch: epoch,
            epoch: inspection_epoch,
            tree,
            next_write,
            horizon,
            _inspection: inspection,
        };
        result.require_current()?;
        Ok(result)
    }
    fn require_actual_read(
        read: &ReadTransaction,
        epoch: &NativeResourceEpoch<'_>,
    ) -> PathResult<(TransactionId, TransactionId)> {
        let parts = read
            .tree
            .transaction_guard()
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let tracker = parts.actual_tracker();
        let actual = parts.actual_memory();
        let transaction_id = parts.transaction_id();
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        if !alloc::sync::Arc::ptr_eq(actual, &read.mem)
            || read.mem.get_last_committed_transaction_id()? != transaction_id
        {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        let counters = epoch
            .observe_native_write_counters(transaction_id)
            .map_err(NativeKeyPathError::Epoch)?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        Ok(counters)
    }
    pub(crate) fn require_current(&self) -> PathResult<()> {
        if Self::require_actual_read(self.read, &self.epoch)? != (self.next_write, self.horizon) {
            return Err(NativeKeyPathError::Epoch(
                NativeResourcePlanningRefusal::Stale,
            ));
        }
        Ok(())
    }
    pub(crate) fn inspect_namespace_profile(
        &self,
    ) -> PathResult<crate::tree_store::NativeSystemNamespaceProfile<'_, '_, 'tracker>> {
        self.require_current()?;
        let profile = self.tree.inspect_system_namespace_profile(&self.epoch)?;
        self.require_current()?;
        Ok(profile)
    }
    pub(crate) fn next_write_proposal(&self) -> TransactionId {
        self.next_write
    }
    pub(crate) fn observed_horizon(&self) -> TransactionId {
        self.horizon
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn fixed_cleanup_allocated_keys_for_fixture(&self) -> alloc::vec::Vec<(u64, u64)> {
        let Some(tree) = self
            .open_normal::<TransactionIdWithPagination, PageList<'static>>(
                crate::transactions::DATA_ALLOCATED_TABLE.name(),
            )
            .unwrap()
        else {
            return alloc::vec::Vec::new();
        };
        let mut result = alloc::vec::Vec::new();
        for row in tree.range::<_, TransactionIdWithPagination>(&..).unwrap() {
            let entry = row.unwrap();
            let key = entry.key();
            assert!(result.len() < 32, "finite real history fixture");
            result.push((key.transaction_id, key.pagination_id));
            let pages = entry.value();
            for index in 0..pages.len() {
                assert!(self.read.mem.is_allocated(pages.get(index)));
            }
        }
        result
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn fixed_phase_witness_for_fixture(
        &self,
        numerical: &crate::tree_store::NativeNumericalEpoch,
    ) -> PathResult<
        crate::tree_store::NativeOwnedShared<
            crate::sync::Mutex<crate::transactions::NativeFixedPhaseWitness>,
        >,
    > {
        self.require_current()?;
        let resolver = self.read.tree.read_page_resolver();
        let affinity = resolver.admit_fixed_observation(0)?;
        if !affinity.belongs_to(numerical.resources()) {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        // Preadmit actual Arc<OwnedValue<Mutex<Witness>>> before control creation.
        // Each actual byte copy independently admits its physical bytes first.
        let witness = crate::tree_store::NativeOwnedShared::source(
            crate::sync::Mutex::new(crate::transactions::NativeFixedPhaseWitness::empty()),
            numerical.resources(),
        )?;
        let header = self
            .read
            .mem
            .get_system_root()
            .ok_or(NativeKeyPathError::UnsupportedState)?;
        let page = resolver.get_page(header.root, PageHint::Clean)?;
        witness.lock().map_err(StorageError::from)?.retain_actual(
            0,
            self.read.mem.get_last_committed_transaction_id()?,
            header,
            page,
        );
        self.require_current()?;
        Ok(witness)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn actual_post_emission_for_phase_fixture(
        &self,
        witness: &crate::transactions::NativeFixedPhaseWitness,
    ) -> (usize, usize, usize) {
        self.require_current().unwrap();
        let (main, main_namespace) = witness.actual_main_for_fixture();
        let post = main.raw_id().checked_add(1).unwrap();
        let tree = self
            .open_normal::<TransactionIdWithPagination, PageList<'static>>(
                crate::transactions::SYSTEM_FREED_TABLE.name(),
            )
            .unwrap()
            .unwrap();
        let mut rows = 0usize;
        let mut numbers = 0usize;
        let mut actual_main_retired = 0usize;
        for row in tree.range::<_, TransactionIdWithPagination>(&..).unwrap() {
            let entry = row.unwrap();
            if entry.key().transaction_id != post {
                continue;
            }
            assert!(rows < 32, "bounded actual post fixture rows");
            rows += 1;
            let pages = entry.value();
            assert_eq!(
                <PageList<'_> as Value>::as_bytes(&pages).len(),
                PageList::system_freed_capacity_bytes()
            );
            assert!(pages.len() <= PageList::system_freed_capacity_count());
            numbers += pages.len();
            for index in 0..pages.len() {
                actual_main_retired += usize::from(pages.get(index) == main_namespace.root);
            }
        }
        self.require_current().unwrap();
        (rows, numbers, actual_main_retired)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn fixed_cleanup_freed_keys_for_fixture(&self) -> alloc::vec::Vec<(bool, u64, u64)> {
        let mut result = alloc::vec::Vec::new();
        for (system, name) in [
            (false, crate::transactions::DATA_FREED_TABLE.name()),
            (true, crate::transactions::SYSTEM_FREED_TABLE.name()),
        ] {
            let Some(tree) = self
                .open_normal::<TransactionIdWithPagination, PageList<'static>>(name)
                .unwrap()
            else {
                continue;
            };
            let mut family_rows = 0;
            for row in tree.range::<_, TransactionIdWithPagination>(&..).unwrap() {
                let entry = row.unwrap();
                assert!(family_rows < 32, "bounded genuine native history census");
                family_rows += 1;
                let key = entry.key();
                result.push((system, key.transaction_id, key.pagination_id));
            }
        }
        result
    }

    fn open_normal<K: Key + 'static, V: Value + 'static>(
        &self,
        name: &str,
    ) -> PathResult<Option<Btree<K, V>>> {
        let header = self
            .tree
            .get_table::<K, V>(name, TableType::Normal)
            .map_err(|error| {
                NativeKeyPathError::Native(
                    error.into_storage_error_or_corrupted("Native resource definition mismatch"),
                )
            })?;
        let root = match header {
            Some(InternalTableDefinition::Normal { table_root, .. }) => table_root,
            None => return Ok(None),
            Some(InternalTableDefinition::Multimap { .. }) => {
                return Err(NativeKeyPathError::Malformed);
            }
        };
        Ok(Some(Btree::new(
            root,
            PageHint::Clean,
            self.read.tree.transaction_guard().clone(),
            self.read.tree.read_page_resolver(),
        )?))
    }
    // SYSTEM pages are not historical user data. Consume every SYSTEM tree
    // and cached PageImpl before the native writer can reclaim/rewrite them.
    // The returned owner keeps the genuine user read/epoch and exact stamps;
    // it does not retain a SYSTEM page/table/root handle.
    pub(crate) fn prepare_reclamation(
        self,
        budget: &mut ReclamationBudget,
    ) -> PathResult<NativePreparedReclamation<'read, 'epoch, 'tracker>> {
        let system_root = self.read.mem.get_system_root();
        let (budget, committed, selected_records, selected_prefix) = {
            let mut observed = self.observe_freed_paths(budget)?;
            while observed.next()? {}
            observed.finish_prepared_reclamation()?
        }; // Every observer DATA/SYSTEM Btree and cached root is destroyed.
        let prepared = NativePreparedReclamation {
            read: self.read,
            epoch: self.original_epoch,
            next_write: self.next_write,
            system_root,
            budget: Some(budget),
            committed,
            selected_records,
            selected_prefix,
            #[cfg(not(redb_no_std))]
            selected_frontiers: None,
        };
        drop(self); // Destroy the SYSTEM namespace's cached root as well.
        prepared.require_before_write()?;
        Ok(prepared)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_reclamation_with_freed_frontiers(
        self,
        budget: &mut ReclamationBudget,
    ) -> PathResult<NativePreparedReclamation<'read, 'epoch, 'tracker>> {
        let system_root = self.read.mem.get_system_root();
        let (budget, committed, selected_records, selected_prefix, selected_frontiers) = {
            let mut observed = self.observe_freed_paths(budget)?;
            while observed.next()? {}
            let selected_frontiers = observed.inspect_selected_recipe_frontiers()?;
            let (budget, committed, records, prefix) = observed.finish_prepared_reclamation()?;
            (budget, committed, records, prefix, selected_frontiers)
        }; // SYSTEM trees, typed key vectors and their charges are destroyed.
        let prepared = NativePreparedReclamation {
            read: self.read,
            epoch: self.original_epoch,
            next_write: self.next_write,
            system_root,
            budget: Some(budget),
            committed,
            selected_records,
            selected_prefix,
            selected_frontiers,
        };
        drop(self); // cached namespace root before inspection lease release.
        prepared.require_before_write()?;
        Ok(prepared)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn next_write_for_fixture(&self) -> u64 {
        self.require_current().unwrap();
        self.next_write.raw_id()
    }

    pub(crate) fn observe_freed_paths<'system, 'budget>(
        &'system self,
        budget: &'budget mut ReclamationBudget,
    ) -> PathResult<NativeFreedPathObserver<'system, 'read, 'epoch, 'tracker, 'budget>> {
        self.require_current()?;
        let limits = budget.observed_limits();
        // Fixed actual native namespace/control recipe. This inspects native
        // definition paths without treating an endpoint as a height certificate.
        let names = [
            NativePreparedNormalKey::Str(DATA_FREED_TABLE.name()),
            NativePreparedNormalKey::Str(SYSTEM_FREED_TABLE.name()),
            NativePreparedNormalKey::Str(ALLOCATOR_STATE_TABLE_NAME),
        ];
        let namespace = self
            .tree
            .inspect_prepared_resource_definitions(&names, &self.epoch)?;
        let namespace_visits = namespace.visited();
        let mut path_bytes = namespace.observed_page_bytes();
        let mut path_visits = namespace_visits;
        let mut maximum_depth = namespace.maximum_depth();
        drop(namespace);
        let data = self.open_normal::<TransactionIdWithPagination, PageList<'static>>(
            DATA_FREED_TABLE.name(),
        )?;
        let system = self.open_normal::<TransactionIdWithPagination, PageList<'static>>(
            SYSTEM_FREED_TABLE.name(),
        )?;
        // Actual native next-ID append positions, not caller-supplied scalar
        // keys or a rightmost-path global-height certificate. Prospective
        // pagination growth and postcommit mutation costs remain unquoted.
        let epilogue_id = self
            .next_write
            .raw_id()
            .checked_add(1)
            .ok_or(NativeKeyPathError::Overflow)?;
        let append_keys = [
            TransactionIdWithPagination {
                transaction_id: self.next_write.raw_id(),
                pagination_id: 0,
            },
            TransactionIdWithPagination {
                transaction_id: epilogue_id,
                pagination_id: 0,
            },
        ];
        for tree in [&data, &system].into_iter().flatten() {
            for append_key in &append_keys {
                let mut visits = CompiledPathVisits::for_freed_probe();
                let path = tree.inspect_native_resource_key_at_epoch(
                    append_key,
                    &self.epoch,
                    &mut visits,
                )?;
                path_bytes = path_bytes
                    .checked_add(path.cost().observed_page_bytes())
                    .ok_or(NativeKeyPathError::Overflow)?;
                path_visits = path_visits
                    .checked_add(visits.visited())
                    .ok_or(NativeKeyPathError::Overflow)?;
                maximum_depth = maximum_depth.max(path.cost().depth());
            }
        }
        let allocator =
            self.open_normal::<AllocatorStateKey, &'static [u8]>(ALLOCATOR_STATE_TABLE_NAME)?;
        let allocator_state_present = allocator.is_some();
        if let Some(tree) = allocator.as_ref() {
            let mut visits = CompiledPathVisits::for_freed_probe();
            let path = tree.inspect_native_resource_key_at_epoch(
                &AllocatorStateKey::TransactionId,
                &self.epoch,
                &mut visits,
            )?;
            path_bytes = path_bytes
                .checked_add(path.cost().observed_page_bytes())
                .ok_or(NativeKeyPathError::Overflow)?;
            path_visits = path_visits
                .checked_add(visits.visited())
                .ok_or(NativeKeyPathError::Overflow)?;
            maximum_depth = maximum_depth.max(path.cost().depth());
        }
        drop(allocator);
        self.require_current()?;
        Ok(NativeFreedPathObserver {
            actual: self,
            budget,
            limits,
            data,
            system,
            after_data: None,
            after_system: None,
            boundary: None,
            selected_records: 0,
            selected_prefix: NativeSelectedFreedPrefix::empty(),
            selected_page_numbers: 0,
            selected_encoded_bytes: 0,
            selected_weighted_pages: 0,
            path_visits,
            path_bytes,
            maximum_depth,
            allocator_state_present,
            boundary_records: 0,
        })
    }
}
impl Drop for NativeSystemRead<'_, '_, '_> {
    fn drop(&mut self) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeFreedBoundary {
    Empty,
    Horizon,
    BudgetExhausted,
    PageNumbersExhausted,
    UnsupportedWholeEntry,
}
struct Head {
    transaction: u64,
    pagination: u64,
    system: bool,
    count: usize,
    bytes: usize,
    weighted_pages: u64,
}
impl Head {
    fn key(&self) -> TransactionIdWithPagination {
        TransactionIdWithPagination {
            transaction_id: self.transaction,
            pagination_id: self.pagination,
        }
    }
    fn order(&self) -> (u64, u64, bool) {
        (self.transaction, self.pagination, self.system)
    }
}
enum Probe {
    Entry(Head),
    Empty,
    Horizon,
    Exhausted,
    Unsupported,
}

// Exact observations from one captured SYSTEM owner. The one supplied finite
// budget is shared across both families, head probes and selected-entry probes.
// It remains borrowed until this observer and all its native trees are dropped.
pub(crate) struct NativeFreedPathObserver<'system, 'read, 'epoch, 'tracker, 'budget> {
    actual: &'system NativeSystemRead<'read, 'epoch, 'tracker>,
    budget: &'budget mut ReclamationBudget,
    limits: ReclamationLimits,
    data: Option<Btree<TransactionIdWithPagination, PageList<'static>>>,
    system: Option<Btree<TransactionIdWithPagination, PageList<'static>>>,
    after_data: Option<TransactionIdWithPagination>,
    after_system: Option<TransactionIdWithPagination>,
    boundary: Option<NativeFreedBoundary>,
    selected_records: usize,
    selected_prefix: NativeSelectedFreedPrefix,
    selected_page_numbers: usize,
    selected_encoded_bytes: usize,
    selected_weighted_pages: u64,
    path_visits: usize,
    path_bytes: u64,
    boundary_records: usize,
    maximum_depth: usize,
    allocator_state_present: bool,
}
impl<'system, 'read, 'epoch, 'tracker, 'budget>
    NativeFreedPathObserver<'system, 'read, 'epoch, 'tracker, 'budget>
{
    fn add_path(&mut self, visits: usize, bytes: u64, depth: usize) -> PathResult<()> {
        self.maximum_depth = self.maximum_depth.max(depth);
        self.path_visits = self
            .path_visits
            .checked_add(visits)
            .ok_or(NativeKeyPathError::Overflow)?;
        self.path_bytes = self
            .path_bytes
            .checked_add(bytes)
            .ok_or(NativeKeyPathError::Overflow)?;
        Ok(())
    }
    fn peek(
        tree: &Option<Btree<TransactionIdWithPagination, PageList<'static>>>,
        after: Option<&TransactionIdWithPagination>,
        system: bool,
        horizon: TransactionId,
        epoch: &NativeResourceEpoch<'_>,
        budget: &mut ReclamationBudget,
    ) -> PathResult<(Probe, usize, u64, usize)> {
        if !budget.begin_probe() {
            return Ok((Probe::Exhausted, 0, 0, 0));
        }
        let Some(tree) = tree else {
            return Ok((Probe::Empty, 0, 0, 0));
        };
        let mut visits = CompiledPathVisits::for_freed_probe();
        let (entry, cost) = tree.read_native_freed_after(after, epoch, &mut visits)?;
        let Some(entry) = entry else {
            return Ok((
                Probe::Empty,
                visits.visited(),
                cost.observed_page_bytes(),
                cost.depth(),
            ));
        };
        if entry.key().transaction_id >= horizon.raw_id() {
            return Ok((
                Probe::Horizon,
                visits.visited(),
                cost.observed_page_bytes(),
                cost.depth(),
            ));
        }
        let pages = entry.page_list();
        let (count, bytes) = match budget.observe(&pages) {
            Ok(observed) => observed,
            Err(StorageError::ValueTooLarge(_)) => {
                return Ok((
                    Probe::Unsupported,
                    visits.visited(),
                    cost.observed_page_bytes(),
                    cost.depth(),
                ));
            }
            Err(error) => return Err(NativeKeyPathError::Native(error)),
        };
        let mut weighted_pages = 0_u64;
        for index in 0..count {
            weighted_pages = weighted_pages
                .checked_add(pages.get(index).page_size_bytes(1))
                .ok_or(NativeKeyPathError::Overflow)?;
        }
        entry.require_current()?;
        Ok((
            Probe::Entry(Head {
                transaction: entry.key().transaction_id,
                pagination: entry.key().pagination_id,
                system,
                count,
                bytes,
                weighted_pages,
            }),
            visits.visited(),
            cost.observed_page_bytes(),
            cost.depth(),
        ))
    }
    pub(crate) fn next(&mut self) -> PathResult<bool> {
        if self.boundary.is_some() {
            self.actual.require_current()?;
            return Ok(false);
        }
        self.actual.require_current()?;
        if !self.budget.has_removal_slot() {
            self.boundary = Some(NativeFreedBoundary::BudgetExhausted);
            return Ok(false);
        }
        let (data, visits, bytes, depth) = Self::peek(
            &self.data,
            self.after_data.as_ref(),
            false,
            self.actual.horizon,
            &self.actual.epoch,
            self.budget,
        )?;
        self.add_path(visits, bytes, depth)?;
        if matches!(&data, Probe::Unsupported | Probe::Exhausted) {
            self.boundary = Some(if matches!(&data, Probe::Unsupported) {
                self.boundary_records = self
                    .boundary_records
                    .checked_add(1)
                    .ok_or(NativeKeyPathError::Overflow)?;
                NativeFreedBoundary::UnsupportedWholeEntry
            } else {
                NativeFreedBoundary::BudgetExhausted
            });
            self.actual.require_current()?;
            return Ok(false);
        }
        let (system, visits, bytes, depth) = Self::peek(
            &self.system,
            self.after_system.as_ref(),
            true,
            self.actual.horizon,
            &self.actual.epoch,
            self.budget,
        )?;
        self.add_path(visits, bytes, depth)?;
        let boundary_probes = usize::from(matches!(&data, Probe::Horizon | Probe::Unsupported))
            + usize::from(matches!(&system, Probe::Horizon | Probe::Unsupported));
        self.boundary_records = self
            .boundary_records
            .checked_add(boundary_probes)
            .ok_or(NativeKeyPathError::Overflow)?;
        let chosen = match (data, system) {
            (Probe::Exhausted, _) | (_, Probe::Exhausted) => {
                self.boundary = Some(NativeFreedBoundary::BudgetExhausted);
                None
            }
            (Probe::Unsupported, _) | (_, Probe::Unsupported) => {
                self.boundary = Some(NativeFreedBoundary::UnsupportedWholeEntry);
                None
            }
            (Probe::Entry(one), Probe::Entry(two)) => {
                Some(if one.order() <= two.order() { one } else { two })
            }
            (Probe::Entry(one), _) | (_, Probe::Entry(one)) => Some(one),
            (Probe::Empty, Probe::Empty) => {
                self.boundary = Some(NativeFreedBoundary::Empty);
                None
            }
            _ => {
                self.boundary = Some(NativeFreedBoundary::Horizon);
                None
            }
        };
        let Some(chosen) = chosen else {
            self.actual.require_current()?;
            return Ok(false);
        };
        if !self.budget.can_remove(chosen.count) {
            self.boundary_records = self
                .boundary_records
                .checked_add(1)
                .ok_or(NativeKeyPathError::Overflow)?;
            self.boundary = Some(NativeFreedBoundary::PageNumbersExhausted);
            self.actual.require_current()?;
            return Ok(false);
        }
        // Re-probe the exact selected key under the SAME numerical budget,
        // matching cleanup's head + extraction examination accounting.
        let (again, visits, bytes, depth) = if chosen.system {
            Self::peek(
                &self.system,
                self.after_system.as_ref(),
                true,
                self.actual.horizon,
                &self.actual.epoch,
                self.budget,
            )?
        } else {
            Self::peek(
                &self.data,
                self.after_data.as_ref(),
                false,
                self.actual.horizon,
                &self.actual.epoch,
                self.budget,
            )?
        };
        self.add_path(visits, bytes, depth)?;
        let Probe::Entry(again) = again else {
            self.boundary = Some(NativeFreedBoundary::BudgetExhausted);
            self.actual.require_current()?;
            return Ok(false);
        };
        if again.order() != chosen.order()
            || again.count != chosen.count
            || again.bytes != chosen.bytes
            || again.weighted_pages != chosen.weighted_pages
        {
            return Err(NativeKeyPathError::Malformed);
        }
        self.budget.remove(chosen.count);
        if chosen.system {
            self.after_system = Some(chosen.key());
        } else {
            self.after_data = Some(chosen.key());
        }
        self.selected_records = self
            .selected_records
            .checked_add(1)
            .ok_or(NativeKeyPathError::Overflow)?;
        self.selected_page_numbers = self
            .selected_page_numbers
            .checked_add(chosen.count)
            .ok_or(NativeKeyPathError::Overflow)?;
        self.selected_encoded_bytes = self
            .selected_encoded_bytes
            .checked_add(chosen.bytes)
            .ok_or(NativeKeyPathError::Overflow)?;
        self.selected_weighted_pages = self
            .selected_weighted_pages
            .checked_add(chosen.weighted_pages)
            .ok_or(NativeKeyPathError::Overflow)?;
        self.selected_prefix.observe_selected(
            chosen.system,
            chosen.count,
            chosen.bytes,
            chosen.weighted_pages,
        )?;
        self.actual.require_current()?;
        Ok(true)
    }
    fn finish_prepared_reclamation(
        mut self,
    ) -> PathResult<(
        ReclamationBudget,
        TransactionId,
        usize,
        NativeSelectedFreedPrefix,
    )> {
        self.actual.require_current()?;
        if !matches!(
            self.boundary,
            Some(
                NativeFreedBoundary::BudgetExhausted
                    | NativeFreedBoundary::Empty
                    | NativeFreedBoundary::Horizon
            )
        ) {
            return Err(NativeKeyPathError::UnsupportedState);
        }
        // One actual tail record per family admits exact subsequent
        // remaining-ID checks. No guessed ID completion or full-tree scan.
        let (data, visits, bytes, depth) = Self::peek(
            &self.data,
            self.after_data.as_ref(),
            false,
            self.actual.horizon,
            &self.actual.epoch,
            self.budget,
        )?;
        self.add_path(visits, bytes, depth)?;
        let (system, visits, bytes, depth) = Self::peek(
            &self.system,
            self.after_system.as_ref(),
            true,
            self.actual.horizon,
            &self.actual.epoch,
            self.budget,
        )?;
        self.add_path(visits, bytes, depth)?;
        let data_tail = observed_tail(data)?;
        let system_tail = observed_tail(system)?;
        self.actual.require_current()?;
        let fence = NativeReclamationFence::from_observed_prefix(
            self.actual.horizon,
            self.after_data.as_ref(),
            self.after_system.as_ref(),
            data_tail,
            system_tail,
        );
        // Complete every fallible stamp read/check before moving the actual
        // guarded buffers out of the caller's simulation budget.
        let committed = self.actual.read.mem.get_last_committed_transaction_id()?;
        self.actual.require_current()?;
        let budget = self.budget.take_prepared_observation(self.limits, fence);
        let selected_prefix = core::mem::replace(
            &mut self.selected_prefix,
            NativeSelectedFreedPrefix::empty(),
        );
        Ok((budget, committed, self.selected_records, selected_prefix))
    }

    pub(crate) fn boundary(&self) -> Option<NativeFreedBoundary> {
        self.boundary
    }
    pub(crate) fn selected_records(&self) -> usize {
        self.selected_records
    }
    pub(crate) fn selected_page_numbers(&self) -> usize {
        self.selected_page_numbers
    }
    pub(crate) fn selected_encoded_bytes(&self) -> usize {
        self.selected_encoded_bytes
    }
    pub(crate) fn selected_weighted_pages(&self) -> u64 {
        self.selected_weighted_pages
    }
    pub(crate) fn path_visits(&self) -> usize {
        self.path_visits
    }
    pub(crate) fn path_bytes(&self) -> u64 {
        self.path_bytes
    }
    pub(crate) fn maximum_depth(&self) -> usize {
        self.maximum_depth
    }
    pub(crate) fn allocator_state_present(&self) -> bool {
        self.allocator_state_present
    }
    pub(crate) fn boundary_records(&self) -> usize {
        self.boundary_records
    }
    pub(crate) fn last_data(&self) -> Option<&TransactionIdWithPagination> {
        self.after_data.as_ref()
    }
    pub(crate) fn last_system(&self) -> Option<&TransactionIdWithPagination> {
        self.after_system.as_ref()
    }
    pub(crate) fn require_current(&self) -> PathResult<()> {
        self.actual.require_current()
    }
}
impl Drop for NativeFreedPathObserver<'_, '_, '_, '_, '_> {
    fn drop(&mut self) {}
}

fn observed_tail(probe: Probe) -> PathResult<NativeReclamationTail> {
    match probe {
        Probe::Empty => Ok(NativeReclamationTail::Empty),
        Probe::Horizon => Ok(NativeReclamationTail::Horizon),
        Probe::Entry(head) => Ok(NativeReclamationTail::Entry {
            key: head.key(),
            count: head.count,
            bytes: head.bytes,
        }),
        Probe::Exhausted | Probe::Unsupported => Err(NativeKeyPathError::UnsupportedState),
    }
}

// Partial native planning evidence only. The complete resource factory must
// validate + admit BEFORE constructing WriteTransaction, then install this
// fence before first-party Source obligations. No public shipping factory.
pub(crate) struct NativePreparedReclamation<'read, 'epoch, 'tracker> {
    read: &'read ReadTransaction,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    next_write: TransactionId,
    system_root: Option<crate::tree_store::BtreeHeader>,
    budget: Option<ReclamationBudget>,
    committed: TransactionId,
    selected_records: usize,
    selected_prefix: NativeSelectedFreedPrefix,
    #[cfg(not(redb_no_std))]
    selected_frontiers: Option<NativeSelectedFreedFrontiers>,
}
impl NativePreparedReclamation<'_, '_, '_> {
    #[cfg(all(test, not(redb_no_std)))]
    fn detachment_state_for_test(
        &self,
    ) -> (
        Option<TransactionId>,
        (usize, usize, usize, usize),
        usize,
        bool,
    ) {
        let (horizon, remaining, buffers) =
            self.budget.as_ref().unwrap().detachment_state_for_test();
        (
            horizon,
            remaining,
            buffers,
            self.selected_frontiers.is_some(),
        )
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn selected_frontiers(&self) -> PathResult<Option<&NativeSelectedFreedFrontiers>> {
        self.require_before_write()?;
        Ok(self.selected_frontiers.as_ref())
    }
    pub(crate) fn selected_prefix(&self) -> PathResult<&NativeSelectedFreedPrefix> {
        self.require_before_write()?;
        Ok(&self.selected_prefix)
    }
    pub(crate) fn selected_records(&self) -> usize {
        self.selected_records
    }
    pub(crate) fn require_before_write(&self) -> PathResult<()> {
        let (next, _current_horizon) =
            NativeSystemRead::require_actual_read(self.read, self.epoch)?;
        if next != self.next_write
            || self.read.mem.get_system_root() != self.system_root
            || self.read.mem.get_last_committed_transaction_id()? != self.committed
        {
            return Err(NativeKeyPathError::Epoch(
                NativeResourcePlanningRefusal::Stale,
            ));
        }
        // Reader release may enlarge eligibility: the installed fence retains
        // the smaller prepared horizon and exact old prefix.
        Ok(())
    }
    pub(crate) fn install_into(
        mut self,
        transaction: &mut super::WriteTransaction,
    ) -> PathResult<()> {
        if !alloc::sync::Arc::ptr_eq(&transaction.mem, &self.read.mem)
            || transaction.transaction_id != self.next_write
            || transaction.mem.get_system_root() != self.system_root
            || transaction.mem.get_last_committed_transaction_id()? != self.committed
        {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        if transaction
            .dirty
            .load(core::sync::atomic::Ordering::Acquire)
            || transaction.is_poisoned()
            || transaction.quick_repair
            || transaction.durability != super::InternalDurability::Immediate
            || transaction.mem.concurrency_mode() != ConcurrencyMode::ExclusiveWriter
        {
            return Err(NativeKeyPathError::UnsupportedState);
        }
        transaction
            .transaction_tracker
            .require_prepared_resource_writer(self.epoch, transaction.transaction_id)
            .map_err(NativeKeyPathError::Epoch)?;
        #[cfg(not(redb_no_std))]
        if let Some(prefix) = self
            .budget
            .as_ref()
            .and_then(|budget| budget.allocated.as_ref())
        {
            prefix.require_before_install(transaction)?;
        }
        let mut owner = transaction
            .reclamation
            .lock()
            .map_err(|_| NativeKeyPathError::Epoch(NativeResourcePlanningRefusal::Poisoned))?;
        if owner.is_some() {
            return Err(NativeKeyPathError::UnsupportedState);
        }
        *owner = Some(
            self.budget
                .take()
                .ok_or(NativeKeyPathError::UnsupportedState)?,
        );
        Ok(())
    }
}
// The original fence/budget and guarded selected facts move inseparably.
// There is deliberately no budget getter/installation/extraction on this owner.
#[cfg(not(redb_no_std))]
pub(super) struct NativeDetachedReclamation<'work> {
    binding: crate::db::NativePreparedSystemBinding<'work>,
    budget: Option<ReclamationBudget>,
    selected_records: usize,
    selected_prefix: NativeSelectedFreedPrefix,
    selected_frontiers: Option<NativeSelectedFreedFrontiers>,
}
#[cfg(not(redb_no_std))]
impl NativeDetachedReclamation<'_> {
    pub(super) fn phase_forests(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> PathResult<[crate::tree_store::NativeFreedPhaseForest; 3]> {
        self.require_current()?;
        let selected = self
            .selected_frontiers
            .as_ref()
            .ok_or(NativeKeyPathError::UnsupportedState)?;
        let (data, system) = selected.phase_forests(resources)?;
        let allocated = self
            .budget
            .as_ref()
            .ok_or(NativeKeyPathError::UnsupportedState)?
            .allocated
            .as_ref()
            .ok_or(NativeKeyPathError::UnsupportedState)?
            .phase_forest(resources)?;
        self.require_current()?;
        Ok([data, system, allocated])
    }

    pub(super) fn take_for_fixed_writer(
        &mut self,
        claim: &crate::db::NativeFixedWriterClaim<'_>,
    ) -> PathResult<ReclamationBudget> {
        self.binding.require_fixed_writer(claim.actual_guard())?;
        self.budget
            .take()
            .ok_or(NativeKeyPathError::UnsupportedState)
    }

    #[cfg(test)]
    fn detachment_state_for_test(
        &self,
    ) -> (
        Option<TransactionId>,
        (usize, usize, usize, usize),
        usize,
        bool,
    ) {
        let (horizon, remaining, buffers) =
            self.budget.as_ref().unwrap().detachment_state_for_test();
        (
            horizon,
            remaining,
            buffers,
            self.selected_frontiers.is_some(),
        )
    }
    pub(super) fn require_current(&self) -> PathResult<()> {
        self.binding.require_current()
    }
    pub(super) fn selected_prefix(&self) -> PathResult<&NativeSelectedFreedPrefix> {
        self.require_current()?;
        Ok(&self.selected_prefix)
    }
    pub(super) fn append_positions(&self) -> PathResult<&NativeFreedAppendPositions> {
        self.require_current()?;
        self.selected_frontiers
            .as_ref()
            .map(NativeSelectedFreedFrontiers::append_positions)
            .ok_or(NativeKeyPathError::UnsupportedState)
    }
}
#[cfg(not(redb_no_std))]
impl<'read, 'epoch, 'work> NativePreparedReclamation<'read, 'epoch, 'work> {
    fn detach_parts(
        &mut self,
        actual: &'work crate::Database,
        original: &NativeResourceEpoch<'work>,
    ) -> PathResult<NativeDetachedReclamation<'work>> {
        // Same epoch INSTANCE while still borrowed, not merely matching stamps.
        if !core::ptr::eq(self.epoch, original) {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        self.require_before_write()?;
        let parts = self
            .read
            .tree
            .transaction_guard()
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let binding = crate::db::NativePreparedSystemBinding::capture(actual, &parts, original)?;
        if !binding.matches_prepared(self.next_write, self.system_root, self.committed)
            || self.budget.is_none()
        {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        binding.require_current()?;
        // All fallible checks precede the sole ownership transfer. The old
        // EmptyDrop wrapper retains no SYSTEM page or inspection lease.
        Ok(NativeDetachedReclamation {
            binding,
            budget: self.budget.take(),
            selected_records: self.selected_records,
            selected_prefix: core::mem::replace(
                &mut self.selected_prefix,
                NativeSelectedFreedPrefix::empty(),
            ),
            selected_frontiers: self.selected_frontiers.take(),
        })
    }
}

impl Drop for NativePreparedReclamation<'_, '_, '_> {
    fn drop(&mut self) {}
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_system_inspection_tests.rs"]
mod inspection_tests;

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_system_namespace_tests.rs"]
mod namespace_profile_tests;
