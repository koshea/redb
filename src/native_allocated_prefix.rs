//! Closed bounded historical DATA_ALLOCATED row service. This is not a
//! complete resource factory, Source authority, or a production service policy.
use super::freed_point_recipe::{NativeFreedPointRecipe, SelectedPoint};
use super::{NativeKeyPathError as E, NativeSystemRead, PathResult};
use crate::transaction_tracker::TransactionId;
use crate::transactions::{
    DATA_ALLOCATED_TABLE, PageList, SystemTable, TransactionIdWithPagination, WriteTransaction,
    bounded_reclamation::ReclamationBudget,
};
use crate::tree_store::{
    Btree, BtreeHeader, CompiledPathVisits, NativeControlAllocation,
    NativeFixedObservationAdmission, NativeFreedPointFrontierShape,
};
use crate::{TableHandle, Value};
use alloc::vec::Vec;
use core::mem::size_of;

// Independent of FREED K. The complete recipe must derive its production
// service/physical allowance; only the bounded native fixture can construct it
// before that producer is joined. No guessed production constructor exists.
pub(crate) struct NativeAllocatedPrefixWork {
    records: usize,
    requires_checked_ignore: bool,
}
impl NativeAllocatedPrefixWork {
    // Independent of FREED K: the closed checked Immediate executor has ZERO
    // DATA_ALLOCATED emission. One historical whole row is the minimal strict
    // service quantum. The installed tag revalidates that premise at use.
    pub(crate) fn for_checked_fixed_recipe(
        data: &crate::transactions::NativeFixedDataCensus<'_>,
        numerical: &crate::tree_store::NativeNumericalEpoch,
    ) -> PathResult<Self> {
        data.require_fixed_cleanup_pool(numerical)?;
        Ok(Self {
            records: 1,
            requires_checked_ignore: true,
        })
    }

    #[cfg(test)]
    pub(in crate::transactions) fn for_test(records: usize) -> Self {
        Self {
            records,
            requires_checked_ignore: false,
        }
    }
}
pub(in crate::transactions) struct NativeAllocatedPrefix {
    requires_checked_ignore: bool,
    root: Option<BtreeHeader>,
    points: Vec<SelectedPoint>,
    first: Option<TransactionIdWithPagination>,
    after: Option<TransactionIdWithPagination>,
    frontier: Option<NativeFreedPointFrontierShape>,
    path_visits: usize,
    path_bytes: u64,
    inspected_page_numbers: usize,
    page_number_visits_upper: usize,
    post_horizon: Option<u64>,
    pre_free_until: Option<TransactionId>,
    post_free_until: Option<TransactionId>,
    admission: NativeFixedObservationAdmission,
    // LAST: vector backing and all retained inline facts die before charge.
    _allocation: NativeControlAllocation,
}
fn copy_key(key: &TransactionIdWithPagination) -> TransactionIdWithPagination {
    TransactionIdWithPagination {
        transaction_id: key.transaction_id,
        pagination_id: key.pagination_id,
    }
}
fn order(key: &TransactionIdWithPagination) -> (u64, u64) {
    (key.transaction_id, key.pagination_id)
}
impl NativeAllocatedPrefix {
    pub(in crate::transactions) fn phase_forest(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> PathResult<crate::tree_store::NativeFreedPhaseForest> {
        self.admission.require_current()?;
        if !self._allocation.belongs_to(resources) {
            return Err(E::ForeignOwner);
        }
        crate::tree_store::NativeFreedPhaseForest::from_original_owners(
            self.frontier.as_ref(),
            None,
            resources,
        )
    }

    pub(in crate::transactions) fn require_before_install(
        &self,
        tx: &WriteTransaction,
    ) -> PathResult<()> {
        self.admission.require_current()?;
        if self.requires_checked_ignore && !tx.checked_source_immediate {
            return Err(E::UnsupportedState);
        }
        tx.transaction_tracker
            .check_source_no_savepoint_writer(&tx.mem, &tx.transaction_guard)?;
        tx.mem.require_allocated_prefix_post_only()?;
        Ok(())
    }
    pub(in crate::transactions) fn require_before_flush(
        &self,
        tx: &WriteTransaction,
    ) -> crate::Result<()> {
        self.admission
            .require_current()
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        if self.requires_checked_ignore && !tx.checked_source_immediate {
            return Err(crate::tree_store::source_owner_refused());
        }
        tx.transaction_tracker
            .check_source_no_savepoint_writer(&tx.mem, &tx.transaction_guard)?;
        tx.mem.require_allocated_prefix_post_only()?;
        if tx.durability != crate::transactions::InternalDurability::Immediate
            || tx.quick_repair
            || tx.restored_transaction.is_some()
            || self.post_horizon.is_some()
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        {
            let savepoints = tx.savepoint_state.lock()?;
            if savepoints.has_created_or_deleted() || !savepoints.invalidated.is_empty() {
                return Err(crate::tree_store::source_owner_refused());
            }
        }
        Ok(())
    }
    pub(in crate::transactions) fn require_root(
        &self,
        root: Option<BtreeHeader>,
    ) -> crate::Result<()> {
        if root != self.root {
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(())
    }
    pub(in crate::transactions) fn effective_horizon(
        &self,
        current: TransactionId,
    ) -> TransactionId {
        // Checked at the actual typed observation/removal boundary, never a
        // sentinel passed to TransactionId::next(). No stored H is modified.
        let cap = if self.post_horizon.is_some() {
            self.post_free_until
        } else {
            self.pre_free_until
        };
        cap.map_or(current, |cap| current.min(cap))
    }
    pub(in crate::transactions) fn remove_from(
        &mut self,
        table: &mut SystemTable<'_, TransactionIdWithPagination, PageList<'static>>,
    ) -> crate::Result<u64> {
        // No raw cursor/old-value guard escapes; each alias dies before the
        // next removal. Full delete-count windows were inspected, not a single
        // neighbour assumed stable across this sequence.
        for point in &self.points {
            let first = table
                .range::<TransactionIdWithPagination>(..)?
                .next()
                .transpose()?;
            let matches = first
                .as_ref()
                .is_some_and(|(key, _)| order(&key.value()) == order(&point.key));
            drop(first);
            if !matches {
                return Err(crate::tree_store::source_owner_refused());
            }
            let removed = table.remove(&point.key)?;
            if removed.is_none() {
                return Err(crate::tree_store::source_owner_refused());
            }
            drop(removed);
        }
        let remaining = table
            .range::<TransactionIdWithPagination>(..)?
            .next()
            .transpose()?;
        let horizon = remaining
            .as_ref()
            .map_or(u64::MAX, |(key, _)| key.value().transaction_id);
        // Exact remaining historical head or exact end. The checked runtime
        // profile proved no inherited/current DATA rows can be appended here;
        // PageAllocator's real DATA/SYS rollback/adoption tracking is separate.
        let matches = match (&self.after, &remaining) {
            (Some(after), Some((key, _))) => order(&key.value()) == order(after),
            (None, None) => true,
            _ => false,
        };
        if !matches {
            return Err(crate::tree_store::source_owner_refused());
        }
        let free_until = remaining
            .as_ref()
            .map(|(key, _)| {
                key.value()
                    .transaction_id
                    .checked_add(1)
                    .map(TransactionId::new)
                    .ok_or_else(crate::tree_store::source_owner_refused)
            })
            .transpose()?;
        drop(remaining);
        self.post_free_until = free_until;
        self.post_horizon = Some(horizon);
        Ok(horizon)
    }
}
impl<'read, 'epoch, 'tracker> NativeSystemRead<'read, 'epoch, 'tracker> {
    fn inspect_allocated_prefix(
        &self,
        work: NativeAllocatedPrefixWork,
    ) -> PathResult<NativeAllocatedPrefix> {
        self.require_current()?;
        self.read.mem.require_allocated_prefix_post_only()?;
        // Independent finite CPU work: selected M rows plus one successor,
        // each with at most the native 400-PN padded capacity. Not free work.
        let page_number_visits_upper = work
            .records
            .checked_add(1)
            .and_then(|rows| rows.checked_mul(PageList::allocated_capacity_count()))
            .ok_or(E::Overflow)?;
        // Actual typed-vector and result/capture overlap before reserve/open.
        let bytes = crate::tree_store::freed_point_selection_capacity_bytes::<
            SelectedPoint,
            (
                NativeAllocatedPrefix,
                NativeAllocatedPrefixWork,
                Option<NativeAllocatedPrefix>,
                Option<Btree<TransactionIdWithPagination, PageList<'static>>>,
                CompiledPathVisits,
                Option<TransactionIdWithPagination>,
                Option<TransactionIdWithPagination>,
                NativeFixedObservationAdmission,
                crate::tree_store::PageResolver,
                core::result::Result<NativeAllocatedPrefix, E>,
                usize,
                usize,
                u64,
            ),
        >(work.records, 0)?;
        let resolver = self.read.tree.read_page_resolver();
        let bytes = usize::try_from(bytes)
            .map_err(|_| E::Overflow)?
            .checked_add(
                crate::tree_store::PageResolver::fixed_page_number_validation_control_bytes(),
            )
            .ok_or(E::Overflow)?;
        let allocation = resolver.admit_fixed_observation(bytes)?;
        let admission = resolver.seal_fixed_observation_admission()?;
        let mut result = NativeAllocatedPrefix {
            requires_checked_ignore: work.requires_checked_ignore,
            root: None,
            points: Vec::new(),
            first: None,
            after: None,
            frontier: None,
            path_visits: 0,
            path_bytes: 0,
            inspected_page_numbers: 0,
            page_number_visits_upper,
            post_horizon: None,
            pre_free_until: None,
            post_free_until: None,
            admission,
            _allocation: allocation,
        };
        result
            .points
            .try_reserve_exact(work.records)
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        let tree = self.open_normal::<TransactionIdWithPagination, PageList<'static>>(
            DATA_ALLOCATED_TABLE.name(),
        )?;
        if let Some(tree) = tree {
            result.root = tree.get_root();
            let mut after = None;
            // Exactly M selected rows + ONE real successor/empty witness.
            for index in 0..=work.records {
                let mut visits = CompiledPathVisits::for_freed_probe();
                let (entry, cost) =
                    tree.read_native_freed_after(after.as_ref(), &self.epoch, &mut visits)?;
                result.path_visits = result
                    .path_visits
                    .checked_add(visits.visited())
                    .ok_or(E::Overflow)?;
                result.path_bytes = result
                    .path_bytes
                    .checked_add(cost.observed_page_bytes())
                    .ok_or(E::Overflow)?;
                let Some(entry) = entry else {
                    break;
                };
                let pages = entry.page_list();
                // PageList has native padded capacity. Historical PN lists
                // are validated, NEVER interpreted as pages to free here.
                if pages.data.len() < size_of::<u16>() {
                    return Err(E::Malformed);
                }
                let count = pages.len();
                if count > crate::transactions::MAIN_PAGE_LIST_CHUNK
                    || pages.data.len()
                        != PageList::required_bytes(crate::transactions::MAIN_PAGE_LIST_CHUNK)
                    || entry.key().transaction_id >= self.next_write.raw_id()
                {
                    return Err(E::UnsupportedState);
                }
                resolver.require_fixed_allocated_page_numbers(&pages)?;
                result.inspected_page_numbers = result
                    .inspected_page_numbers
                    .checked_add(count)
                    .ok_or(E::Overflow)?;
                if result.inspected_page_numbers > result.page_number_visits_upper {
                    return Err(E::VisitsExceeded);
                }
                // Numeric inspection work only; these PNs are NEVER counted
                // as pages serviced by deletion of allocated metadata rows.
                entry.require_current()?;
                if index == 0 {
                    result.pre_free_until = Some(TransactionId::new(
                        entry
                            .key()
                            .transaction_id
                            .checked_add(1)
                            .ok_or(E::Overflow)?,
                    ));
                    result.first = Some(copy_key(entry.key()));
                }
                if index == work.records {
                    result.after = Some(copy_key(entry.key()));
                    break;
                }
                let key = copy_key(entry.key());
                let encoded = TransactionIdWithPagination::as_bytes(&key);
                after = Some(copy_key(&key));
                result.points.push(SelectedPoint { key, encoded });
            }
            if !result.points.is_empty() {
                let recipe =
                    NativeFreedPointRecipe::for_allocated(&tree, &self.epoch, &result.points);
                result.frontier = Some(
                    tree.prepare_freed_point_frontier(&recipe, &self.epoch)?
                        .collect_shape()?,
                );
            }
        }
        self.require_current()?;
        result.admission.require_current()?;
        Ok(result)
    }
    pub(crate) fn prepare_reclamation_with_allocated_prefix(
        self,
        budget: &mut ReclamationBudget,
        work: NativeAllocatedPrefixWork,
    ) -> PathResult<super::NativePreparedReclamation<'read, 'epoch, 'tracker>> {
        if budget.allocated.is_some() {
            return Err(E::UnsupportedState);
        }
        budget.require_allocated_prefix_buffers(&self.read.tree.read_page_resolver())?;
        let allocated = self.inspect_allocated_prefix(work)?;
        budget.allocated = Some(allocated);
        self.prepare_reclamation_with_freed_frontiers(budget)
    }
    pub(crate) fn prepare_profiled_system_with_allocated_prefix(
        self,
        budget: &mut ReclamationBudget,
        work: NativeAllocatedPrefixWork,
    ) -> PathResult<super::NativePreparedProfiledSystemResources<'read, 'epoch, 'tracker>> {
        if budget.allocated.is_some() {
            return Err(E::UnsupportedState);
        }
        budget.require_allocated_prefix_buffers(&self.read.tree.read_page_resolver())?;
        let allocated = self.inspect_allocated_prefix(work)?;
        budget.allocated = Some(allocated);
        self.prepare_profiled_system_with_freed_frontiers(budget)
    }
}
#[cfg(test)]
#[path = "native_allocated_prefix_tests.rs"]
mod tests;
