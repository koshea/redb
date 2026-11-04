//! Closed typed SYSTEM deletion recipes selected by the actual inspected owner.
//! Prospective appends and mixed evolved-tree policy remain separate obligations.
use super::{NativeFreedPathObserver, NativeKeyPathError as E, PathResult};
use crate::transaction_tracker::NativeResourceEpoch;
use crate::transactions::{PageList, TransactionIdWithPagination};
use crate::tree_store::{
    Btree, NativeControlAllocation, NativeFreedAppendGapShape, NativeFreedPointFrontierShape,
};
use crate::{TableHandle, Value};
use alloc::vec::Vec;
use core::mem::size_of;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeFreedPointFamily {
    Data,
    System,
    Allocated,
}
impl NativeFreedPointFamily {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Data => crate::transactions::DATA_FREED_TABLE.name(),
            Self::System => crate::transactions::SYSTEM_FREED_TABLE.name(),
            Self::Allocated => crate::transactions::DATA_ALLOCATED_TABLE.name(),
        }
    }
}
pub(super) struct SelectedPoint {
    // The typed key is retained; encoded bytes are derived by its native Value
    // implementation after admission. They never enter byte-lex compilation.
    pub(super) key: TransactionIdWithPagination,
    pub(super) encoded: [u8; 2 * size_of::<u64>()],
}
pub(super) struct NativeFreedRecipeBuffers {
    data: Vec<SelectedPoint>,
    system: Vec<SelectedPoint>,
    // Both vectors physically die before SAME context charge refunds.
    _allocation: NativeControlAllocation,
}
impl NativeFreedRecipeBuffers {
    fn storage_bytes(data: usize, system: usize) -> PathResult<u64> {
        crate::tree_store::freed_point_selection_capacity_bytes::<
            SelectedPoint,
            (Self, NativeSelectedFreedFrontiers),
        >(data, system)
    }
    pub(super) fn observe(
        &mut self,
        system: bool,
        key: TransactionIdWithPagination,
    ) -> PathResult<()> {
        let points = if system {
            &mut self.system
        } else {
            &mut self.data
        };
        if points.len() == points.capacity() {
            return Err(E::VisitsExceeded);
        }
        if points.last().is_some_and(|last| {
            (last.key.transaction_id, last.key.pagination_id)
                >= (key.transaction_id, key.pagination_id)
        }) {
            return Err(E::Malformed);
        }
        let encoded = TransactionIdWithPagination::as_bytes(&key);
        points.push(SelectedPoint { key, encoded });
        Ok(())
    }
}

pub(crate) struct NativeFreedPointRecipe<'recipe, 'tree, 'tracker> {
    family: NativeFreedPointFamily,
    actual: &'tree Btree<TransactionIdWithPagination, PageList<'static>>,
    epoch: &'tree NativeResourceEpoch<'tracker>,
    points: &'recipe [SelectedPoint],
}
impl<'recipe, 'tree, 'tracker> NativeFreedPointRecipe<'recipe, 'tree, 'tracker> {
    // Only the actual allocated-table inspector supplies these admitted keys.
    pub(super) fn for_allocated(
        actual: &'tree Btree<TransactionIdWithPagination, PageList<'static>>,
        epoch: &'tree NativeResourceEpoch<'tracker>,
        points: &'recipe [SelectedPoint],
    ) -> Self {
        Self {
            family: NativeFreedPointFamily::Allocated,
            actual,
            epoch,
            points,
        }
    }
    pub(crate) fn family(&self) -> NativeFreedPointFamily {
        self.family
    }
    pub(crate) fn points(&self) -> usize {
        self.points.len()
    }
    pub(crate) fn key_at(&self, index: usize) -> PathResult<&[u8]> {
        Ok(&self.points.get(index).ok_or(E::Malformed)?.encoded)
    }
    pub(crate) fn require_for(
        &self,
        actual: &Btree<TransactionIdWithPagination, PageList<'static>>,
        epoch: &NativeResourceEpoch<'_>,
    ) -> PathResult<()> {
        if !core::ptr::eq(self.actual, actual) || !core::ptr::eq(self.epoch, epoch) {
            return Err(E::ForeignOwner);
        }
        actual.require_freed_point_epoch(epoch)
    }
}
impl Drop for NativeFreedPointRecipe<'_, '_, '_> {
    fn drop(&mut self) {}
}

// A native-key interval, not an executable put/deletion recipe. Only the
// actual inspected owner constructs it; all fields/constructors stay private.
pub(crate) struct NativeFreedAppendGapRecipe<'tree, 'epoch, 'tracker> {
    actual: &'tree Btree<TransactionIdWithPagination, PageList<'static>>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    lower: [u8; 2 * size_of::<u64>()],
    upper: [u8; 2 * size_of::<u64>()],
}
impl<'tree, 'epoch, 'tracker> NativeFreedAppendGapRecipe<'tree, 'epoch, 'tracker> {
    fn for_native_interval(
        actual: &'tree Btree<TransactionIdWithPagination, PageList<'static>>,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
        start: &TransactionIdWithPagination,
    ) -> PathResult<Self> {
        actual.require_freed_point_epoch(epoch)?;
        let upper = TransactionIdWithPagination {
            transaction_id: start.transaction_id.checked_add(1).ok_or(E::Overflow)?,
            pagination_id: 0,
        };
        let result = Self {
            actual,
            epoch,
            lower: TransactionIdWithPagination::as_bytes(start),
            upper: TransactionIdWithPagination::as_bytes(&upper),
        };
        result.require_for(actual, epoch)?;
        Ok(result)
    }
    pub(crate) fn lower_bytes(&self) -> &[u8; 2 * size_of::<u64>()] {
        &self.lower
    }
    pub(crate) fn upper_bytes(&self) -> &[u8; 2 * size_of::<u64>()] {
        &self.upper
    }
    pub(crate) fn require_for(
        &self,
        actual: &Btree<TransactionIdWithPagination, PageList<'static>>,
        epoch: &NativeResourceEpoch<'_>,
    ) -> PathResult<()> {
        if !core::ptr::eq(actual, self.actual) || !core::ptr::eq(epoch, self.epoch) {
            return Err(E::ForeignOwner);
        }
        actual.require_freed_point_epoch(epoch)
    }
}

pub(crate) struct NativeSelectedFreedFrontiers {
    data: Option<NativeFreedPointFrontierShape>,
    system: Option<NativeFreedPointFrontierShape>,
    append_positions: NativeFreedAppendPositions,
    data_main_gap: Option<NativeFreedAppendGapShape>,
    system_post_gap: Option<NativeFreedAppendGapShape>,
    // Retained inline controls remain charged in the exact prepared owner.
    _allocation: NativeControlAllocation,
}
impl NativeSelectedFreedFrontiers {
    pub(crate) fn data(&self) -> Option<&NativeFreedPointFrontierShape> {
        self.data.as_ref()
    }
    pub(crate) fn system(&self) -> Option<&NativeFreedPointFrontierShape> {
        self.system.as_ref()
    }
    pub(super) fn phase_forests(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> PathResult<(
        crate::tree_store::NativeFreedPhaseForest,
        crate::tree_store::NativeFreedPhaseForest,
    )> {
        if !self._allocation.belongs_to(resources) {
            return Err(E::ForeignOwner);
        }
        Ok((
            crate::tree_store::NativeFreedPhaseForest::from_original_owners(
                self.data.as_ref(),
                self.data_main_gap.as_ref(),
                resources,
            )?,
            crate::tree_store::NativeFreedPhaseForest::from_original_owners(
                self.system.as_ref(),
                self.system_post_gap.as_ref(),
                resources,
            )?,
        ))
    }
    pub(crate) fn append_positions(&self) -> &NativeFreedAppendPositions {
        &self.append_positions
    }
    // Production callers obtain this borrowed owner only through the existing
    // prepared reclamation getter, which checks the ORIGINAL read/epoch/root.
    // There is no take/clone of either gap and no standalone epoch authority.
    // A future detached getter MUST call its existing binding.require_current.
    pub(crate) fn data_main_gap(&self) -> Option<&NativeFreedAppendGapShape> {
        self.data_main_gap.as_ref()
    }
    pub(crate) fn system_post_gap(&self) -> Option<&NativeFreedAppendGapShape> {
        self.system_post_gap.as_ref()
    }
}

// Actual current append positions only. Prospective row counts/padded value
// envelopes require the complete emission owner and are NOT minted here.
pub(crate) struct NativeFreedAppendPositions {
    data_main: TransactionIdWithPagination,
    system_main: TransactionIdWithPagination,
    system_post: TransactionIdWithPagination,
    path_visits: usize,
    path_bytes: u64,
    maximum_depth: usize,
}
impl NativeFreedAppendPositions {
    pub(crate) fn data_main(&self) -> &TransactionIdWithPagination {
        &self.data_main
    }
    pub(crate) fn system_main(&self) -> &TransactionIdWithPagination {
        &self.system_main
    }
    pub(crate) fn system_post(&self) -> &TransactionIdWithPagination {
        &self.system_post
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
}

impl NativeFreedPathObserver<'_, '_, '_, '_, '_> {
    fn collect_selected_recipe_buffers(&mut self) -> PathResult<NativeFreedRecipeBuffers> {
        self.actual.require_current()?;
        if self.boundary.is_none() {
            return Err(E::UnsupportedState);
        }
        let data_count = self.selected_prefix.data().records();
        let system_count = self.selected_prefix.system().records();
        let bytes = usize::try_from(NativeFreedRecipeBuffers::storage_bytes(
            data_count,
            system_count,
        )?)
        .map_err(|_| E::Overflow)?;
        let allocation = self
            .actual
            .read
            .tree
            .read_page_resolver()
            .admit_fixed_observation(bytes)?;
        let mut result = NativeFreedRecipeBuffers {
            data: Vec::new(),
            system: Vec::new(),
            _allocation: allocation,
        };
        result
            .data
            .try_reserve_exact(data_count)
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        result
            .system
            .try_reserve_exact(system_count)
            .map_err(|_| crate::tree_store::source_owner_refused())?;
        for system in [false, true] {
            let expected = if system {
                self.selected_prefix.system()
            } else {
                self.selected_prefix.data()
            };
            let expected = (
                expected.records(),
                expected.page_numbers(),
                expected.canonical_kv_bytes(),
                expected.weighted_pages(),
            );
            let last = if system {
                self.after_system.as_ref()
            } else {
                self.after_data.as_ref()
            }
            .map(|key| (key.transaction_id, key.pagination_id));
            let mut after = None;
            let mut observed = (0_usize, 0_usize, 0_usize, 0_u64);
            for _ in 0..expected.0 {
                // Exact-prefix re-read consumes the REMAINING allowance of the
                // same budget. No reset, independent budget or hidden scan.
                let (head, visits, bytes, depth) = Self::peek(
                    if system { &self.system } else { &self.data },
                    after.as_ref(),
                    system,
                    self.actual.horizon,
                    &self.actual.epoch,
                    self.budget,
                )?;
                self.add_path(visits, bytes, depth)?;
                let super::Probe::Entry(head) = head else {
                    return Err(E::UnsupportedState);
                };
                if !last.is_some_and(|last| (head.transaction, head.pagination) <= last) {
                    return Err(E::Malformed);
                }
                observed.0 = observed.0.checked_add(1).ok_or(E::Overflow)?;
                observed.1 = observed.1.checked_add(head.count).ok_or(E::Overflow)?;
                observed.2 = observed.2.checked_add(head.bytes).ok_or(E::Overflow)?;
                observed.3 = observed
                    .3
                    .checked_add(head.weighted_pages)
                    .ok_or(E::Overflow)?;
                result.observe(system, head.key())?;
                after = Some(head.key());
            }
            if observed != expected
                || after
                    .as_ref()
                    .map(|key| (key.transaction_id, key.pagination_id))
                    != last
            {
                return Err(E::Malformed);
            }
        }
        self.actual.require_current()?;
        Ok(result)
    }
    pub(super) fn inspect_selected_recipe_frontiers(
        &mut self,
    ) -> PathResult<Option<NativeSelectedFreedFrontiers>> {
        self.actual.require_current()?;
        let retained_bytes = crate::tree_store::freed_point_selection_capacity_bytes::<
            (),
            (NativeSelectedFreedFrontiers, NativeFreedAppendPositions),
        >(0, 0)?;
        let allocation = self
            .actual
            .read
            .tree
            .read_page_resolver()
            .admit_fixed_observation(usize::try_from(retained_bytes).map_err(|_| E::Overflow)?)?;
        let buffers = self.collect_selected_recipe_buffers()?;
        let append_positions = self.inspect_append_positions()?;
        // One extra planning key probe; same original examination/byte owner.
        self.require_data_main_interval_absent(&append_positions)?;
        let inspect_gap = |tree: &Option<Btree<TransactionIdWithPagination, PageList<'static>>>,
                           start: &TransactionIdWithPagination| {
            let Some(actual) = tree.as_ref() else {
                return Ok(None);
            };
            let recipe =
                NativeFreedAppendGapRecipe::for_native_interval(actual, &self.actual.epoch, start)?;
            actual
                .prepare_freed_append_gap(&recipe, &self.actual.epoch)?
                .collect()
                .map(Some)
        };
        // None is actual namespace absence, sealed by this same inspected owner.
        let data_main_gap = inspect_gap(&self.data, append_positions.data_main())?;
        let system_post_gap = inspect_gap(&self.system, append_positions.system_post())?;
        let inspect = |family,
                       tree: &Option<Btree<TransactionIdWithPagination, PageList<'static>>>,
                       points: &[SelectedPoint]| {
            if points.is_empty() {
                return Ok(None);
            }
            let actual = tree.as_ref().ok_or(E::Malformed)?;
            let recipe = NativeFreedPointRecipe {
                family,
                actual,
                epoch: &self.actual.epoch,
                points,
            };
            let preparation = actual.prepare_freed_point_frontier(&recipe, &self.actual.epoch)?;
            preparation.collect_shape().map(Some)
        };
        let data = inspect(NativeFreedPointFamily::Data, &self.data, &buffers.data)?;
        let system = inspect(
            NativeFreedPointFamily::System,
            &self.system,
            &buffers.system,
        )?;
        self.actual.require_current()?;
        Ok(Some(NativeSelectedFreedFrontiers {
            data,
            system,
            append_positions,
            data_main_gap,
            system_post_gap,
            _allocation: allocation,
        }))
    }
    fn require_data_main_interval_absent(
        &mut self,
        positions: &NativeFreedAppendPositions,
    ) -> PathResult<()> {
        self.actual.require_current()?;
        if !self.budget.begin_probe() {
            return Err(E::UnsupportedState);
        }
        let main = positions.data_main();
        let upper = TransactionIdWithPagination {
            transaction_id: main.transaction_id.checked_add(1).ok_or(E::Overflow)?,
            pagination_id: 0,
        };
        let observation = if let Some(tree) = &self.data {
            let mut visits = crate::tree_store::CompiledPathVisits::for_freed_probe();
            let (entry, cost) =
                tree.read_native_freed_before(&upper, &self.actual.epoch, &mut visits)?;
            // Borrow the entry through its Option, then drop that ENTIRE
            // original Option before leaving this immutable-tree scope.
            // Only fixed scalars cross into the subsequent mutable self use.
            let collision = if let Some(entry) = entry.as_ref() {
                self.budget.observe(&entry.page_list())?;
                let collision = entry.key().transaction_id == main.transaction_id;
                entry.require_current()?;
                collision
            } else {
                false
            };
            drop(entry);
            Some((
                visits.visited(),
                cost.observed_page_bytes(),
                cost.depth(),
                collision,
            ))
        } else {
            None
        };
        if let Some((visits, bytes, depth, collision)) = observation {
            self.add_path(visits, bytes, depth)?;
            if collision {
                return Err(E::UnsupportedState);
            }
        }
        self.actual.require_current()?;
        Ok(())
    }
    fn inspect_append_positions(&mut self) -> PathResult<NativeFreedAppendPositions> {
        self.actual.require_current()?;
        let main = self.actual.next_write.raw_id();
        let post = main.checked_add(1).ok_or(E::Overflow)?;
        let mut positions = NativeFreedAppendPositions {
            data_main: TransactionIdWithPagination {
                transaction_id: main,
                pagination_id: 0,
            },
            system_main: TransactionIdWithPagination {
                transaction_id: main,
                pagination_id: 0,
            },
            system_post: TransactionIdWithPagination {
                transaction_id: post,
                pagination_id: 0,
            },
            path_visits: 0,
            path_bytes: 0,
            maximum_depth: 0,
        };
        for (transaction_id, target) in [
            (main, &mut positions.system_main),
            (post, &mut positions.system_post),
        ] {
            // Native SYSTEM insertion uses the last row in [txn, txn.next),
            // unlike DATA insertion which starts at pagination zero.
            let next_id = transaction_id.checked_add(1).ok_or(E::Overflow)?;
            if !self.budget.begin_probe() {
                return Err(E::UnsupportedState);
            }
            if let Some(tree) = &self.system {
                let before = TransactionIdWithPagination {
                    transaction_id: next_id,
                    pagination_id: 0,
                };
                let mut visits = crate::tree_store::CompiledPathVisits::for_freed_probe();
                let (entry, cost) =
                    tree.read_native_freed_before(&before, &self.actual.epoch, &mut visits)?;
                positions.path_visits = positions
                    .path_visits
                    .checked_add(visits.visited())
                    .ok_or(E::Overflow)?;
                positions.path_bytes = positions
                    .path_bytes
                    .checked_add(cost.observed_page_bytes())
                    .ok_or(E::Overflow)?;
                positions.maximum_depth = positions.maximum_depth.max(cost.depth());
                if let Some(entry) = entry {
                    if entry.key().transaction_id == transaction_id {
                        self.budget.observe(&entry.page_list())?;
                        target.pagination_id = entry
                            .key()
                            .pagination_id
                            .checked_add(1)
                            .ok_or(E::Overflow)?;
                    } else if entry.key().transaction_id > transaction_id {
                        return Err(E::Malformed);
                    }
                    entry.require_current()?;
                }
            }
        }
        self.actual.require_current()?;
        Ok(positions)
    }
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_freed_point_recipe_tests.rs"]
mod tests;

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_freed_append_gap_tests.rs"]
mod append_gap_tests;
