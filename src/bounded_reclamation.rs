//! Native whole-entry cleanup accounting. This private primitive does not
//! authorize a planned transaction or provide its complete native memory quote.
use super::{
    DATA_FREED_TABLE, PageList, PageNumber, Result, SYSTEM_FREED_TABLE, StorageError,
    SystemNamespace, TransactionId, TransactionIdWithPagination,
};
#[cfg(test)]
use super::{PageTracker, WriteTransaction};
use alloc::string::ToString;
use core::mem::size_of;

const KEY_BYTES: usize = 2 * size_of::<u64>();

#[derive(Clone, Copy)]
pub(super) struct ReclamationLimits {
    removed: usize,
    examined: usize,
    page_numbers: usize,
    encoded_bytes: usize,
    max_entry_page_numbers: usize,
    max_entry_encoded_bytes: usize,
}

pub(crate) struct ReclamationBudget {
    remaining: ReclamationLimits,
    prepared: Option<NativeReclamationFence>,
    #[cfg(not(redb_no_std))]
    pub(super) allocated: Option<super::native_system_resource_paths::NativeAllocatedPrefix>,
    #[cfg(not(redb_no_std))]
    completed_storage: Option<[Option<crate::tree_store::NativeOwnedTxnIds>; 2]>,
}

impl ReclamationBudget {
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn inspect_entry_for_quote_test(pages: &PageList<'_>) -> Result<(usize, usize)> {
        let mut budget = Self::for_emitted_entries(1);
        assert!(budget.begin_probe());
        budget.observe(pages)
    }

    #[cfg(test)]
    pub(super) fn for_emitted_entries(removed: usize) -> Self {
        // Derived from the actual 4.4 emitter, not a production/default policy:
        // DATA reserves 400 numbers; SYSTEM reserves 200; every key is 16 bytes.
        let entry = KEY_BYTES + PageList::required_bytes(400);
        // At most two family heads + one exact extraction per removal, two
        // remaining-family probes per touched ID, and two terminal head probes.
        // All callers are fixed small test cases; a production constructor must
        // checked-validate limits together with its complete native memory plan.
        // A complete zero-service profile still inspects TWO native SYSTEM
        // append positions and TWO original family tails. These are planning
        // probes, never additional removal/emission allowance. The prior
        // fixture envelope was 4 at K0 and 5*K+2 for K>=1. This sequence adds
        // ONE actual DATA-main collision probe: 5 at K0 and 5*K+3 for K>=1,
        // charged to this SAME planning envelope. Service/PN limits unchanged.
        let examinations = if removed == 0 { 5 } else { 5 * removed + 3 };
        Self {
            remaining: ReclamationLimits {
                removed,
                examined: examinations,
                page_numbers: 400 * removed,
                encoded_bytes: examinations * entry,
                max_entry_page_numbers: 400,
                max_entry_encoded_bytes: entry,
            },
            prepared: None,
            #[cfg(not(redb_no_std))]
            allocated: None,
            #[cfg(not(redb_no_std))]
            completed_storage: None,
        }
    }

    // Test-only pressure on the SAME actual planning envelope; no production
    // limit constructor or replacement budget is exposed.
    #[cfg(test)]
    pub(super) fn limit_planning_for_test(&mut self, examined: usize, encoded_bytes: usize) {
        self.remaining.examined = examined;
        self.remaining.encoded_bytes = encoded_bytes;
    }

    #[cfg(test)]
    pub(super) fn limit_page_numbers_for_test(&mut self, page_numbers: usize) {
        self.remaining.page_numbers = page_numbers;
    }

    pub(super) fn observed_limits(&self) -> ReclamationLimits {
        self.remaining
    }
    pub(super) fn take_prepared_observation(
        &mut self,
        limits: ReclamationLimits,
        prepared: NativeReclamationFence,
    ) -> Self {
        // Simulation spent scalar observation counters; execution keeps its
        // original limits/fence and moves the SAME preadmitted physical buffers.
        // Only the actual finishing observer calls this; there is no getter,
        // allocation, pool substitution or detached buffer reconstruction.
        Self {
            remaining: limits,
            prepared: Some(prepared),
            #[cfg(not(redb_no_std))]
            allocated: self.allocated.take(),
            #[cfg(not(redb_no_std))]
            completed_storage: self.completed_storage.take(),
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn require_allocated_prefix_buffers(
        &self,
        resolver: &crate::tree_store::PageResolver,
    ) -> Result<()> {
        let storage = self
            .completed_storage
            .as_ref()
            .ok_or_else(crate::tree_store::source_owner_refused)?;
        for slot in storage {
            let buffer = slot
                .as_ref()
                .ok_or_else(crate::tree_store::source_owner_refused)?;
            resolver.require_fixed_observation_completed_buffer(buffer, self.remaining.removed)?;
        }
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn require_completed_storage(
        &self,
        resources: &crate::tree_store::NativePageResources,
        maximum_removed_entries: usize,
    ) -> Result<()> {
        if self.remaining.removed != maximum_removed_entries {
            return Err(crate::tree_store::source_owner_refused());
        }
        let storage = self
            .completed_storage
            .as_ref()
            .ok_or_else(crate::tree_store::source_owner_refused)?;
        for slot in storage {
            slot.as_ref()
                .ok_or_else(crate::tree_store::source_owner_refused)?
                .require_for(maximum_removed_entries, resources)?;
        }
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn adopt_completed_storage(
        &mut self,
        from: &mut Option<[crate::tree_store::NativeOwnedTxnIds; 2]>,
        resources: &crate::tree_store::NativePageResources,
        maximum_removed_entries: usize,
    ) -> Result<()> {
        if self.completed_storage.is_some() || self.remaining.removed != maximum_removed_entries {
            return Err(crate::tree_store::source_owner_refused());
        }
        for buffer in from
            .as_ref()
            .ok_or_else(crate::tree_store::source_owner_refused)?
        {
            buffer.require_for(maximum_removed_entries, resources)?;
        }
        // All fallible checks precede moving the actual pair. The local owners
        // cannot change concurrently; no allocation or fresh pair is needed.
        let [first, second] = from.take().expect("checked sole completion-pair owner");
        self.completed_storage = Some([Some(first), Some(second)]);
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn install_completed_storage(
        &mut self,
        storage: [crate::tree_store::NativeOwnedTxnIds; 2],
        resources: &crate::tree_store::NativePageResources,
    ) -> Result<()> {
        if self.completed_storage.is_some() {
            return Err(crate::tree_store::source_owner_refused());
        }
        // One ID per removed whole entry is a direct bound on BOTH passes,
        // with duplicate IDs deduplicated while maintaining existing order.
        for buffer in &storage {
            buffer.require_for(self.remaining.removed, resources)?;
        }
        let [first, second] = storage;
        self.completed_storage = Some([Some(first), Some(second)]);
        Ok(())
    }
    fn take_completed_storage(&mut self) -> Result<crate::tree_store::NativeOwnedTxnIds> {
        #[cfg(not(redb_no_std))]
        if let Some(storage) = &mut self.completed_storage {
            for slot in storage {
                if let Some(buffer) = slot.take() {
                    return Ok(buffer);
                }
            }
            // A third pass was never part of the native fixed recipe. Refuse
            // BEFORE any page/table mutation; no ad-hoc late allocation.
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(crate::tree_store::NativeOwnedTxnIds::default())
    }
    fn effective_horizon(&self, current: TransactionId) -> TransactionId {
        let current = self
            .prepared
            .as_ref()
            .map_or(current, |fence| current.min(fence.horizon));
        // Preserve H itself. The cap changes only after the exact historical
        // prefix has been removed in the main transaction, never while planning.
        #[cfg(not(redb_no_std))]
        let current = self
            .allocated
            .as_ref()
            .map_or(current, |prefix| prefix.effective_horizon(current));
        current
    }
    fn prepared_last(&self, system: bool) -> Option<Option<TransactionIdWithPagination>> {
        self.prepared.as_ref().map(|fence| {
            let last = if system {
                &fence.system_last
            } else {
                &fence.data_last
            };
            last.as_ref().map(copy_key)
        })
    }
    fn verify_completion_record(
        &self,
        system: bool,
        key: &TransactionIdWithPagination,
        count: usize,
        bytes: usize,
    ) -> Result<()> {
        let Some(fence) = self.prepared.as_ref() else {
            return Ok(());
        };
        let (last, tail) = if system {
            (&fence.system_last, &fence.system_tail)
        } else {
            (&fence.data_last, &fence.data_tail)
        };
        if last
            .as_ref()
            .is_some_and(|last| key_order(key) <= key_order(last))
        {
            return Ok(());
        }
        if let NativeReclamationTail::Entry {
            key: tail_key,
            count: tail_count,
            bytes: tail_bytes,
        } = tail
            && key_order(key) == key_order(tail_key)
            && count == *tail_count
            && bytes == *tail_bytes
        {
            return Ok(());
        }
        Err(StorageError::Corrupted(
            "Uninspected native reclamation completion key".to_string(),
        ))
    }

    pub(super) fn begin_probe(&mut self) -> bool {
        // Reserve enough numerical allowance BEFORE examining another entry.
        // The separate native resource plan must also bound the physical page
        // read/path allocation that can precede learning this value's length.
        if self.remaining.examined == 0
            || self.remaining.encoded_bytes < self.remaining.max_entry_encoded_bytes
        {
            return false;
        }
        self.remaining.examined -= 1;
        true
    }

    pub(super) fn observe(&mut self, pages: &PageList<'_>) -> Result<(usize, usize)> {
        let bytes = KEY_BYTES
            .checked_add(pages.data.len())
            .ok_or(StorageError::ValueTooLarge(pages.data.len()))?;
        if bytes > self.remaining.max_entry_encoded_bytes {
            return Err(StorageError::ValueTooLarge(bytes));
        }
        if pages.data.len() < size_of::<u16>() {
            return Err(StorageError::Corrupted(
                "Truncated freed-page count".to_string(),
            ));
        }
        let count = pages.len();
        if pages.data.len() < PageList::required_bytes(count) {
            return Err(StorageError::Corrupted(
                "Truncated freed-page list".to_string(),
            ));
        }
        if count > self.remaining.max_entry_page_numbers {
            return Err(StorageError::ValueTooLarge(bytes));
        }
        // begin_probe already proved the complete supported capacity fits.
        self.remaining.encoded_bytes -= bytes;
        Ok((count, bytes))
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn detachment_state_for_test(
        &self,
    ) -> (Option<TransactionId>, (usize, usize, usize, usize), usize) {
        (
            self.prepared.as_ref().map(|fence| fence.horizon),
            self.remaining_for_observation_test(),
            self.completed_storage.as_ref().map_or(0, |buffers| {
                buffers.iter().filter(|buffer| buffer.is_some()).count()
            }),
        )
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn fixed_phase_for_test(
        &self,
        current: TransactionId,
        removed: usize,
        writer: TransactionId,
    ) -> [u64; 10] {
        let (horizon, remaining, completed) = self.detachment_state_for_test();
        [
            1,
            horizon.expect("prepared fixed budget").raw_id(),
            self.effective_horizon(current).raw_id(),
            remaining.0 as u64,
            remaining.1 as u64,
            remaining.2 as u64,
            remaining.3 as u64,
            completed as u64,
            removed as u64,
            writer.raw_id(),
        ]
    }

    #[cfg(test)]
    pub(super) fn remaining_for_observation_test(&self) -> (usize, usize, usize, usize) {
        (
            self.remaining.removed,
            self.remaining.examined,
            self.remaining.page_numbers,
            self.remaining.encoded_bytes,
        )
    }

    pub(super) fn has_removal_slot(&self) -> bool {
        self.remaining.removed != 0
    }

    pub(super) fn can_remove(&self, count: usize) -> bool {
        self.remaining.removed != 0 && self.remaining.page_numbers >= count
    }

    pub(super) fn remove(&mut self, count: usize) {
        self.remaining.removed -= 1;
        self.remaining.page_numbers -= count;
    }
}

// These facts are minted only from actual current SYSTEM observations. They
// contain no caller root/guard, Source capability or complete memory admission.
pub(super) enum NativeReclamationTail {
    Empty,
    Horizon,
    Entry {
        key: TransactionIdWithPagination,
        count: usize,
        bytes: usize,
    },
}
pub(super) struct NativeReclamationFence {
    horizon: TransactionId,
    data_last: Option<TransactionIdWithPagination>,
    system_last: Option<TransactionIdWithPagination>,
    data_tail: NativeReclamationTail,
    system_tail: NativeReclamationTail,
}
impl NativeReclamationFence {
    pub(super) fn from_observed_prefix(
        horizon: TransactionId,
        data_last: Option<&TransactionIdWithPagination>,
        system_last: Option<&TransactionIdWithPagination>,
        data_tail: NativeReclamationTail,
        system_tail: NativeReclamationTail,
    ) -> Self {
        Self {
            horizon,
            data_last: data_last.map(copy_key),
            system_last: system_last.map(copy_key),
            data_tail,
            system_tail,
        }
    }
}
fn copy_key(key: &TransactionIdWithPagination) -> TransactionIdWithPagination {
    TransactionIdWithPagination {
        transaction_id: key.transaction_id,
        pagination_id: key.pagination_id,
    }
}
fn key_order(key: &TransactionIdWithPagination) -> (u64, u64) {
    (key.transaction_id, key.pagination_id)
}

#[derive(Clone, Copy)]
struct Head {
    transaction: u64,
    pagination: u64,
    count: usize,
    bytes: usize,
    system: bool,
}
impl Head {
    fn key(self) -> TransactionIdWithPagination {
        TransactionIdWithPagination {
            transaction_id: self.transaction,
            pagination_id: self.pagination,
        }
    }
    fn order(self) -> (u64, u64, bool) {
        (self.transaction, self.pagination, self.system)
    }
}
enum Probe {
    Exhausted,
    Empty,
    Entry(Head),
}

pub(super) struct Extraction {
    pub(super) completed: crate::tree_store::NativeOwnedTxnIds,
    pub(super) removed: usize,
}

fn peek(
    tables: &mut SystemNamespace,
    system: bool,
    free_until: TransactionId,
    budget: &mut ReclamationBudget,
) -> Result<Probe> {
    if !budget.begin_probe() {
        return Ok(Probe::Exhausted);
    }
    let definition = if system {
        SYSTEM_FREED_TABLE
    } else {
        DATA_FREED_TABLE
    };
    if tables.get_system_table_root(definition)?.is_none() {
        return Ok(Probe::Empty);
    }
    let table = tables.open_system_table(definition)?;
    let end = TransactionIdWithPagination {
        transaction_id: free_until.raw_id(),
        pagination_id: 0,
    };
    let mut range = match budget.prepared_last(system) {
        None => table.range(..end)?,
        Some(Some(last)) => table.range(..=last)?,
        Some(None) => return Ok(Probe::Empty),
    };
    let Some((key, value)) = range.next().transpose()? else {
        return Ok(Probe::Empty);
    };
    let key = key.value();
    if key.transaction_id >= free_until.raw_id() {
        return Ok(Probe::Empty);
    }
    let (count, bytes) = budget.observe(&value.value())?;
    Ok(Probe::Entry(Head {
        transaction: key.transaction_id,
        pagination: key.pagination_id,
        count,
        bytes,
        system,
    }))
}

fn remaining_for_id(
    tables: &mut SystemNamespace,
    system: bool,
    id: TransactionId,
    budget: &mut ReclamationBudget,
) -> Result<bool> {
    if !budget.begin_probe() {
        return Ok(true); // Incomplete observation cannot mark an ID complete.
    }
    let definition = if system {
        SYSTEM_FREED_TABLE
    } else {
        DATA_FREED_TABLE
    };
    if tables.get_system_table_root(definition)?.is_none() {
        return Ok(false);
    }
    let table = tables.open_system_table(definition)?;
    let first = TransactionIdWithPagination {
        transaction_id: id.raw_id(),
        pagination_id: 0,
    };
    let last = TransactionIdWithPagination {
        transaction_id: id.raw_id(),
        pagination_id: u64::MAX,
    };
    let mut range = table.range(first..=last)?;
    if let Some((key, value)) = range.next().transpose()? {
        let (count, bytes) = budget.observe(&value.value())?;
        budget.verify_completion_record(system, &key.value(), count, bytes)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Merge exact oldest heads, never scanning rejected entries. The single
/// supplied budget persists through both families and the postcommit pass.
pub(super) fn extract(
    tables: &mut SystemNamespace,
    free_until: TransactionId,
    include_system: bool,
    budget: &mut ReclamationBudget,
    mut process_page: impl FnMut(PageNumber),
) -> Result<Extraction> {
    // The current floor still protects every newly pinned reader. A release
    // cannot expand this planned turn beyond its prepared eligible horizon.
    let free_until = budget.effective_horizon(free_until);
    let before = budget.remaining.removed;
    let mut touched = budget.take_completed_storage()?;
    while budget.remaining.removed != 0 {
        let data = peek(tables, false, free_until, budget)?;
        let system = if include_system {
            peek(tables, true, free_until, budget)?
        } else {
            Probe::Empty
        };
        let selected = match (data, system) {
            (Probe::Exhausted, _) | (_, Probe::Exhausted) | (Probe::Empty, Probe::Empty) => break,
            (Probe::Entry(one), Probe::Empty) | (Probe::Empty, Probe::Entry(one)) => one,
            (Probe::Entry(one), Probe::Entry(two)) => {
                if one.order() <= two.order() {
                    one
                } else {
                    two
                }
            }
        };
        if !budget.can_remove(selected.count) || !budget.begin_probe() {
            break;
        }
        let definition = if selected.system {
            SYSTEM_FREED_TABLE
        } else {
            DATA_FREED_TABLE
        };
        let mut table = tables.open_system_table(definition)?;
        // A single exact key range leaves all unread entries untouched. No
        // predicate-false scan or partial PageList freeing is possible.
        let mut extracted = table.extract_from_if(selected.key()..=selected.key(), |_, _| true)?;
        let Some((key, value)) = extracted.next().transpose()? else {
            return Err(StorageError::Corrupted(
                "Freed entry disappeared".to_string(),
            ));
        };
        let key = key.value();
        let pages = value.value();
        let (count, bytes) = budget.observe(&pages)?;
        if key.transaction_id != selected.transaction
            || key.pagination_id != selected.pagination
            || count != selected.count
            || bytes != selected.bytes
        {
            return Err(StorageError::Corrupted("Freed entry changed".to_string()));
        }
        budget.remove(count);
        for index in 0..count {
            process_page(pages.get(index));
        }
        drop(value);
        extracted.close()?;
        let id = TransactionId::new(selected.transaction);
        if touched.last().copied() != Some(id) {
            touched.push(id);
        }
    }
    let mut complete_count = 0;
    for index in 0..touched.len() {
        let id = touched[index];
        if !remaining_for_id(tables, false, id, budget)?
            && !remaining_for_id(tables, true, id, budget)?
        {
            touched[complete_count] = id;
            complete_count += 1;
        }
    }
    touched.truncate(complete_count);
    Ok(Extraction {
        completed: touched,
        removed: before - budget.remaining.removed,
    })
}

#[cfg(test)]
#[path = "bounded_reclamation_unit_tests.rs"]
mod tests;
