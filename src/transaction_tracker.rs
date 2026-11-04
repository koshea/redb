use crate::StorageError;
use crate::sync::{Condvar, Mutex};
use crate::tree_store::{BtreeHeader, HeaderGuard, TransactionalMemory, WriterLock};
use crate::{Key, Result, TypeName, Value};
use alloc::collections::BTreeSet;
use alloc::collections::btree_map::BTreeMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::mem;
use core::mem::size_of;
#[cfg(feature = "logging")]
use log::{debug, error};

#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct TransactionId(u64);

impl TransactionId {
    pub(crate) fn new(value: u64) -> TransactionId {
        Self(value)
    }

    pub(crate) fn raw_id(self) -> u64 {
        self.0
    }

    pub(crate) fn next(self) -> TransactionId {
        TransactionId(self.0 + 1)
    }

    pub(crate) fn increment(&mut self) -> TransactionId {
        let next = self.next();
        *self = next;
        next
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SavepointId(pub u64);

// Unique within one tracker, including across aborted transactions and peer ID reuse.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct LocalSavepointId(u64);

struct SavepointRegistration {
    local_id: LocalSavepointId,
    transaction_id: TransactionId,
}

impl SavepointId {
    pub(crate) fn next(self) -> SavepointId {
        SavepointId(self.0 + 1)
    }
}

impl crate::types::NativeResourceTypeName for SavepointId {
    const INTERNAL_RESOURCE_NAME: &'static str = "redb::SavepointId";
}

impl Value for SavepointId {
    type SelfType<'a> = SavepointId;
    type AsBytes<'a> = [u8; size_of::<u64>()];

    fn fixed_width() -> Option<usize> {
        Some(size_of::<u64>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        SavepointId(u64::from_le_bytes(data.try_into().unwrap()))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal(<Self as crate::types::NativeResourceTypeName>::INTERNAL_RESOURCE_NAME)
    }
}

impl Key for SavepointId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).0.cmp(&Self::from_bytes(data2).0)
    }
}

// The write slot's state. It is taken ahead of the writer byte, and given its transaction's id
// once the writer byte is held and the file's latest commit is known.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum WriteSlotState {
    Free,
    // Bounded SYSTEM inspection excludes ordinary native writers but issues
    // no transaction ID and changes no current-root resource generation.
    Inspecting(u64),
    // The slot is taken and the writer byte is being acquired, which waits on any peer that
    // holds it. There is no transaction id yet: it is issued from the file's latest commit,
    // which is only known once the byte is held
    Initializing,
    // A live transaction guarantees that this process holds the writer byte
    Live(TransactionId),
    // The transaction is releasing its writer-lock reference while still holding the slot.
    // Another owner of the lock may keep the writer byte held after the transaction ends.
    Finalizing,
}

#[cfg(all(test, not(redb_no_std)))]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PlannedReadTrackerSnapshot {
    pub(crate) ordinary_entries: usize,
    pub(crate) ordinary_references: u64,
    pub(crate) ordinary_at_slot: u64,
    pub(crate) planned_id: Option<TransactionId>,
    pub(crate) planned_serial: Option<u64>,
    pub(crate) oldest: Option<TransactionId>,
    pub(crate) planned_matches_pending: bool,
}
#[cfg(all(test, not(redb_no_std)))]
pub(crate) struct ReclamationTrackerSnapshot {
    pub(crate) savepoints: usize,
    pub(crate) persistent_savepoints: usize,
    pub(crate) pending: usize,
    pub(crate) first_pending: Option<(TransactionId, TransactionId)>,
    pub(crate) unprocessed: usize,
}

// Private planning state, never a durable/root or complete resource certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeResourcePlanningRefusal {
    Busy,
    Stale,
    Unsupported,
    Exhausted,
    Poisoned,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct NativeTrackerProfile {
    single_pending: Option<(TransactionId, TransactionId)>,
    unprocessed_freed_ids: usize,
}

// The outer native plan must additionally borrow its actual Database and own
// admitted native read/page pins. This epoch alone authorizes no physical read.
pub(crate) struct NativeResourceEpoch<'tracker> {
    actual: &'tracker TransactionTracker,
    generation: u64,
    profile: NativeTrackerProfile,
    inspection_serial: Option<u64>,
}

impl NativeResourceEpoch<'_> {
    // Pure cfgtest comparison of two genuine native epoch captures. No raw
    // stamp is exposed or accepted, and no generation can be installed.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn same_generation_for_test(&self, other: &Self) -> bool {
        core::ptr::eq(self.actual, other.actual) && self.generation == other.generation
    }

    // SYSTEM pages need this real inspection exclusion, never USER MVCC alone.
    pub(crate) fn require_system_inspection_for(
        &self,
        tracker: &Arc<TransactionTracker>,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        if self.inspection_serial.is_none() {
            return Err(NativeResourcePlanningRefusal::Unsupported);
        }
        self.require_tracker(tracker)
    }
    pub(crate) fn require_tracker(
        &self,
        actual: &TransactionTracker,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        if !core::ptr::eq(self.actual, actual) {
            return Err(NativeResourcePlanningRefusal::Stale);
        }
        self.require_current()
    }

    // Numerical proposals from the actual tracker state. The owning system
    // read supplies its matching committed read ID; no mutation is authorized.
    pub(crate) fn observe_native_write_counters(
        &self,
        committed: TransactionId,
    ) -> core::result::Result<(TransactionId, TransactionId), NativeResourcePlanningRefusal> {
        let state = self
            .actual
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        self.actual.require_resource_epoch(&state, self)?;
        let next = state
            .next_transaction_id
            .max(committed)
            .raw_id()
            .checked_add(1)
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        // The actual current read remains live, so a local floor must exist.
        // ExclusiveWriter is checked by the system-read owner: no peer scan.
        let oldest = state
            .oldest_read_reference()
            .ok_or(NativeResourcePlanningRefusal::Unsupported)?;
        let horizon = oldest
            .raw_id()
            .checked_add(1)
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        Ok((TransactionId::new(next), TransactionId::new(horizon)))
    }

    pub(crate) fn single_pending(&self) -> Option<(TransactionId, TransactionId)> {
        self.profile.single_pending
    }

    pub(crate) fn unprocessed_freed_ids(&self) -> usize {
        self.profile.unprocessed_freed_ids
    }

    pub(crate) fn require_current(
        &self,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        let state = self
            .actual
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        self.actual.require_resource_epoch(&state, self)
    }
}

// A numerical seal borrowing the actual tracker, not the movable epoch value.
// It cannot claim a writer or mint an actual read; only the original epoch can.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeDetachedResourceEpoch<'tracker> {
    actual: &'tracker TransactionTracker,
    generation: u64,
    profile: NativeTrackerProfile,
}
#[cfg(not(redb_no_std))]
impl<'tracker> NativeResourceEpoch<'tracker> {
    pub(crate) fn seal_detached(
        &self,
    ) -> core::result::Result<NativeDetachedResourceEpoch<'tracker>, NativeResourcePlanningRefusal>
    {
        if self.inspection_serial.is_some() {
            return Err(NativeResourcePlanningRefusal::Unsupported);
        }
        self.require_current()?;
        Ok(NativeDetachedResourceEpoch {
            actual: self.actual,
            generation: self.generation,
            profile: self.profile,
        })
    }
}
#[cfg(not(redb_no_std))]
impl NativeDetachedResourceEpoch<'_> {
    pub(crate) fn require_writer(
        &self,
        actual: &TransactionTracker,
        id: TransactionId,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        let state = actual
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        actual.require_prepared_writer_parts(&state, self.actual, self.generation, self.profile, id)
    }

    pub(crate) fn require_for(
        &self,
        actual: &TransactionTracker,
        committed: TransactionId,
        expected_next: TransactionId,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        let state = actual
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        actual.require_resource_parts(&state, self.actual, self.generation, self.profile, None)?;
        let next = state
            .next_transaction_id
            .max(committed)
            .raw_id()
            .checked_add(1)
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        if TransactionId::new(next) != expected_next {
            return Err(NativeResourcePlanningRefusal::Stale);
        }
        // Preserve the already prepared horizon; no live read floor is needed.
        Ok(())
    }
}

// Only try_claim_resource_write_slot mints this value, and immediately transfers
// it into the existing WriteSlot. No successful bare slot claim escapes.
pub(crate) struct NativeResourceSlotClaim {
    tracker: Arc<TransactionTracker>,
}

impl NativeResourceSlotClaim {
    pub(crate) fn into_tracker(self) -> Arc<TransactionTracker> {
        self.tracker
    }
}

pub(crate) struct NativeSystemInspectionClaim {
    tracker: Arc<TransactionTracker>,
    serial: u64,
}
impl NativeSystemInspectionClaim {
    pub(crate) fn actual_tracker(&self) -> &Arc<TransactionTracker> {
        &self.tracker
    }
}

// One optional numerical-read pin, allocated inline with the actual tracker.
// It is independent of ordinary and savepoint map references at the same ID.
#[derive(Clone, Copy)]
struct PlannedReadSlot {
    transaction_id: TransactionId,
    serial: u64,
    actual_memory: usize,
}
#[cfg(not(redb_no_std))]
pub(crate) struct NativePlannedReadSnapshot<'owner> {
    actual_tracker: &'owner TransactionTracker,
    actual_memory: &'owner TransactionalMemory,
    transaction_id: TransactionId,
    root: Option<BtreeHeader>,
    generation: u64,
}
#[cfg(not(redb_no_std))]
#[derive(Debug)]
pub(crate) enum NativePlannedReadError {
    Resource(NativeResourcePlanningRefusal),
    Storage(StorageError),
}
#[cfg(not(redb_no_std))]
impl From<StorageError> for NativePlannedReadError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
// Only the closed Source Immediate executor may install/register this tail.
// Its internal ancestor is independent of every ordinary/planned read at that ID.
#[cfg(not(redb_no_std))]
#[derive(Clone, Copy)]
struct SourcePendingTail {
    id: TransactionId,
    durable_ancestor: TransactionId,
}
#[cfg(not(redb_no_std))]
#[derive(Clone, Copy)]
struct SourceTailPreparationStamp {
    actual_memory: usize,
    writer_id: TransactionId,
    generation: u64,
}

struct State {
    next_savepoint_id: SavepointId,
    next_local_savepoint_id: LocalSavepointId,
    // reference count of read transactions per transaction id
    live_read_transactions: BTreeMap<TransactionId, u64>,
    planned_read_slot: Option<PlannedReadSlot>,
    next_planned_read_serial: Option<u64>,
    // Subset of live_read_transactions which are persistent savepoints
    persistent_savepoint_references: BTreeMap<TransactionId, u64>,
    next_transaction_id: TransactionId,
    write_slot: WriteSlotState,
    // Changes before any ordinary write initialization, including aborted turns.
    // Exhaustion disables only optional resource planning, never ordinary writes.
    resource_generation: Option<u64>,
    next_inspection_serial: Option<u64>,
    valid_savepoints: BTreeMap<SavepointId, SavepointRegistration>,
    // Subset of valid_savepoints that are persistent
    persistent_savepoints: BTreeSet<SavepointId>,
    // Non-durable commits that are still in-memory, and waiting for a durable commit to get flushed
    // We need to make sure that the freed-table does not get processed for these, since they are not durable yet
    // Therefore, we hold a read transaction on their nearest durable ancestor
    //
    // Maps non-durable transaction id -> durable ancestor
    pending_non_durable_commits: BTreeMap<TransactionId, TransactionId>,
    #[cfg(not(redb_no_std))]
    source_pending_tail: Option<SourcePendingTail>,
    #[cfg(not(redb_no_std))]
    source_tail_preparation: Option<SourceTailPreparationStamp>,
    #[cfg(not(redb_no_std))]
    source_freed_markers: Option<crate::tree_store::NativeOwnedTransactionMarkers>,
    // Non-durable commits which have NOT been processed in the freed table
    unprocessed_freed_non_durable_commits: BTreeSet<TransactionId>,
    // Set when the Database was dropped while a write transaction was live. That transaction
    // keeps the database open, and end_write_transaction() hands the close back to it when
    // it ends
    deferred_close: Option<Arc<TransactionalMemory>>,
}

impl State {
    fn source_ancestor_reference(&self) -> Option<TransactionId> {
        #[cfg(not(redb_no_std))]
        if let Some(tail) = self.source_pending_tail {
            return Some(tail.durable_ancestor);
        }
        None
    }
    fn pending_contains(&self, id: TransactionId) -> bool {
        #[cfg(not(redb_no_std))]
        if self.source_pending_tail.is_some_and(|tail| tail.id == id) {
            return true;
        }
        self.pending_non_durable_commits.contains_key(&id)
    }
    fn pending_count(&self) -> usize {
        let count = self.pending_non_durable_commits.len();
        #[cfg(not(redb_no_std))]
        let count = count
            .checked_add(usize::from(self.source_pending_tail.is_some()))
            .expect("native pending owner count fits its address space");
        count
    }
    fn first_pending(&self) -> Option<(TransactionId, TransactionId)> {
        let ordinary = self
            .pending_non_durable_commits
            .first_key_value()
            .map(|(&id, &ancestor)| (id, ancestor));
        #[cfg(not(redb_no_std))]
        if let Some(tail) = self.source_pending_tail {
            let source = (tail.id, tail.durable_ancestor);
            return Some(ordinary.map_or(
                source,
                |value| if value.0 < source.0 { value } else { source },
            ));
        }
        ordinary
    }
    fn source_unprocessed_first(&self) -> Option<TransactionId> {
        #[cfg(not(redb_no_std))]
        if let Some(markers) = &self.source_freed_markers {
            return markers.first();
        }
        None
    }
    fn unprocessed_contains(&self, id: TransactionId) -> bool {
        #[cfg(not(redb_no_std))]
        if self
            .source_freed_markers
            .as_ref()
            .is_some_and(|markers| markers.contains(id))
        {
            return true;
        }
        self.unprocessed_freed_non_durable_commits.contains(&id)
    }
    fn unprocessed_count(&self) -> usize {
        let count = self.unprocessed_freed_non_durable_commits.len();
        #[cfg(not(redb_no_std))]
        let count = count
            .checked_add(
                self.source_freed_markers
                    .as_ref()
                    .map_or(0, |markers| markers.len()),
            )
            .expect("native freed marker count fits its address space");
        count
    }
    fn oldest_read_reference(&self) -> Option<TransactionId> {
        let ordinary = self.live_read_transactions.keys().next().copied();
        let ordinary = match (ordinary, self.source_ancestor_reference()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        match self.planned_read_slot {
            Some(slot) => {
                Some(ordinary.map_or(slot.transaction_id, |id| id.min(slot.transaction_id)))
            }
            None => ordinary,
        }
    }
    fn advance_resource_generation(&mut self) {
        self.resource_generation = self
            .resource_generation
            .and_then(|value| value.checked_add(1));
    }

    fn resource_profile(
        &self,
    ) -> core::result::Result<NativeTrackerProfile, NativeResourcePlanningRefusal> {
        if !self.valid_savepoints.is_empty()
            || !self.persistent_savepoints.is_empty()
            || !self.persistent_savepoint_references.is_empty()
            || self.pending_count() > 1
        {
            return Err(NativeResourcePlanningRefusal::Unsupported);
        }
        Ok(NativeTrackerProfile {
            single_pending: self.first_pending(),
            unprocessed_freed_ids: self.unprocessed_count(),
        })
    }

    fn register_savepoint(
        &mut self,
        id: SavepointId,
        transaction_id: TransactionId,
    ) -> LocalSavepointId {
        let local_id = self.next_local_savepoint_id;
        self.next_local_savepoint_id = LocalSavepointId(local_id.0.checked_add(1).unwrap());
        let registration = SavepointRegistration {
            local_id,
            transaction_id,
        };
        assert!(self.valid_savepoints.insert(id, registration).is_none());
        local_id
    }

    // The references to `id` that are reads, which the "active transaction byte" announces to the
    // other processes. Persistent savepoints are excluded
    fn active_transaction_lock_references(&self, id: TransactionId) -> u64 {
        let references = self.live_read_transactions.get(&id).copied().unwrap_or(0);
        let savepoints = self
            .persistent_savepoint_references
            .get(&id)
            .copied()
            .unwrap_or(0);
        references
            .checked_sub(savepoints)
            .and_then(|count| {
                count.checked_add(u64::from(
                    self.planned_read_slot
                        .is_some_and(|slot| slot.transaction_id == id),
                ))
            })
            .and_then(|count| {
                count.checked_add(u64::from(self.source_ancestor_reference() == Some(id)))
            })
            .expect("native read reference counts are internally consistent")
    }

    // Takes the "active transaction byte" as the first read of `id` appears, and releases it
    // as the last goes away, so a writer in another process sees exactly the transactions this one
    // is still reading. Done under the lock that holds the count, so the two cannot disagree: a
    // reference taken between the count reaching zero and the byte being released would otherwise
    // read a snapshot nothing protects
    fn add_active_transaction_lock_reference(
        &mut self,
        mem: &TransactionalMemory,
        id: TransactionId,
        header: &HeaderGuard<'_>,
    ) -> Result {
        *self.live_read_transactions.entry(id).or_insert(0) += 1;
        if self.active_transaction_lock_references(id) == 1
            && let Err(err) = mem.lock_mp_transaction(id, header)
        {
            self.decrement_reference_count(id);
            return Err(err);
        }

        Ok(())
    }

    fn remove_active_transaction_lock_reference(
        &mut self,
        mem: &TransactionalMemory,
        id: TransactionId,
    ) {
        self.decrement_reference_count(id);
        if self.active_transaction_lock_references(id) == 0 {
            Self::release_active_transaction_lock(mem, id);
        }
    }

    fn release_active_transaction_lock(mem: &TransactionalMemory, id: TransactionId) {
        if let Err(failure) = mem.unlock_mp_transaction(id) {
            if matches!(failure, StorageError::DatabaseClosed) {
                return;
            }
            #[cfg(feature = "logging")]
            error!(
                "Failed to release a finished read transaction: {failure}. Until this database is \
                 closed, no process sharing it can reclaim space from data overwritten or deleted \
                 after this point, so the file may grow"
            );
            #[cfg(not(feature = "logging"))]
            let _ = failure;
        }
    }

    // A persistent savepoint's reference, which takes no byte. See `persistent_savepoint_references`
    fn reference_persistent_savepoint(&mut self, id: TransactionId) {
        *self.live_read_transactions.entry(id).or_insert(0) += 1;
        *self.persistent_savepoint_references.entry(id).or_insert(0) += 1;
    }

    fn dereference_persistent_savepoint(&mut self, id: TransactionId) {
        self.decrement_reference_count(id);
        let count = self.persistent_savepoint_references.get_mut(&id).unwrap();
        *count -= 1;
        if *count == 0 {
            self.persistent_savepoint_references.remove(&id);
        }
    }

    // Hands the read transaction's reference over to the savepoint made from it, releasing the
    // byte with it: from here on the savepoint is protected the way a synced one is
    fn convert_reference_to_persistent_savepoint(
        &mut self,
        mem: &TransactionalMemory,
        id: TransactionId,
    ) {
        *self.persistent_savepoint_references.entry(id).or_insert(0) += 1;
        if self.active_transaction_lock_references(id) == 0 {
            Self::release_active_transaction_lock(mem, id);
        }
    }

    fn decrement_reference_count(&mut self, id: TransactionId) {
        let count = self.live_read_transactions.get_mut(&id).unwrap();
        *count -= 1;
        if *count == 0 {
            self.live_read_transactions.remove(&id);
        }
    }
}

#[cfg(not(redb_no_std))]
pub(crate) struct NativeSourceNoSavepointWriteProfile {
    actual_tracker: usize,
    actual_memory: usize,
    transaction_id: TransactionId,
    generation: u64,
}
#[cfg(not(redb_no_std))]
impl NativeSourceNoSavepointWriteProfile {
    pub(crate) fn require_for(
        &self,
        tracker: &TransactionTracker,
        memory: &TransactionalMemory,
        guard: &crate::db::TransactionGuard,
    ) -> Result {
        let current = tracker.check_source_no_savepoint_writer(memory, guard)?;
        if self.actual_tracker != current.actual_tracker
            || self.actual_memory != current.actual_memory
            || self.transaction_id != current.transaction_id
            || self.generation != current.generation
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(())
    }
}

pub(crate) struct TransactionTracker {
    state: Mutex<State>,
    live_write_transaction_available: Condvar,
    #[cfg(all(test, not(redb_no_std)))]
    inspection_wait_notification: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
}

impl TransactionTracker {
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_ordinary_cache_handoff_writer(
        &self,
        memory: &TransactionalMemory,
        guard: &crate::db::TransactionGuard,
    ) -> Result<()> {
        if !guard.is_exclusive_writer_for(memory, self)
            || self.state.lock()?.write_slot != WriteSlotState::Live(guard.id())
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        // No savepoint exclusion: ordinary writers retain their full behavior.
        Ok(())
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn check_source_no_savepoint_writer(
        &self,
        memory: &TransactionalMemory,
        guard: &crate::db::TransactionGuard,
    ) -> Result<NativeSourceNoSavepointWriteProfile> {
        if !guard.is_exclusive_writer_for(memory, self) {
            return Err(crate::tree_store::source_owner_refused());
        }
        let state = self.state.lock()?;
        if state.write_slot != WriteSlotState::Live(guard.id())
            || !state.valid_savepoints.is_empty()
            || !state.persistent_savepoints.is_empty()
            || !state.persistent_savepoint_references.is_empty()
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        let generation = state
            .resource_generation
            .ok_or_else(crate::tree_store::source_owner_refused)?;
        Ok(NativeSourceNoSavepointWriteProfile {
            actual_tracker: core::ptr::from_ref(self) as usize,
            actual_memory: core::ptr::from_ref(memory) as usize,
            transaction_id: guard.id(),
            generation,
        })
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn planned_read_capture_inline_bytes() -> usize {
        core::mem::size_of::<crate::sync::MutexGuard<'static, State>>()
            + core::mem::size_of::<NativePlannedReadSnapshot<'static>>()
            + core::mem::size_of::<NativePlannedReadError>()
    }
    pub(crate) fn readonly_capacity_bytes()
    -> core::result::Result<u64, crate::tree_store::AllocatorResourceError> {
        use crate::tree_store::{
            ReadOnlyResourceProfile as P, readonly_tracker_node_capacity_bytes,
        };
        P::arc_allocation::<Self>()?
            .checked_add(readonly_tracker_node_capacity_bytes()?)
            .ok_or(crate::tree_store::AllocatorResourceError::Overflow)
    }
    pub(crate) fn new(next_transaction_id: TransactionId) -> Self {
        Self {
            state: Mutex::new(State {
                next_savepoint_id: SavepointId(0),
                next_local_savepoint_id: LocalSavepointId(0),
                live_read_transactions: BTreeMap::default(),
                planned_read_slot: None,
                next_planned_read_serial: Some(1),
                persistent_savepoint_references: BTreeMap::default(),
                next_transaction_id,
                write_slot: WriteSlotState::Free,
                resource_generation: Some(0),
                next_inspection_serial: Some(0),
                valid_savepoints: BTreeMap::default(),
                persistent_savepoints: BTreeSet::default(),
                pending_non_durable_commits: BTreeMap::default(),
                #[cfg(not(redb_no_std))]
                source_pending_tail: None,
                #[cfg(not(redb_no_std))]
                source_tail_preparation: None,
                #[cfg(not(redb_no_std))]
                source_freed_markers: None,
                unprocessed_freed_non_durable_commits: BTreeSet::default(),
                deferred_close: None,
            }),
            live_write_transaction_available: Condvar::new(),
            #[cfg(all(test, not(redb_no_std)))]
            inspection_wait_notification: Mutex::new(None),
        }
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn planned_read_snapshot_for_test(&self) -> PlannedReadTrackerSnapshot {
        let state = self.state.lock().unwrap();
        let planned_id = state.planned_read_slot.map(|slot| slot.transaction_id);
        PlannedReadTrackerSnapshot {
            ordinary_entries: state.live_read_transactions.len(),
            ordinary_references: state.live_read_transactions.values().copied().sum(),
            ordinary_at_slot: planned_id
                .and_then(|id| state.live_read_transactions.get(&id).copied())
                .unwrap_or(0),
            planned_id,
            planned_serial: state.planned_read_slot.map(|slot| slot.serial),
            oldest: state.oldest_read_reference(),
            planned_matches_pending: planned_id.is_some_and(|id| state.pending_contains(id)),
        }
    }
    // Takes the write slot, waiting for the write transaction that holds it to end. The
    // transaction's id is issued separately, by `issue_write_transaction_id()`, once the file's
    // latest commit is known.
    pub(crate) fn take_write_slot(&self) {
        let mut state = self.state.lock().unwrap();
        while state.write_slot != WriteSlotState::Free {
            #[cfg(all(test, not(redb_no_std)))]
            if matches!(state.write_slot, WriteSlotState::Inspecting(_)) {
                if let Some(notification) = self.inspection_wait_notification.lock().unwrap().take()
                {
                    // Bounded one-shot signal only. The real Condvar below,
                    // not a test barrier, excludes the ordinary writer.
                    // A failing test may have already dropped its receiver.
                    // Never poison the actual tracker during test teardown.
                    let _ = notification.try_send(());
                }
            }
            state = self.live_write_transaction_available.wait(state).unwrap();
        }
        state.advance_resource_generation();
        state.write_slot = WriteSlotState::Initializing;
    }

    // Constant work over tracker metadata. One pending pair is only the tracker
    // part of the epilogue class; the memory owner must prove post-only state,
    // exact latest/durable IDs and admitted capacities before any native read.
    pub(crate) fn capture_resource_epoch(
        &self,
    ) -> core::result::Result<NativeResourceEpoch<'_>, NativeResourcePlanningRefusal> {
        let state = self
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        if state.write_slot != WriteSlotState::Free {
            return Err(NativeResourcePlanningRefusal::Busy);
        }
        let generation = state
            .resource_generation
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        Ok(NativeResourceEpoch {
            actual: self,
            generation,
            profile: state.resource_profile()?,
            inspection_serial: None,
        })
    }

    pub(crate) fn require_prepared_resource_writer(
        &self,
        epoch: &NativeResourceEpoch<'_>,
        actual: TransactionId,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        if epoch.inspection_serial.is_some() {
            return Err(NativeResourcePlanningRefusal::Unsupported);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        self.require_prepared_writer_parts(
            &state,
            epoch.actual,
            epoch.generation,
            epoch.profile,
            actual,
        )
    }

    fn require_prepared_writer_parts(
        &self,
        state: &State,
        prepared_owner: &TransactionTracker,
        prepared_generation: u64,
        prepared_profile: NativeTrackerProfile,
        actual: TransactionId,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        let generation = prepared_generation
            .checked_add(1)
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        if !core::ptr::eq(self, prepared_owner)
            || state.resource_generation != Some(generation)
            || state.write_slot != WriteSlotState::Live(actual)
            || state.next_transaction_id != actual
            || state.resource_profile()? != prepared_profile
        {
            return Err(NativeResourcePlanningRefusal::Stale);
        }
        Ok(())
    }

    fn require_resource_epoch(
        &self,
        state: &State,
        epoch: &NativeResourceEpoch<'_>,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        self.require_resource_parts(
            state,
            epoch.actual,
            epoch.generation,
            epoch.profile,
            epoch.inspection_serial,
        )
    }

    fn require_resource_parts(
        &self,
        state: &State,
        actual: &TransactionTracker,
        generation: u64,
        profile: NativeTrackerProfile,
        inspection_serial: Option<u64>,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        if !core::ptr::eq(self, actual) {
            return Err(NativeResourcePlanningRefusal::Stale);
        }
        let permitted = match inspection_serial {
            None => state.write_slot == WriteSlotState::Free,
            Some(serial) => state.write_slot == WriteSlotState::Inspecting(serial),
        };
        if !permitted {
            return Err(NativeResourcePlanningRefusal::Busy);
        }
        let current = state
            .resource_generation
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        if current != generation || state.resource_profile()? != profile {
            return Err(NativeResourcePlanningRefusal::Stale);
        }
        Ok(())
    }

    // The actual Database factory must complete all pre-read/pre-construction
    // admission first. This method atomically refuses a stale plan or returns
    // the ordinary RAII write slot, with no wait, page read or allocation.
    pub(crate) fn try_claim_resource_write_slot(
        self: &Arc<Self>,
        epoch: NativeResourceEpoch<'_>,
    ) -> core::result::Result<crate::db::WriteSlot, NativeResourcePlanningRefusal> {
        self.try_claim_fixed_resource_write_slot(&epoch)
    }

    pub(crate) fn try_claim_fixed_resource_write_slot(
        self: &Arc<Self>,
        epoch: &NativeResourceEpoch<'_>,
    ) -> core::result::Result<crate::db::WriteSlot, NativeResourcePlanningRefusal> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        if epoch.inspection_serial.is_some() {
            return Err(NativeResourcePlanningRefusal::Unsupported);
        }
        self.require_resource_epoch(&state, epoch)?;
        let Some(next) = epoch.generation.checked_add(1) else {
            state.resource_generation = None;
            return Err(NativeResourcePlanningRefusal::Exhausted);
        };
        let claim = NativeResourceSlotClaim {
            tracker: self.clone(),
        };
        state.resource_generation = Some(next);
        state.write_slot = WriteSlotState::Initializing;
        Ok(crate::db::WriteSlot::from_resource_claim(claim))
    }

    pub(crate) fn claim_system_inspection<'tracker>(
        self: &Arc<Self>,
        epoch: &NativeResourceEpoch<'tracker>,
    ) -> core::result::Result<
        (
            crate::db::NativeSystemInspectionLease,
            NativeResourceEpoch<'tracker>,
        ),
        NativeResourcePlanningRefusal,
    > {
        let mut state = self
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        if epoch.inspection_serial.is_some() {
            return Err(NativeResourcePlanningRefusal::Unsupported);
        }
        self.require_resource_epoch(&state, epoch)?;
        let serial = state
            .next_inspection_serial
            .ok_or(NativeResourcePlanningRefusal::Exhausted)?;
        state.next_inspection_serial = serial.checked_add(1);
        let private_epoch = NativeResourceEpoch {
            actual: epoch.actual,
            generation: epoch.generation,
            profile: epoch.profile,
            inspection_serial: Some(serial),
        };
        let claim = NativeSystemInspectionClaim {
            tracker: self.clone(),
            serial,
        };
        state.write_slot = WriteSlotState::Inspecting(serial);
        drop(state); // Never hold the tracker mutex during page/backend work.
        Ok((
            crate::db::NativeSystemInspectionLease::from_claim(claim),
            private_epoch,
        ))
    }

    pub(crate) fn end_system_inspection(
        &self,
        claim: NativeSystemInspectionClaim,
    ) -> Option<Arc<TransactionalMemory>> {
        assert!(core::ptr::eq(self, claim.tracker.as_ref()));
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Inspecting(claim.serial));
        state.write_slot = WriteSlotState::Free;
        self.live_write_transaction_available.notify_one();
        state.deferred_close.take()
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn notify_inspection_wait_for_test(&self, sender: std::sync::mpsc::SyncSender<()>) {
        let mut notification = self.inspection_wait_notification.lock().unwrap();
        assert!(notification.is_none());
        *notification = Some(sender);
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn exhaust_inspection_serial_for_test(&self) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Free);
        state.next_inspection_serial = None;
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn resource_scalars_for_test(&self) -> (Option<u64>, TransactionId) {
        let state = self.state.lock().unwrap();
        (state.resource_generation, state.next_transaction_id)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn exhaust_resource_generation_for_test(&self) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Free);
        state.resource_generation = Some(u64::MAX);
    }

    // Issues the slot's transaction an id that follows `last_committed`, the id of the file's
    // latest commit. `writer_lock` proves that the caller holds the writer byte, so that no
    // other process commits between reading that id and issuing this one.
    pub(crate) fn issue_write_transaction_id(
        &self,
        last_committed: TransactionId,
        _writer_lock: &WriterLock,
    ) -> TransactionId {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Initializing);
        state.next_transaction_id = state.next_transaction_id.max(last_committed);
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.write_slot = WriteSlotState::Live(transaction_id);

        transaction_id
    }

    // Leave `Live` before releasing the transaction's writer-lock reference
    pub(crate) fn begin_finalizing(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(matches!(state.write_slot, WriteSlotState::Live(_)));
        state.write_slot = WriteSlotState::Finalizing;
    }

    #[cfg(all(test, feature = "experimental-multiprocess"))]
    pub(crate) fn holds_writer_byte(&self) -> bool {
        matches!(
            self.state.lock().unwrap().write_slot,
            WriteSlotState::Live(_)
        )
    }

    // Whether a write transaction holds the write slot in this process
    pub(crate) fn write_transaction_live(&self) -> bool {
        self.state.lock().unwrap().write_slot != WriteSlotState::Free
    }

    // Returns the deferred close, if the Database was dropped while this transaction was live.
    // The caller must close the database, now that the write transaction has ended
    pub(crate) fn end_write_transaction(&self) -> Option<Arc<TransactionalMemory>> {
        let mut state = self.state.lock().unwrap();
        // A transaction that reached `Live` must finalize before releasing the slot.
        // Initialization failures release the slot directly from `Initializing`.
        assert!(matches!(
            state.write_slot,
            WriteSlotState::Initializing | WriteSlotState::Finalizing
        ));
        state.write_slot = WriteSlotState::Free;
        self.live_write_transaction_available.notify_one();
        #[cfg(not(redb_no_std))]
        {
            state.source_tail_preparation = None;
        }
        #[cfg(not(redb_no_std))]
        let retired = if state.source_tail_preparation.is_none()
            && state
                .source_freed_markers
                .as_ref()
                .is_some_and(|markers| markers.len() == 0)
        {
            state.source_freed_markers.take()
        } else {
            None
        };
        let deferred_close = state.deferred_close.take();
        drop(state);
        #[cfg(not(redb_no_std))]
        drop(retired);
        deferred_close
    }

    // Defers the database close to the end of the live write transaction, if one exists.
    // Returns false if there is no live write transaction, in which case the caller must close
    // the database itself. The check and the handoff share the state lock, making this atomic
    // with end_write_transaction(), so exactly one side performs the close
    pub(crate) fn defer_close_if_write_transaction_live(
        &self,
        mem: &Arc<TransactionalMemory>,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.write_slot != WriteSlotState::Free {
            state.deferred_close = Some(mem.clone());
            true
        } else {
            false
        }
    }

    // Scalar-only test observation. No collection or tracker authority escapes.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn reclamation_quote_snapshot_for_test(&self) -> ReclamationTrackerSnapshot {
        let state = self.state.lock().unwrap();
        ReclamationTrackerSnapshot {
            savepoints: state.valid_savepoints.len(),
            persistent_savepoints: state.persistent_savepoints.len(),
            pending: state.pending_count(),
            first_pending: state.first_pending(),
            unprocessed: state.unprocessed_count(),
        }
    }

    pub(crate) fn clear_pending_non_durable_commits(&self, memory: &TransactionalMemory) {
        let mut state = self.state.lock().unwrap();
        let ids = mem::take(&mut state.pending_non_durable_commits);
        for (_, durable_ancestor) in ids {
            state.remove_active_transaction_lock_reference(memory, durable_ancestor);
        }
        #[cfg(not(redb_no_std))]
        if let Some(tail) = state.source_pending_tail.take() {
            if state.active_transaction_lock_references(tail.durable_ancestor) == 0 {
                State::release_active_transaction_lock(memory, tail.durable_ancestor);
            }
        }
        // Neither origin's unprocessed markers are cleared here: the existing
        // extraction evidence, not durable publication, owns that transition.
    }

    pub(crate) fn is_unprocessed_non_durable_commit(&self, id: TransactionId) -> bool {
        let state = self.state.lock().unwrap();
        state.unprocessed_contains(id)
    }

    pub(crate) fn mark_non_durable_freed_pages_processed(
        &self,
        ids: impl IntoIterator<Item = TransactionId>,
    ) {
        let mut state = self.state.lock().unwrap();
        for id in ids {
            state.unprocessed_freed_non_durable_commits.remove(&id);
            #[cfg(not(redb_no_std))]
            if let Some(markers) = state.source_freed_markers.as_mut() {
                markers.remove(id);
            }
        }
        #[cfg(not(redb_no_std))]
        let retired = if state.source_tail_preparation.is_none()
            && state
                .source_freed_markers
                .as_ref()
                .is_some_and(|markers| markers.len() == 0)
        {
            state.source_freed_markers.take()
        } else {
            None
        };
        drop(state);
        // Logical removals retain their buffer guard. Only actual empty-owner
        // retirement destroys the Vec before refund, outside the tracker lock.
        #[cfg(not(redb_no_std))]
        drop(retired);
    }

    pub(crate) fn oldest_unprocessed_non_durable_commit(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        let ordinary = state
            .unprocessed_freed_non_durable_commits
            .iter()
            .next()
            .copied();
        match (ordinary, state.source_unprocessed_first()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    // `has_unprocessed_freed_pages` is true when a future non-durable commit should scan
    // freed-table entries under this transaction id for pages that can be reclaimed before the
    // id becomes durable.
    pub(crate) fn register_non_durable_commit(
        &self,
        mem: &TransactionalMemory,
        id: TransactionId,
        durable_ancestor: TransactionId,
        has_unprocessed_freed_pages: bool,
    ) -> Result {
        let header = mem.lock_header_shared()?;
        let mut state = self.state.lock().unwrap();
        state.add_active_transaction_lock_reference(mem, durable_ancestor, &header)?;
        assert!(
            state
                .pending_non_durable_commits
                .insert(id, durable_ancestor)
                .is_none()
        );
        if has_unprocessed_freed_pages {
            state.unprocessed_freed_non_durable_commits.insert(id);
        }

        Ok(())
    }

    // Caller is the future closed Source Immediate constructor, deriving the
    // actual guard/memory from its owning Database. No public tuple factory.
    // No mutation, ID reservation, or pending reference is created here.
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_source_tail_markers(
        &self,
        memory: &TransactionalMemory,
        guard: &crate::db::TransactionGuard,
        resources: &crate::tree_store::NativePageResources,
    ) -> Result {
        if memory.concurrency_mode() != crate::db::ConcurrencyMode::ExclusiveWriter
            || !resources.is_source_write()
            || !guard.is_exclusive_writer_for(memory, self)
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        let mut state = self.state.lock()?;
        if state.write_slot != WriteSlotState::Live(guard.id())
            || !state.valid_savepoints.is_empty()
            || !state.persistent_savepoints.is_empty()
            || !state.persistent_savepoint_references.is_empty()
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        let generation = state
            .resource_generation
            .ok_or_else(crate::tree_store::source_owner_refused)?;
        let replacement = crate::tree_store::NativeOwnedTransactionMarkers::prepare_replacement(
            state.source_freed_markers.as_ref(),
            resources,
        )?;
        let retired = state.source_freed_markers.replace(replacement);
        state.source_tail_preparation = Some(SourceTailPreparationStamp {
            actual_memory: core::ptr::from_ref(memory) as usize,
            writer_id: guard.id(),
            generation,
        });
        drop(state);
        // Both buffers coexist under their own actual guards until this point.
        drop(retired);
        Ok(())
    }

    // Intentionally NOT wired into register_non_durable_commit or the Source
    // test constructor. Actual Immediate/quick-repair/post-only recipe checks,
    // prospective marker preparation and all owner coverage must join first.
    #[cfg(not(redb_no_std))]
    pub(crate) fn register_source_post_tail(
        &self,
        memory: &TransactionalMemory,
        guard: &crate::db::TransactionGuard,
        id: TransactionId,
        has_unprocessed_freed_pages: bool,
    ) -> Result {
        if memory.concurrency_mode() != crate::db::ConcurrencyMode::ExclusiveWriter
            || !guard.is_exclusive_writer_for(memory, self)
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        let header = memory.lock_header_shared()?;
        let (actual_published, _) = memory.latest_committed_snapshot(&header)?;
        let actual_durable = memory.get_last_durable_transaction_id()?;
        let mut state = self.state.lock()?;
        if state.write_slot != WriteSlotState::Live(guard.id())
            || actual_published != id
            || actual_durable != guard.id()
            || !state.source_tail_preparation.is_some_and(|prepared| {
                prepared.actual_memory == core::ptr::from_ref(memory) as usize
                    && prepared.writer_id == guard.id()
                    && state.resource_generation == Some(prepared.generation)
            })
            || state.next_transaction_id != id
            || guard.id().raw_id().checked_add(1) != Some(id.raw_id())
            || state.source_pending_tail.is_some()
            || state.pending_contains(id)
            || state.unprocessed_contains(id)
            || !state.valid_savepoints.is_empty()
            || !state.persistent_savepoints.is_empty()
            || !state.persistent_savepoint_references.is_empty()
            || (has_unprocessed_freed_pages
                && !state
                    .source_freed_markers
                    .as_ref()
                    .is_some_and(|markers| markers.has_spare()))
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        // ExclusiveWriter's active-byte operation is a no-op. The exact
        // ancestor reference still joins every local read/reclamation count.
        state.source_pending_tail = Some(SourcePendingTail {
            id,
            durable_ancestor: guard.id(),
        });
        if has_unprocessed_freed_pages {
            state
                .source_freed_markers
                .as_mut()
                .expect("checked prepared marker owner")
                .insert_prepared(id);
        }
        state.source_tail_preparation = None;
        let retired = if state
            .source_freed_markers
            .as_ref()
            .is_some_and(|markers| markers.len() == 0)
        {
            state.source_freed_markers.take()
        } else {
            None
        };
        drop(state);
        drop(retired);
        Ok(())
    }

    // Reserve a transaction id that was created without starting a new write transaction.
    // The caller must still register the resulting root if it is a non-durable commit.
    pub(crate) fn reserve_transaction_id(
        &self,
        id: TransactionId,
        live_write_transaction: TransactionId,
    ) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.write_slot,
            WriteSlotState::Live(live_write_transaction)
        );
        assert_eq!(id, state.next_transaction_id.next());
        state.next_transaction_id = id;
    }

    // Reserves a repair commit's id so it is never issued again: crash recovery orders the
    // commit slots by transaction id, so an id must never be committed twice
    pub(crate) fn reserve_repair_transaction_id(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Free);
        state.advance_resource_generation();
        state.next_transaction_id = state.next_transaction_id.max(id);
    }

    // Continues savepoint ids from `next_savepoint` where that is past the ones issued here:
    // another process may have created the file's persistent savepoints
    pub(crate) fn restore_savepoint_counter_state(&self, next_savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        state.next_savepoint_id = state.next_savepoint_id.max(next_savepoint);
    }

    // Sync the file's persistent savepoints. `current` must be the current set of persistent savepoints.
    pub(crate) fn sync_persistent_savepoints(
        &self,
        current: &BTreeMap<SavepointId, TransactionId>,
    ) -> Result {
        let mut state = self.state.lock().unwrap();
        let gone: Vec<SavepointId> = state
            .persistent_savepoints
            .iter()
            .filter(|id| current.get(id) != Some(&state.valid_savepoints[id].transaction_id))
            .copied()
            .collect();
        for id in gone {
            state.persistent_savepoints.remove(&id);
            let registration = state.valid_savepoints.remove(&id).unwrap();
            state.dereference_persistent_savepoint(registration.transaction_id);
        }
        for (&id, &transaction) in current {
            if state.valid_savepoints.contains_key(&id) {
                continue;
            }
            state.reference_persistent_savepoint(transaction);
            state.register_savepoint(id, transaction);
            state.persistent_savepoints.insert(id);
        }

        Ok(())
    }

    // Marks an already-registered savepoint as persistent, which hands it the reference held by
    // the read transaction it was created from
    pub(crate) fn convert_savepoint_to_persistent(
        &self,
        mem: &TransactionalMemory,
        id: SavepointId,
    ) {
        let mut state = self.state.lock().unwrap();
        let transaction = state.valid_savepoints.get(&id).unwrap().transaction_id;
        state.persistent_savepoints.insert(id);
        state.convert_reference_to_persistent_savepoint(mem, transaction);
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn capture_planned_read_snapshot<'owner>(
        &'owner self,
        mem: &'owner TransactionalMemory,
    ) -> core::result::Result<NativePlannedReadSnapshot<'owner>, NativePlannedReadError> {
        if mem.concurrency_mode() != crate::db::ConcurrencyMode::ExclusiveWriter {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Unsupported,
            ));
        }
        // Same lock order and actual snapshot primitive as ordinary begin_read.
        let header = mem.lock_header_shared()?;
        let state = self.state.lock().map_err(StorageError::from)?;
        if state.write_slot != WriteSlotState::Free || state.planned_read_slot.is_some() {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Busy,
            ));
        }
        let generation = state
            .resource_generation
            .ok_or(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Exhausted,
            ))?;
        if state.next_planned_read_serial.is_none() {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Exhausted,
            ));
        }
        let (transaction_id, root) = mem.latest_committed_snapshot(&header)?;
        Ok(NativePlannedReadSnapshot {
            actual_tracker: self,
            actual_memory: mem,
            transaction_id,
            root,
            generation,
        })
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn register_planned_read(
        &self,
        mem: &TransactionalMemory,
        snapshot: &NativePlannedReadSnapshot<'_>,
    ) -> core::result::Result<(TransactionId, Option<BtreeHeader>, u64), NativePlannedReadError>
    {
        if !core::ptr::eq(snapshot.actual_tracker, self)
            || !core::ptr::eq(snapshot.actual_memory, mem)
        {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Stale,
            ));
        }
        if mem.concurrency_mode() != crate::db::ConcurrencyMode::ExclusiveWriter {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Unsupported,
            ));
        }
        let header = mem.lock_header_shared()?;
        let mut state = self.state.lock().map_err(StorageError::from)?;
        if state.planned_read_slot.is_some() || state.write_slot != WriteSlotState::Free {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Busy,
            ));
        }
        if state.resource_generation != Some(snapshot.generation) {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Stale,
            ));
        }
        let (id, root) = mem.latest_committed_snapshot(&header)?;
        if id != snapshot.transaction_id || root != snapshot.root {
            return Err(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Stale,
            ));
        }
        let serial = state
            .next_planned_read_serial
            .ok_or(NativePlannedReadError::Resource(
                NativeResourcePlanningRefusal::Exhausted,
            ))?;
        state.next_planned_read_serial = serial.checked_add(1);
        state.planned_read_slot = Some(PlannedReadSlot {
            transaction_id: id,
            serial,
            actual_memory: core::ptr::from_ref(mem) as usize,
        });
        // No BTreeMap node, OS lock-range entry or namespace allocation occurs.
        Ok((id, root, serial))
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_planned_read_pin(
        &self,
        mem: &TransactionalMemory,
        id: TransactionId,
        serial: u64,
    ) -> core::result::Result<(), NativeResourcePlanningRefusal> {
        let state = self
            .state
            .lock()
            .map_err(|_| NativeResourcePlanningRefusal::Poisoned)?;
        if !state.planned_read_slot.is_some_and(|slot| {
            slot.transaction_id == id
                && slot.serial == serial
                && slot.actual_memory == core::ptr::from_ref(mem) as usize
        }) {
            return Err(NativeResourcePlanningRefusal::Stale);
        }
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn deallocate_planned_read(
        &self,
        mem: &TransactionalMemory,
        id: TransactionId,
        serial: u64,
    ) {
        let mut state = self.state.lock().unwrap();
        let matches = state.planned_read_slot.is_some_and(|slot| {
            slot.transaction_id == id
                && slot.serial == serial
                && slot.actual_memory == core::ptr::from_ref(mem) as usize
        });
        assert!(
            matches,
            "only the actual final planned read guard releases its slot"
        );
        state.planned_read_slot = None;
        // ExclusiveWriter never announces active IDs through OS range locks.
        // Ordinary map references at the same ID remain untouched.
    }
    pub(crate) fn register_read_transaction(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<(TransactionId, Option<BtreeHeader>)> {
        let header = mem.lock_header_shared()?;
        // Hold the tracker across snapshot capture and registration so reclamation cannot
        // miss this reader. The header lock also excludes peer commits and local reloads.
        let mut state = self.state.lock()?;
        let (id, root) = mem.latest_committed_snapshot(&header)?;
        state.add_active_transaction_lock_reference(mem, id, &header)?;

        Ok((id, root))
    }

    pub(crate) fn deallocate_read_transaction(&self, mem: &TransactionalMemory, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        state.remove_active_transaction_lock_reference(mem, id);
    }

    pub(crate) fn any_savepoint_exists(&self) -> bool {
        !self.state.lock().unwrap().valid_savepoints.is_empty()
    }

    pub(crate) fn any_persistent_savepoint_exists(&self) -> bool {
        !self.state.lock().unwrap().persistent_savepoints.is_empty()
    }

    // True if an ephemeral (non-persistent) savepoint exists. Unlike persistent ones, it may pin a
    // non-durable transaction whose pages a reload would discard.
    pub(crate) fn any_ephemeral_savepoint_exists(&self) -> bool {
        let state = self.state.lock().unwrap();
        state
            .valid_savepoints
            .keys()
            .any(|id| !state.persistent_savepoints.contains(id))
    }

    // Excludes internal read refs that only pin durable ancestors of pending
    // non-durable commits.
    pub(crate) fn any_user_read_reference_exists(&self) -> bool {
        let state = self.state.lock().unwrap();
        if state.planned_read_slot.is_some() {
            return true;
        }
        for (id, count) in &state.live_read_transactions {
            let pending_count = state
                .pending_non_durable_commits
                .values()
                .filter(|ancestor| *ancestor == id)
                .count() as u64;
            if *count > pending_count {
                return true;
            }
        }
        false
    }

    pub(crate) fn allocate_savepoint(
        &self,
        transaction_id: TransactionId,
    ) -> (SavepointId, LocalSavepointId) {
        let mut state = self.state.lock().unwrap();
        let id = state.next_savepoint_id.next();
        state.next_savepoint_id = id;
        let local_id = state.register_savepoint(id, transaction_id);
        (id, local_id)
    }

    // Forgets the savepoint, leaving the transaction's reference to whoever owns it
    pub(crate) fn remove_savepoint_registration(&self, savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        state.valid_savepoints.remove(&savepoint);
        state.persistent_savepoints.remove(&savepoint);
    }

    // Deallocates the given persistent savepoint and its matching reference on the transaction
    pub(crate) fn deallocate_persistent_savepoint(
        &self,
        savepoint: SavepointId,
        transaction: TransactionId,
    ) {
        self.remove_savepoint_registration(savepoint);
        self.state
            .lock()
            .unwrap()
            .dereference_persistent_savepoint(transaction);
    }

    pub(crate) fn savepoint_local_id(&self, id: SavepointId) -> Option<LocalSavepointId> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .get(&id)
            .map(|registration| registration.local_id)
    }

    pub(crate) fn list_savepoints_after(&self, id: SavepointId) -> Vec<SavepointId> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .range((
                core::ops::Bound::Excluded(id),
                core::ops::Bound::Unbounded::<SavepointId>,
            ))
            .map(|(x, _)| *x)
            .collect()
    }

    // Removes the given savepoints from the in-memory `valid_savepoints` map without touching
    // live_read_transactions refs. The caller is responsible for making sure those refs are
    // released by some other means: ephemeral `Savepoint::drop` or `deallocate_persistent_savepoint`
    // (called via `delete_persistent_savepoint`) do that for their respective savepoint kinds.
    //
    // Savepoints that have already been removed (for example, by `deallocate_persistent_savepoint` earlier
    // in the same transaction) are silently skipped.
    pub(crate) fn invalidate_savepoints(&self, savepoints: impl IntoIterator<Item = SavepointId>) {
        let mut state = self.state.lock().unwrap();
        for id in savepoints {
            state.valid_savepoints.remove(&id);
            state.persistent_savepoints.remove(&id);
        }
    }

    // The oldest savepoint, ignoring the given savepoint ids. Used by committing transactions
    // to compute the savepoint horizon as it will be after their staged savepoint deletions
    // are applied on commit.
    pub(crate) fn oldest_savepoint_excluding(
        &self,
        exclude: &BTreeSet<SavepointId>,
    ) -> Option<(SavepointId, TransactionId)> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .iter()
            .find(|(id, _)| !exclude.contains(id))
            .map(|(id, registration)| (*id, registration.transaction_id))
    }

    pub(crate) fn oldest_local_referenced_transaction(&self) -> Option<TransactionId> {
        self.state.lock().unwrap().oldest_read_reference()
    }

    // Returns the transaction id of the oldest non-durable transaction which has not been processed
    // for freeing, which has live read transactions
    pub(crate) fn oldest_live_read_nondurable_transaction(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        let ordinary = state
            .live_read_transactions
            .keys()
            .find(|id| state.pending_contains(**id))
            .copied();
        let planned = state
            .planned_read_slot
            .map(|slot| slot.transaction_id)
            .filter(|id| state.pending_contains(*id));
        let user = match (ordinary, planned) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let source = state
            .source_ancestor_reference()
            .filter(|id| state.pending_contains(*id));
        match (user, source) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // The tracker takes the "active transaction byte" through this, so it needs somewhere to
    // take it. Opened ExclusiveWriter, where that is a no-op
    fn memory() -> TransactionalMemory {
        use crate::tree_store::{InMemoryBackend, PAGE_SIZE};

        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();

        mem
    }

    #[test]
    fn non_durable_commit_without_freed_pages_is_not_unprocessed() {
        let mem = memory();
        let tracker = TransactionTracker::new(TransactionId::new(0));

        tracker
            .register_non_durable_commit(&mem, TransactionId::new(1), TransactionId::new(0), false)
            .unwrap();
        assert_eq!(None, tracker.oldest_unprocessed_non_durable_commit());

        tracker
            .register_non_durable_commit(&mem, TransactionId::new(2), TransactionId::new(0), true)
            .unwrap();
        assert_eq!(
            Some(TransactionId::new(2)),
            tracker.oldest_unprocessed_non_durable_commit()
        );
    }

    #[test]
    fn syncing_a_reused_savepoint_id_replaces_its_identity_and_reference() {
        let tracker = TransactionTracker::new(TransactionId::new(3));
        let id = SavepointId(1);
        let old_transaction = TransactionId::new(1);
        let new_transaction = TransactionId::new(2);
        tracker
            .sync_persistent_savepoints(&[(id, old_transaction)].into())
            .unwrap();
        let old_local_id = tracker.savepoint_local_id(id).unwrap();

        tracker
            .sync_persistent_savepoints(&[(id, new_transaction)].into())
            .unwrap();
        let new_local_id = tracker.savepoint_local_id(id).unwrap();
        assert_ne!(old_local_id, new_local_id);
        assert_eq!(
            tracker.oldest_local_referenced_transaction(),
            Some(new_transaction)
        );

        tracker
            .sync_persistent_savepoints(&[(id, new_transaction)].into())
            .unwrap();
        assert_eq!(tracker.savepoint_local_id(id), Some(new_local_id));
        tracker
            .sync_persistent_savepoints(&BTreeMap::new())
            .unwrap();
        assert_eq!(tracker.oldest_local_referenced_transaction(), None);
        assert_eq!(tracker.savepoint_local_id(id), None);
    }
}
