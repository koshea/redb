//! Closed actual Database binding for consuming SYSTEM preparation.
use super::*;
use crate::transaction_tracker::NativeDetachedResourceEpoch;
use crate::tree_store::NativeKeyPathError as E;
type BindingResult<T> = core::result::Result<T, E>;

// No native read/page/inspection or borrow of an epoch struct survives here.
// The actual Database and its tracker remain borrowed for the whole plan.
pub(crate) struct NativePreparedSystemBinding<'work> {
    actual: &'work Database,
    epoch: NativeDetachedResourceEpoch<'work>,
    user_root: Option<BtreeHeader>,
    system_root: Option<BtreeHeader>,
    committed: TransactionId,
    next: TransactionId,
}
impl<'work> NativePreparedSystemBinding<'work> {
    pub(crate) fn capture(
        actual: &'work Database,
        parts: &CheckedPinnedReadParts<'_>,
        epoch: &crate::transaction_tracker::NativeResourceEpoch<'work>,
    ) -> BindingResult<Self> {
        if !Arc::ptr_eq(&actual.mem, parts.actual_memory())
            || actual.mem.concurrency_mode() != ConcurrencyMode::ExclusiveWriter
        {
            return Err(E::ForeignOwner);
        }
        // Revalidate the installed read slot at actual use; the proof remains
        // a borrow of its genuine guard and cannot adopt caller raw tuples.
        let checked = parts
            .guard
            .checked_pinned_read_parts()
            .map_err(E::Epoch)?
            .ok_or(E::Unpinned)?;
        let parts = &checked;
        epoch
            .require_tracker(&actual.transaction_tracker)
            .map_err(E::Epoch)?;
        epoch
            .require_tracker(parts.actual_tracker())
            .map_err(E::Epoch)?;
        if !Arc::ptr_eq(parts.actual_memory(), &actual.mem) {
            return Err(E::ForeignOwner);
        }
        let committed = actual.mem.get_last_committed_transaction_id()?;
        if parts.transaction_id() != committed {
            return Err(E::ForeignOwner);
        }
        let (next, _) = epoch
            .observe_native_write_counters(committed)
            .map_err(E::Epoch)?;
        let user_root = actual.mem.get_data_root();
        let system_root = actual.mem.get_system_root();
        let sealed = epoch.seal_detached().map_err(E::Epoch)?;
        let result = Self {
            actual,
            epoch: sealed,
            user_root,
            system_root,
            committed,
            next,
        };
        result.require_current()?;
        Ok(result)
    }
    pub(crate) fn require_current(&self) -> BindingResult<()> {
        self.epoch
            .require_for(&self.actual.transaction_tracker, self.committed, self.next)
            .map_err(E::Epoch)?;
        if self.actual.mem.get_data_root() != self.user_root
            || self.actual.mem.get_system_root() != self.system_root
            || self.actual.mem.get_last_committed_transaction_id()? != self.committed
        {
            return Err(E::Epoch(
                crate::transaction_tracker::NativeResourcePlanningRefusal::Stale,
            ));
        }
        // Close the optimistic check after all actual native stamp reads.
        self.epoch
            .require_for(&self.actual.transaction_tracker, self.committed, self.next)
            .map_err(E::Epoch)
    }
    pub(crate) fn require_fixed_writer(&self, guard: &TransactionGuard) -> BindingResult<()> {
        if !guard.is_exclusive_writer_for(&self.actual.mem, &self.actual.transaction_tracker)
            || guard.id() != self.next
        {
            return Err(E::ForeignOwner);
        }
        self.epoch
            .require_writer(&self.actual.transaction_tracker, guard.id())
            .map_err(E::Epoch)?;
        if self.actual.mem.get_data_root() != self.user_root
            || self.actual.mem.get_system_root() != self.system_root
            || self.actual.mem.get_last_committed_transaction_id()? != self.committed
        {
            return Err(E::Epoch(
                crate::transaction_tracker::NativeResourcePlanningRefusal::Stale,
            ));
        }
        self.epoch
            .require_writer(&self.actual.transaction_tracker, guard.id())
            .map_err(E::Epoch)
    }
    pub(crate) fn matches_prepared(
        &self,
        next: TransactionId,
        root: Option<BtreeHeader>,
        committed: TransactionId,
    ) -> bool {
        self.next == next && self.system_root == root && self.committed == committed
    }
}
