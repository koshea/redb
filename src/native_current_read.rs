//! Preadmitted native capture of an actual Current root, with no cache publish.
use super::{
    Database, PlannedHandleGate, PlannedReadTransaction, ReadOnlyReopenError, TransactionGuard,
};
use crate::transaction_tracker::{
    NativePlannedReadError, NativePlannedReadSnapshot, NativeResourcePlanningRefusal,
    TransactionTracker,
};
use crate::tree_store::{
    NativeReadContext, NativeReadLimits, ReadOnlyResourceProfile, TransactionalMemory,
};
use core::mem::size_of;

/// Numerical proposal for one actual native Current snapshot. It grants no
/// Source history/plan/origin authority and never owns an unregistered read.
pub struct NativeCurrentReadPlan<'owner> {
    actual: &'owner Database,
    snapshot: NativePlannedReadSnapshot<'owner>,
    limits: NativeReadLimits,
    peak: u64,
}
impl NativeCurrentReadPlan<'_> {
    #[cfg(test)]
    pub(crate) fn fixed_cleanup_limits_for_fixture(&self) -> NativeReadLimits {
        self.limits
    }

    /// Already-admitted query peak, including the new actual native guard.
    pub fn peak_bytes(&self) -> u64 {
        self.peak
    }
}
fn error(value: NativePlannedReadError) -> ReadOnlyReopenError {
    match value {
        NativePlannedReadError::Storage(value) => ReadOnlyReopenError::Storage(value),
        NativePlannedReadError::Resource(value) => match value {
            NativeResourcePlanningRefusal::Busy => ReadOnlyReopenError::Capacity,
            NativeResourcePlanningRefusal::Stale => ReadOnlyReopenError::Stale,
            NativeResourcePlanningRefusal::Exhausted => ReadOnlyReopenError::Overflow,
            NativeResourcePlanningRefusal::Unsupported
            | NativeResourcePlanningRefusal::Poisoned => ReadOnlyReopenError::Unsupported,
        },
    }
}
impl Database {
    /// Fixed native stack/control work to admit before observing a Current plan.
    /// The supported ExclusiveWriter capture performs no backend I/O or heap
    /// allocation; poisoning errors carry only a native static location.
    pub fn current_read_plan_capture_bytes() -> Result<u64, ReadOnlyReopenError> {
        let bytes = size_of::<NativeCurrentReadPlan<'static>>()
            .checked_add(TransactionTracker::planned_read_capture_inline_bytes())
            .and_then(|v| v.checked_add(TransactionalMemory::planned_read_capture_inline_bytes()))
            .and_then(|v| {
                v.checked_add(size_of::<(
                    Result<NativeCurrentReadPlan<'static>, ReadOnlyReopenError>,
                    [u64; 3],
                    core::array::IntoIter<u64, 3>,
                    u64,
                    Option<u64>,
                    ReadOnlyReopenError,
                )>())
            })
            .ok_or(ReadOnlyReopenError::Overflow)?;
        crate::ReadTransaction::read_context_plan_capture_bytes()?
            .checked_add(u64::try_from(bytes).map_err(|_| ReadOnlyReopenError::Overflow)?)
            .ok_or(ReadOnlyReopenError::Overflow)
    }
    /// Observe an actual native snapshot before registration/guard/page creation.
    /// The caller holds the compiled capture workspace and then admits peak_bytes.
    pub fn plan_current_read_context(
        &self,
    ) -> Result<NativeCurrentReadPlan<'_>, ReadOnlyReopenError> {
        if cfg!(feature = "logging") {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let limits = self.mem.read_context_limits()?;
        let snapshot = self
            .transaction_tracker
            .capture_planned_read_snapshot(&self.mem)
            .map_err(error)?;
        let mut peak = crate::transactions::read_context_work_bound(limits)?;
        for bytes in [
            Self::current_read_plan_capture_bytes()?,
            ReadOnlyResourceProfile::arc_allocation::<TransactionGuard>()?,
            u64::try_from(size_of::<TransactionGuard>())
                .map_err(|_| ReadOnlyReopenError::Overflow)?,
        ] {
            peak = peak
                .checked_add(bytes)
                .ok_or(ReadOnlyReopenError::Overflow)?;
        }
        Ok(NativeCurrentReadPlan {
            actual: self,
            snapshot,
            limits,
            peak,
        })
    }
    // Inactive private complete-planner seam. The actual native plan supplies
    // owner/snapshot/limits; the managed pool supplies only prior numerical
    // admission. Neither an ID/root tuple nor a caller-selected resolver is
    // accepted. The future closed planner consumes/drops this read before
    // acquiring the native writer/coordinator boundary.
    pub(crate) fn begin_fixed_recipe_observation<'owner>(
        &'owner self,
        plan: NativeCurrentReadPlan<'owner>,
        epoch: &crate::tree_store::NativeNumericalEpoch,
    ) -> Result<crate::ReadTransaction, ReadOnlyReopenError> {
        if !core::ptr::eq(plan.actual, self) {
            return Err(ReadOnlyReopenError::Stale);
        }
        // Source-owned context metadata is admitted before registration and
        // before TableTree can fetch its first namespace page. The inline
        // actual planned registration is independent of ordinary read IDs.
        let context = NativeReadContext::for_fixed_recipe(plan.limits, epoch)?;
        let (guard, root) = TransactionGuard::allocate_planned_read(
            self.transaction_tracker.clone(),
            &self.mem,
            &plan.snapshot,
        )
        .map_err(error)?;
        Ok(crate::ReadTransaction::new_with_read_context(
            self.mem.clone(),
            guard,
            root,
            context,
        )?)
    }

    /// Recheck the actual snapshot and install one bounded native read pin.
    /// This never populates an ordinary native or first-party Current cache.
    /// Every returned handle must retain the caller's numerical admission;
    /// the integrating closed facade supplies that physical lifetime custody.
    pub fn begin_planned_read_context<'owner>(
        &'owner self,
        plan: NativeCurrentReadPlan<'owner>,
    ) -> Result<PlannedReadTransaction<'owner>, ReadOnlyReopenError> {
        if !core::ptr::eq(plan.actual, self) {
            return Err(ReadOnlyReopenError::Stale);
        }
        let (guard, root) = TransactionGuard::allocate_planned_read(
            self.transaction_tracker.clone(),
            &self.mem,
            &plan.snapshot,
        )
        .map_err(error)?;
        // Registration is inline and already checked. Every allocation below
        // occurs only after the caller admitted this complete native query peak.
        let context = NativeReadContext::new_numerical(plan.limits)?;
        let inner =
            crate::ReadTransaction::new_with_read_context(self.mem.clone(), guard, root, context)?;
        Ok(PlannedReadTransaction::from_context(
            inner,
            PlannedHandleGate::new(),
        ))
    }
}

#[cfg(all(test, not(feature = "logging"), not(feature = "experimental-api-5")))]
#[path = "native_current_read_tests.rs"]
mod tests;
