//! Closed numerical context for reads from an already pinned native root.
//! No root, transaction, parser, Source authority or caller file is fabricated.
use super::ReadTransaction;
use crate::db::{
    PLANNED_READ_KEY_BYTES, PlannedHandleGate, PlannedReadTransaction, ReadOnlyReopenError,
};
use crate::tree_store::{
    NativePageResources, NativeReadContext, NativeReadLimits, ReadOnlyResourceProfile,
    readonly_cursor_capacity_bytes,
};
use core::mem::size_of;

/// A finite native read allocation envelope bound to one existing read pin.
/// Larger unobserved page/type shapes refuse; this is not a complete primary
/// baseline-retention or application Source authorization proof.
pub struct NativeReadContextPlan<'read> {
    actual_read: &'read ReadTransaction,
    limits: NativeReadLimits,
    peak: u64,
}
impl NativeReadContextPlan<'_> {
    // Numerical regression control only; cannot increase any admitted limit or
    // change production visibility. The conservative original quote is retained.
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn shorten_page_capacity_for_test(&mut self, capacity: usize) {
        self.limits.page_bytes = self.limits.page_bytes.min(capacity);
    }
    /// Bytes which must already be admitted before opening this context.
    pub fn peak_bytes(&self) -> u64 {
        self.peak
    }
}
impl ReadTransaction {
    /// Fixed native working bytes to admit before capturing an existing-read plan.
    /// This pure quote performs no I/O, registration, allocation or root lookup.
    /// The actual plan peak includes these same bytes, so a caller may retain
    /// its initial admission and add only the remaining peak before opening.
    pub fn read_context_plan_capture_bytes() -> Result<u64, ReadOnlyReopenError> {
        if cfg!(feature = "logging") {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let owned_controls = ReadOnlyResourceProfile::read_context_capture_control_bytes()?;
        // plan_read_context retains its limits/output while the peak expression
        // constructs seven quotes and consumes their IntoIter. Include both input
        // array and iterator rather than assuming compiler stack-slot reuse.
        let controls = size_of::<(
            NativeReadContextPlan<'static>,
            Result<NativeReadContextPlan<'static>, ReadOnlyReopenError>,
            NativeReadLimits,
            crate::db::CheckedPinnedReadParts<'static>,
            Result<
                Option<crate::db::CheckedPinnedReadParts<'static>>,
                crate::transaction_tracker::NativeResourcePlanningRefusal,
            >,
            [u64; 7],
            core::array::IntoIter<u64, 7>,
            // peak, current quote, inline size and checked/conversion results
            u64,
            u64,
            usize,
            Option<u64>,
            Result<u64, ReadOnlyReopenError>,
            Result<u64, crate::tree_store::AllocatorResourceError>,
            ReadOnlyReopenError,
        )>();
        owned_controls
            .checked_add(u64::try_from(controls).map_err(|_| ReadOnlyReopenError::Overflow)?)
            .ok_or(ReadOnlyReopenError::Overflow)
    }
    #[cfg(test)]
    pub(crate) fn tree_context_current_for_test(&self) -> usize {
        self.tree.context_current_for_test()
    }
    /// Capture observed native limits from this exact existing read transaction.
    /// This allocates no new transaction-registration or native page graph.
    pub fn plan_read_context(&self) -> Result<NativeReadContextPlan<'_>, ReadOnlyReopenError> {
        if cfg!(feature = "logging") {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let limits = self.mem.read_context_limits()?;
        let peak = read_context_work_bound(limits)?;
        Ok(NativeReadContextPlan {
            actual_read: self,
            limits,
            peak,
        })
    }
    /// Read the same immutable root under this finite numerical plan.
    /// The caller must retain its already-admitted resource owner through every
    /// returned handle, including forgotten children; an integrating closed
    /// facade must implement that custody. This grants no Source authority.
    /// Ordinary reads retain their default policy; copied pages never enter
    /// their cache. No callback, external file, or current root is accepted.
    pub fn open_planned_read_context<'read>(
        &'read self,
        plan: NativeReadContextPlan<'read>,
    ) -> Result<PlannedReadTransaction<'read>, ReadOnlyReopenError> {
        if cfg!(feature = "logging") {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        if !core::ptr::eq(plan.actual_read, self) {
            return Err(ReadOnlyReopenError::Stale);
        }
        let context = NativeReadContext::new_numerical(plan.limits)?;
        let handle = PlannedHandleGate::new();
        let inner = ReadTransaction {
            mem: self.mem.clone(),
            tree: self.tree.with_read_context(context)?,
        };
        Ok(PlannedReadTransaction::from_context(inner, handle))
    }
}

pub(crate) fn read_context_work_bound(
    limits: NativeReadLimits,
) -> Result<u64, ReadOnlyReopenError> {
    let mut peak = u64::try_from(limits.page_bytes)
        .map_err(|_| ReadOnlyReopenError::Overflow)?
        .checked_add(ReadTransaction::read_context_plan_capture_bytes()?)
        .ok_or(ReadOnlyReopenError::Overflow)?;
    for bytes in [
        NativePageResources::control_capacity_bytes()?,
        PlannedHandleGate::capacity_bytes()?,
        readonly_cursor_capacity_bytes()?,
        u64::try_from(NativeReadContext::metadata_capacity_bytes(limits)?)
            .map_err(|_| ReadOnlyReopenError::Overflow)?,
        read_context_definition_workspace(limits.definition)?,
    ] {
        peak = peak
            .checked_add(bytes)
            .ok_or(ReadOnlyReopenError::Overflow)?;
    }
    let inline = size_of::<NativeReadContextPlan<'static>>()
        + size_of::<NativeReadContext>()
        + size_of::<ReadTransaction>()
        + size_of::<PlannedReadTransaction<'static>>()
        + size_of::<crate::PlannedBytesTable<'static>>()
        + size_of::<crate::PlannedStrTable<'static>>()
        + size_of::<crate::PlannedByteRange<'static>>()
        + size_of::<crate::PlannedKeyValue<'static>>()
        + size_of::<crate::PlannedValue<'static>>()
        + size_of::<ReadOnlyReopenError>();
    peak = peak
        .checked_add(u64::try_from(inline).map_err(|_| ReadOnlyReopenError::Overflow)?)
        .ok_or(ReadOnlyReopenError::Overflow)?;
    Ok(peak)
}

#[cfg(all(test, not(feature = "logging"), not(feature = "experimental-api-5")))]
#[path = "native_read_context_tests.rs"]
mod tests;

// Exact existing readonly definition/key/type temporary quote, shared with the
// closed same-pool planner. Factoring preserves its original numerical value.
// It does not cover mutable builders, namespace serialization or SYSTEM work.
pub(super) fn read_context_definition_workspace(
    definition: usize,
) -> Result<u64, ReadOnlyReopenError> {
    let definitions = ReadOnlyResourceProfile::allocation(definition, 1)?
        .checked_mul(4)
        .ok_or(ReadOnlyReopenError::Overflow)?;
    let keys = ReadOnlyResourceProfile::allocation(PLANNED_READ_KEY_BYTES, 1)?
        .checked_mul(3)
        .ok_or(ReadOnlyReopenError::Overflow)?;
    let types = ReadOnlyResourceProfile::allocation(64, 1)?
        .checked_mul(3)
        .ok_or(ReadOnlyReopenError::Overflow)?;
    definitions
        .checked_add(keys)
        .and_then(|n| n.checked_add(types))
        .ok_or(ReadOnlyReopenError::Overflow)
}
