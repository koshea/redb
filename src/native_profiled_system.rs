//! One closed consuming preparation; numerical shape only, never Source authority.
use super::{NativePreparedSystemResources, NativeSystemRead, PathResult};
use crate::transactions::bounded_reclamation::ReclamationBudget;
use crate::tree_store::NativeSystemNamespaceFacts;

pub(crate) struct NativePreparedProfiledSystemResources<'read, 'epoch, 'tracker> {
    system: NativePreparedSystemResources<'read, 'epoch, 'tracker>,
    namespace: Option<NativeSystemNamespaceFacts>,
}
impl<'read, 'epoch, 'tracker> NativePreparedProfiledSystemResources<'read, 'epoch, 'tracker> {
    pub(crate) fn require_before_write(&self) -> PathResult<()> {
        self.system.require_before_write()
    }
    pub(crate) fn system_resources(
        &self,
    ) -> &NativePreparedSystemResources<'read, 'epoch, 'tracker> {
        &self.system
    }
    pub(crate) fn namespace_facts(&self) -> &NativeSystemNamespaceFacts {
        self.namespace
            .as_ref()
            .expect("private prepared namespace is present until consumption")
    }
}
// Typed failure owns any original native error before its control allowance.
// Only borrowed inspection is provided for first-party redaction/destruction.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeSystemDetachmentFailure {
    error: crate::tree_store::NativeKeyPathError,
    _control: Option<crate::tree_store::NativeControlAllocation>,
}
#[cfg(not(redb_no_std))]
impl NativeSystemDetachmentFailure {
    pub(crate) fn error(&self) -> &crate::tree_store::NativeKeyPathError {
        &self.error
    }
}
#[cfg(not(redb_no_std))]
pub(crate) struct NativeDetachedProfiledSystemResources<'work> {
    admission: crate::tree_store::NativeFixedObservationAdmission,
    system: super::allocator_purge::NativeDetachedSystemResources<'work>,
    namespace: NativeSystemNamespaceFacts,
    // LAST: all moved values and private inline controls die before refund.
    _control: crate::tree_store::NativeControlAllocation,
}
#[cfg(not(redb_no_std))]
impl NativeDetachedProfiledSystemResources<'_> {
    pub(crate) fn phase_forests(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> PathResult<[crate::tree_store::NativeFreedPhaseForest; 3]> {
        self.require_current()?;
        let forests = self.system.phase_forests(resources)?;
        self.require_current()?;
        Ok(forests)
    }

    pub(crate) fn take_for_fixed_writer(
        &mut self,
        claim: &crate::db::NativeFixedWriterClaim<'_>,
    ) -> PathResult<ReclamationBudget> {
        self.admission.require_current()?;
        self.system.take_for_fixed_writer(claim)
    }

    pub(crate) fn require_current(&self) -> PathResult<()> {
        self.admission.require_current()?;
        self.system.require_current()?;
        self.admission.require_current()?;
        Ok(())
    }
    pub(crate) fn selected_prefix(&self) -> PathResult<&super::NativeSelectedFreedPrefix> {
        self.require_current()?;
        self.system.selected_prefix()
    }
    pub(crate) fn append_positions(&self) -> PathResult<&super::NativeFreedAppendPositions> {
        self.require_current()?;
        self.system.append_positions()
    }
    pub(crate) fn namespace_facts(&self) -> PathResult<&NativeSystemNamespaceFacts> {
        self.require_current()?;
        Ok(&self.namespace)
    }
    pub(crate) fn purge_facts(&self) -> PathResult<&crate::tree_store::NativeAllocatorPurgeFacts> {
        self.require_current()?;
        Ok(self.system.purge_facts())
    }
    pub(crate) fn capture_control_bytes() -> PathResult<usize> {
        // This is the actual fixed detachment representation and overlapping
        // result/failure/capture locals, not a physical page or full-plan quote.
        Ok(core::mem::size_of::<(
            NativeDetachedProfiledSystemResources<'static>,
            core::result::Result<
                NativeDetachedProfiledSystemResources<'static>,
                NativeSystemDetachmentFailure,
            >,
            NativeSystemDetachmentFailure,
            crate::db::NativePreparedSystemBinding<'static>,
            Option<crate::tree_store::NativeControlAllocation>,
            crate::tree_store::PageResolver,
            crate::tree_store::NativeFixedObservationAdmission,
            crate::db::CheckedPinnedReadParts<'static>,
            crate::transaction_tracker::NativeResourceEpoch<'static>,
            Option<crate::tree_store::BtreeHeader>,
            crate::transaction_tracker::TransactionId,
            (
                crate::transaction_tracker::TransactionId,
                crate::transaction_tracker::TransactionId,
            ),
        )>())
    }
}
#[cfg(not(redb_no_std))]
impl<'read, 'epoch, 'work> NativePreparedProfiledSystemResources<'read, 'epoch, 'work> {
    pub(crate) fn into_detached(
        mut self,
        actual: &'work crate::Database,
        original: &crate::transaction_tracker::NativeResourceEpoch<'work>,
    ) -> core::result::Result<
        NativeDetachedProfiledSystemResources<'work>,
        NativeSystemDetachmentFailure,
    > {
        let mut control = None;
        let result = (|| -> PathResult<NativeDetachedProfiledSystemResources<'work>> {
            // The existing actual read's managed Source context is the sole
            // allocator. No independently paired numerical pool is accepted.
            let resolver = self.system.reclamation_resolver();
            control = Some(resolver.admit_fixed_observation(
                NativeDetachedProfiledSystemResources::capture_control_bytes()?,
            )?);
            let admission = resolver.seal_fixed_observation_admission()?;
            self.require_before_write()?;
            if self.namespace.is_none() {
                return Err(crate::tree_store::NativeKeyPathError::UnsupportedState);
            }
            let system = self.system.detach_parts(actual, original)?;
            Ok(NativeDetachedProfiledSystemResources {
                admission,
                system,
                namespace: self.namespace.take().expect("checked private namespace"),
                _control: control.take().expect("admitted private detachment control"),
            })
        })();
        match result {
            Ok(value) => Ok(value),
            Err(error) => Err(NativeSystemDetachmentFailure {
                error,
                _control: control,
            }),
        }
    }
}

impl Drop for NativePreparedProfiledSystemResources<'_, '_, '_> {
    fn drop(&mut self) {}
}
impl<'read, 'epoch, 'tracker> NativeSystemRead<'read, 'epoch, 'tracker> {
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_profiled_system_with_freed_frontiers(
        self,
        budget: &mut ReclamationBudget,
    ) -> PathResult<NativePreparedProfiledSystemResources<'read, 'epoch, 'tracker>> {
        let namespace = self.inspect_namespace_profile()?.into_inline_facts()?;
        // One actual inspector/purge producer, same typed frontier observer,
        // one budget/fence and existing physical inspection teardown.
        let system = self.prepare_system_resources_with_freed_frontiers(budget)?;
        let prepared = NativePreparedProfiledSystemResources {
            system,
            namespace: Some(namespace),
        };
        prepared.require_before_write()?;
        Ok(prepared)
    }
    pub(crate) fn prepare_profiled_system_resources(
        self,
        budget: &mut ReclamationBudget,
    ) -> PathResult<NativePreparedProfiledSystemResources<'read, 'epoch, 'tracker>> {
        // SAME genuine inspector/epoch/root, before either consuming path can
        // release exclusion. The temporary namespace borrow dies here; only its
        // inline scalar facts move into the eventual returned native owner.
        let namespace = self.inspect_namespace_profile()?.into_inline_facts()?;
        // Existing preparation seals complete allocator facts and both freed
        // families under ONE budget. It physically destroys all cached SYSTEM
        // pages, drops inspection last, then rechecks the original actual pin.
        let system = self.prepare_system_resources(budget)?;
        let prepared = NativePreparedProfiledSystemResources {
            system,
            namespace: Some(namespace),
        };
        prepared.require_before_write()?;
        Ok(prepared)
    }
}
#[cfg(all(test, not(redb_no_std)))]
#[path = "native_profiled_system_tests.rs"]
mod tests;

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_system_detachment_tests.rs"]
mod detachment_tests;
