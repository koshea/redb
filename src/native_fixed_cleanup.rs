//! Closed native ownership join for DATA recipe and original SYSTEM cleanup.
//! Inactive until whole-family demand and first-party Source admission join.
//! Neither this owner nor its executor exports a native transaction or table.
#[path = "native_fixed_execution_buffers.rs"]
mod execution_buffers;
use super::*;
use crate::transactions::{
    NativeAllocatedPrefixWork, NativeDetachedProfiledSystemResources, NativeFixedDataCensus,
    NativeFixedReclamationBudget, NativePreparedTxnMetadata, NativeSystemDetachmentFailure,
};
use crate::tree_store::{
    NativeControlAllocation, NativeKeyPathError as E, NativeNumericalEpoch, NativePointKey,
    NativePointMutation, NativeReadContext,
};
pub(crate) use execution_buffers::{NativeFixedExecutionBufferDemand, NativeFixedExecutionBuffers};
type R<T> = core::result::Result<T, E>;
#[path = "native_fixed_data_emission.rs"]
mod data_emission;
use data_emission::NativeFixedDataEmission;
#[path = "native_fixed_system_phase.rs"]
mod system_phase;
use system_phase::NativeFixedSystemPhasePlan;

pub(crate) enum NativeFixedCleanupError {
    Path(E),
    Detachment(NativeSystemDetachmentFailure),
    Table(TableError),
    Commit(crate::CommitError),
    #[cfg(test)]
    InjectedAfterFirstPoint {
        materialized_bytes: usize,
    },
    #[cfg(test)]
    RejectedFixedCommitProfile {
        error: StorageError,
        materialized_bytes: usize,
    },
    #[cfg(test)]
    RejectedQuarantinedLocal {
        error: StorageError,
        materialized_bytes: usize,
    },
}
impl From<E> for NativeFixedCleanupError {
    fn from(error: E) -> Self {
        Self::Path(error)
    }
}
impl From<StorageError> for NativeFixedCleanupError {
    fn from(error: StorageError) -> Self {
        Self::Path(error.into())
    }
}
impl From<TableError> for NativeFixedCleanupError {
    fn from(error: TableError) -> Self {
        Self::Table(error)
    }
}
// Scalar-only deterministic fixture diagnostics. No native error text, table
// contents, keys, values, addresses or caller-supplied stage is logged.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum NativeFixedCaptureStage {
    DataAffinity,
    ReadControlAffinity,
    ActualTracker,
    MetadataNames,
    CompletionTransfer,
    AllocatedPolicy,
    SystemCapture,
    SystemPreparation,
    SystemDetachment,
    DetachedCurrent,
    DataFinal,
    SystemPhase,
    ExecutionBuffers,
}
#[cfg(not(test))]
type NativeFixedCaptureStage = ();

#[cfg(test)]
impl NativeFixedCleanupError {
    fn class_for_test(&self) -> &'static str {
        match self {
            Self::Path(E::VisitsExceeded) => "path_visits",
            Self::Path(E::UnsupportedDepth) => "path_depth",
            Self::Path(E::Unpinned) => "path_unpinned",
            Self::Path(E::ForeignOwner) => "path_foreign_owner",
            Self::Path(E::UnsupportedKey) => "path_key",
            Self::Path(E::UnsupportedState) => "path_unsupported_state",
            Self::Path(E::Epoch(_)) => "path_epoch",
            Self::Path(E::Malformed) => "path_malformed",
            Self::Path(E::Overflow) => "path_overflow",
            Self::Path(E::Native(_)) => "path_native_storage",
            Self::Detachment(_) => "detachment",
            Self::Table(_) => "table",
            Self::Commit(_) => "commit",
            Self::InjectedAfterFirstPoint { .. } => "injected_after_first_point",
            Self::RejectedQuarantinedLocal { .. } => "rejected_quarantined_local",
            Self::RejectedFixedCommitProfile { .. } => "rejected_fixed_commit_profile",
        }
    }
}

// Error first, all original inputs next, admitted overlap/error control LAST.
// Redaction may borrow the error; no take-error API severs its allowance.
pub(crate) struct NativeFixedCleanupFailure<'work> {
    error: NativeFixedCleanupError,
    #[cfg(test)]
    stage: NativeFixedCaptureStage,
    _data: Option<NativeFixedDataCensus<'work>>,
    _metadata: Option<NativePreparedTxnMetadata<'work>>,
    _budget: Option<NativeFixedReclamationBudget>,
    _control: Option<NativeControlAllocation>,
}
impl NativeFixedCleanupFailure<'_> {
    #[cfg(test)]
    fn stage_for_test(&self) -> NativeFixedCaptureStage {
        self.stage
    }

    pub(crate) fn error(&self) -> &NativeFixedCleanupError {
        &self.error
    }
}

// Constructor-private native claim. Only the closed executor can construct
// it; no raw guard/pairing API can request detached budget extraction.
pub(crate) struct NativeFixedWriterClaim<'guard> {
    guard: &'guard TransactionGuard,
}
impl NativeFixedWriterClaim<'_> {
    pub(crate) fn actual_guard(&self) -> &TransactionGuard {
        self.guard
    }
}

pub(crate) struct NativePreparedFixedCleanup<'work> {
    actual: &'work Database,
    data: NativeFixedDataCensus<'work>,
    emission: NativeFixedDataEmission,
    system_phase: NativeFixedSystemPhasePlan,
    execution_buffers: NativeFixedExecutionBuffers,
    metadata: Option<NativePreparedTxnMetadata<'work>>,
    system: NativeDetachedProfiledSystemResources<'work>,
    // Original inline owner/result/error allowance outlives every member.
    _control: NativeControlAllocation,
}
impl<'work> NativePreparedFixedCleanup<'work> {
    // The read owner supplies only the genuine scoped native pin proof.
    // No raw memory getter, caller ID/root tuple or new registration escapes.
    pub(crate) fn require_actual_read(
        actual: &Database,
        parts: &CheckedPinnedReadParts<'_>,
    ) -> R<()> {
        if !Arc::ptr_eq(&actual.mem, parts.actual_memory())
            || actual.mem.concurrency_mode() != ConcurrencyMode::ExclusiveWriter
            || parts.transaction_id() != actual.mem.get_last_committed_transaction_id()?
        {
            return Err(E::ForeignOwner);
        }
        actual
            .capture_native_resource_epoch()
            .map_err(E::Epoch)?
            .require_tracker(parts.actual_tracker())
            .map_err(E::Epoch)
    }
    pub(crate) fn capture_control_bytes() -> usize {
        core::mem::size_of::<(
            Self,
            NativeFixedCleanupFailure<'work>,
            core::result::Result<Self, NativeFixedCleanupFailure<'work>>,
            NativeFixedCleanupError,
            NativeFixedCaptureStage,
            Option<NativeControlAllocation>,
            PageResolver,
            NativeAllocatedPrefixWork,
            NativeFixedWriterClaim<'work>,
            crate::transaction_tracker::NativeResourceEpoch<'work>,
            NativeControlAllocation,
            CheckedPinnedReadParts<'work>,
            core::result::Result<
                Option<CheckedPinnedReadParts<'work>>,
                crate::transaction_tracker::NativeResourcePlanningRefusal,
            >,
            NativeFixedWriterInputs,
            NativeFixedExecutionFailure<'work>,
            WriteTransaction,
            TransactionGuard,
            NativeReadContext,
            crate::tree_store::NativePreparedDebugFamilies,
            Option<NativeFixedWriterInputs>,
            NativeFixedDataEmission,
            NativeFixedSystemPhasePlan,
            Option<NativeFixedSystemPhasePlan>,
            core::result::Result<NativeFixedSystemPhasePlan, E>,
            core::result::Result<NativeFixedDataEmission, E>,
            crate::transactions::TransactionIdWithPagination,
            crate::transactions::TransactionIdWithPagination,
            &crate::transactions::NativeFreedAppendPositions,
            &crate::transactions::TransactionIdWithPagination,
        )>() + WriteTransaction::fixed_commit_profile_capture_control_bytes()
    }
    pub(crate) fn capture(
        actual: &'work Database,
        read: &ReadTransaction,
        data: NativeFixedDataCensus<'work>,
        metadata: NativePreparedTxnMetadata<'work>,
        budget: NativeFixedReclamationBudget,
        numerical: &NativeNumericalEpoch,
    ) -> core::result::Result<Self, NativeFixedCleanupFailure<'work>> {
        let data = Some(data);
        let mut metadata = Some(metadata);
        let mut budget = Some(budget);
        let mut control = None;
        #[cfg(test)]
        let mut stage = NativeFixedCaptureStage::DataAffinity;
        let result = (|| -> core::result::Result<(NativeDetachedProfiledSystemResources<'work>, NativeFixedDataEmission, NativeFixedSystemPhasePlan, NativeFixedExecutionBuffers), NativeFixedCleanupError> {
            let data = data.as_ref().expect("original DATA input");
            data.require_fixed_cleanup_pool(numerical)?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::ReadControlAffinity; }
            control = Some(read.admit_fixed_cleanup_capture_control(actual, numerical)?);
            let (recipe, original_epoch) = data.fixed_cleanup_parts();
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::ActualTracker; }
            original_epoch.require_tracker(&actual.transaction_tracker).map_err(E::Epoch)?;
            let metadata = metadata.as_mut().expect("original metadata input");
            let budget = budget.as_mut().expect("original budget input");
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::MetadataNames; }
            metadata.require_fixed_recipe_names(recipe)?;
            // SAME original pair moves before inspection. No second pair is
            // constructed, and failure keeps the receiving budget alive.
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::CompletionTransfer; }
            metadata.move_completed_into(&actual.mem, numerical.resources(), budget)?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::AllocatedPolicy; }
            let work = NativeAllocatedPrefixWork::for_checked_fixed_recipe(data, numerical)?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::SystemCapture; }
            let inspected = read.capture_native_system_read(original_epoch)?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::SystemPreparation; }
            let prepared = inspected.prepare_profiled_system_with_allocated_prefix(budget, work)?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::SystemDetachment; }
            let detached = prepared.into_detached(actual, original_epoch)
                .map_err(NativeFixedCleanupError::Detachment)?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::DetachedCurrent; }
            detached.require_current()?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::DataFinal; }
            data.require_fixed_cleanup_pool(numerical)?;
            let emission = NativeFixedDataEmission::capture(data, &detached, numerical)?;
            metadata.require_data_freed_upper(emission.freed_pages_upper())?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::SystemPhase; }
            let system_phase = NativeFixedSystemPhasePlan::capture(data, &emission, &detached, numerical)?;
            metadata.require_system_freed_upper(system_phase.freed_capacity_upper())?;
            #[cfg(test)]
            { stage = NativeFixedCaptureStage::ExecutionBuffers; }
            let demand = NativeFixedExecutionBufferDemand::from_bound_owners(data, &system_phase)?;
            let execution_buffers = read.prepare_fixed_execution_buffers(actual, numerical, &demand)?;
            Ok((detached, emission, system_phase, execution_buffers))
        })();
        match result {
            Ok((system, emission, system_phase, execution_buffers)) => Ok(Self {
                actual,
                data: data.expect("original DATA input"),
                emission,
                system_phase,
                execution_buffers,
                metadata,
                system,
                _control: control.expect("admitted combined control"),
            }),
            Err(error) => Err(NativeFixedCleanupFailure {
                error,
                #[cfg(test)]
                stage,
                _data: data,
                _metadata: metadata,
                _budget: budget,
                _control: control,
            }),
        }
    }
    pub(crate) fn require_current(&self, numerical: &NativeNumericalEpoch) -> R<()> {
        self.data.require_fixed_cleanup_pool(numerical)?;
        self.system_phase
            .require_for(&self.data, &self.emission, &self.system, numerical)?;
        self.emission
            .require_for(&self.data, &self.system, numerical)?;
        if !self._control.belongs_to(numerical.resources()) {
            return Err(E::ForeignOwner);
        }
        self.system.require_current()?;
        self.data.require_fixed_cleanup_pool(numerical)
    }

    // This native-internal execution seam still needs the complete demand
    // producer. Its fields-private inputs are not a public quota/authority API.
    // Every successful invocation exercises the REAL early Ignore constructor
    // and native Immediate main/post-only commit algorithms.
    pub(crate) fn execute(
        mut self,
        numerical: &NativeNumericalEpoch,
        inputs: NativeFixedWriterInputs,
    ) -> core::result::Result<(), NativeFixedExecutionFailure<'work>> {
        let mut inputs = Some(inputs);
        let result = (|| -> core::result::Result<(), NativeFixedCleanupError> {
            self.require_current(numerical)?;
            let input = inputs.as_ref().unwrap();
            if !input.context.resources.same_pool(numerical.resources()) {
                return Err(E::ForeignOwner.into());
            }
            PageResolver::with_read_context(self.actual.mem.clone(), input.context.clone())
                .seal_fixed_observation_admission()?
                .require_current()?;
            self.actual.mem.check_io_errors()?;
            self.execution_buffers
                .require_available(numerical.resources())?;
            let (_, epoch) = self.data.fixed_cleanup_parts();
            let slot = self
                .actual
                .transaction_tracker
                .try_claim_fixed_resource_write_slot(epoch)
                .map_err(E::Epoch)?;
            let writer_lock = self.actual.mem.lock_writer()?;
            let latch = sync_to_latest_commit(&self.actual.mem, &writer_lock, None)?;
            if latch.is_some() || self.actual.mem.allocator_state_invalidated() {
                return Err(E::UnsupportedState.into());
            }
            self.actual.mem.check_io_errors()?;
            let id = self.actual.transaction_tracker.issue_write_transaction_id(
                self.actual.mem.get_last_committed_transaction_id()?,
                &writer_lock,
            );
            let guard = TransactionGuard::new_write(id, slot, writer_lock);
            self.actual
                .transaction_tracker
                .require_prepared_resource_writer(epoch, id)
                .map_err(E::Epoch)?;
            self.execution_buffers
                .require_available(numerical.resources())?;
            self.data.require_fixed_cleanup_serial(numerical)?;
            let budget = {
                // This private claim cannot escape or survive guard transfer.
                let claim = NativeFixedWriterClaim { guard: &guard };
                self.system.take_for_fixed_writer(&claim)?
            };
            let input = inputs.take().unwrap();
            #[cfg(test)]
            let fail_after_first_point = input.fail_after_first_point;
            #[cfg(test)]
            let quarantine_before_commit = input.quarantine_before_commit;
            #[cfg(test)]
            let profile_drift = input.profile_drift;
            #[cfg(test)]
            let phase_witness = input.phase_witness;
            #[cfg(test)]
            let phase_fault = input.phase_fault;
            let prepared_debug = self.actual.mem.prepare_source_debug_families(
                &self.actual.transaction_tracker,
                &guard,
                &input.context.resources,
                input.additional_new_pages,
                input.concurrent_mutable_pages,
            )?;
            let metadata = self.metadata.take().ok_or(E::UnsupportedState)?;
            // Complete all family/control checks happen before either namespace
            // can allocate. The early constructor itself rechecks the profile.
            metadata.require_with_reclamation(
                &self.actual.mem,
                &input.context.resources,
                &budget,
            )?;
            let transaction = WriteTransaction::new_for_checked_source_immediate(
                guard,
                self.actual.transaction_tracker.clone(),
                self.actual.mem.clone(),
                input.context,
                input.total_requests,
                prepared_debug,
                self.execution_buffers.clone(),
                metadata,
                budget,
            )?;
            #[cfg(test)]
            let mut transaction = transaction;
            #[cfg(test)]
            if let Some(witness) = phase_witness {
                transaction.install_fixed_phase_witness_for_fixture(witness);
            }
            #[cfg(test)]
            if let Some(fault) = phase_fault {
                transaction.install_fixed_phase_fault_for_fixture(fault);
            }
            let (recipe, _) = self.data.fixed_cleanup_parts();
            for index in 0..recipe.points_len() {
                let point = recipe.point(index).ok_or(E::Malformed)?;
                // One table and one returned old-value guard at a time. No
                // output collection, raw handle, callback or configuration API.
                match point.mutation {
                    NativePointMutation::Put {
                        key: NativePointKey::Bytes(key),
                        value,
                    } => {
                        let mut table = transaction
                            .open_table(TableDefinition::<&[u8], &[u8]>::new(point.table))?;
                        drop(table.insert(key, value)?);
                    }
                    NativePointMutation::Delete {
                        key: NativePointKey::Bytes(key),
                    } => {
                        let mut table = transaction
                            .open_table(TableDefinition::<&[u8], &[u8]>::new(point.table))?;
                        drop(table.remove(key)?);
                    }
                    NativePointMutation::Put {
                        key: NativePointKey::Str(key),
                        value,
                    } => {
                        let mut table = transaction
                            .open_table(TableDefinition::<&str, &[u8]>::new(point.table))?;
                        drop(table.insert(key, value)?);
                    }
                    NativePointMutation::Delete {
                        key: NativePointKey::Str(key),
                    } => {
                        let mut table = transaction
                            .open_table(TableDefinition::<&str, &[u8]>::new(point.table))?;
                        drop(table.remove(key)?);
                    }
                }
                #[cfg(test)]
                if fail_after_first_point && index == 0 {
                    // All real mutation/old-guard/table work ABOVE succeeded.
                    // The closure's error drops the actual writer and performs
                    // its normal rollback before returning the owning error.
                    // Numerical observation cannot alter admission or commit.
                    return Err(NativeFixedCleanupError::InjectedAfterFirstPoint {
                        materialized_bytes: numerical
                            .resources()
                            .materialized_observation_for_test()
                            .0,
                    });
                }
            }
            self.data.require_fixed_cleanup_serial(numerical)?;
            #[cfg(test)]
            if quarantine_before_commit {
                // A genuine currently committed PN is retained as scalar debt;
                // this hook neither opens nor frees/adopts that native page.
                let page = self.actual.mem.get_data_root().ok_or(E::Malformed)?.root;
                transaction.quarantine_fixed_local_for_fixture(page)?;
            }
            transaction
                .require_fixed_local_integrity()
                .map_err(|error| {
                    #[cfg(test)]
                    if quarantine_before_commit {
                        return NativeFixedCleanupError::RejectedQuarantinedLocal {
                            error,
                            materialized_bytes: numerical
                                .resources()
                                .materialized_observation_for_test()
                                .0,
                        };
                    }
                    NativeFixedCleanupError::from(error)
                })?;
            {
                // Borrowed witness ends before consuming the real writer. It
                // rechecks actual fields/slot/Ignore lineage and managed serial,
                // plus the original typed append positions retained by this owner.
                #[cfg(test)]
                if let Some(drift) = profile_drift {
                    // Genuine public setters affect the actual owned writer,
                    // after both real points. They are test-only fault input;
                    // no setter/callback/raw transaction escapes the executor.
                    match drift {
                        NativeFixedProfileDrift::QuickRepair => transaction.set_quick_repair(true),
                        NativeFixedProfileDrift::NonDurable => transaction
                            .set_durability(crate::Durability::None)
                            .expect("actual ExclusiveWriter fixture permits None"),
                    }
                }
                let profile = transaction
                    .capture_fixed_commit_profile()
                    .map_err(|error| {
                        #[cfg(test)]
                        if profile_drift.is_some() {
                            return NativeFixedCleanupError::RejectedFixedCommitProfile {
                                error,
                                materialized_bytes: numerical
                                    .resources()
                                    .materialized_observation_for_test()
                                    .0,
                            };
                        }
                        NativeFixedCleanupError::from(error)
                    })?;
                self.system_phase
                    .require_for_writer(&self.emission, &profile)?;
            }
            // commit_inner's actual DATA allocated output check and the token's
            // pre-flush profile check prove ZERO allocation-row emission before
            // irreversible adoption. Post-only Source phase records SYS only.
            #[cfg(test)]
            transaction.observe_fixed_phases_for_test(numerical.resources().clone());
            transaction
                .commit()
                .map_err(NativeFixedCleanupError::Commit)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(NativeFixedExecutionFailure {
                error,
                _owner: self,
                _inputs: inputs,
            }),
        }
    }
}

// An incomplete physical-demand theorem must not be turned into a production
// constructor with guessed counts. The whole-factory join will mint this from
// its inseparable complete recipe bounds; fixture construction is explicit.
#[cfg(test)]
#[derive(Clone, Copy)]
enum NativeFixedProfileDrift {
    QuickRepair,
    NonDurable,
}
pub(crate) struct NativeFixedWriterInputs {
    context: NativeReadContext,
    total_requests: usize,
    additional_new_pages: usize,
    concurrent_mutable_pages: usize,
    #[cfg(test)]
    fail_after_first_point: bool,
    #[cfg(test)]
    quarantine_before_commit: bool,
    #[cfg(test)]
    profile_drift: Option<NativeFixedProfileDrift>,
    #[cfg(test)]
    phase_witness: Option<
        crate::tree_store::NativeOwnedShared<
            crate::sync::Mutex<crate::transactions::NativeFixedPhaseWitness>,
        >,
    >,
    #[cfg(test)]
    phase_fault: Option<crate::transactions::NativeFixedPhaseFault>,
}
#[cfg(test)]
impl NativeFixedWriterInputs {
    pub(super) fn for_fixture(
        context: NativeReadContext,
        total_requests: usize,
        additional_new_pages: usize,
        concurrent_mutable_pages: usize,
    ) -> Self {
        Self {
            context,
            total_requests,
            additional_new_pages,
            concurrent_mutable_pages,
            fail_after_first_point: false,
            quarantine_before_commit: false,
            profile_drift: None,
            phase_witness: None,
            phase_fault: None,
        }
    }
    fn with_phase_fault(mut self, fault: crate::transactions::NativeFixedPhaseFault) -> Self {
        self.phase_fault = Some(fault);
        self
    }
    fn with_phase_witness(
        mut self,
        witness: crate::tree_store::NativeOwnedShared<
            crate::sync::Mutex<crate::transactions::NativeFixedPhaseWitness>,
        >,
    ) -> Self {
        self.phase_witness = Some(witness);
        self
    }
    fn drift_profile_before_consuming_commit(mut self, drift: NativeFixedProfileDrift) -> Self {
        self.profile_drift = Some(drift);
        self
    }
    fn quarantine_before_consuming_commit(mut self) -> Self {
        self.quarantine_before_commit = true;
        self
    }
    fn fail_after_first_real_point(mut self) -> Self {
        self.fail_after_first_point = true;
        self
    }
}
pub(crate) struct NativeFixedExecutionFailure<'work> {
    error: NativeFixedCleanupError,
    _owner: NativePreparedFixedCleanup<'work>,
    _inputs: Option<NativeFixedWriterInputs>,
}
impl NativeFixedExecutionFailure<'_> {
    pub(crate) fn error(&self) -> &NativeFixedCleanupError {
        &self.error
    }
}

#[cfg(test)]
#[path = "native_fixed_cleanup_tests.rs"]
mod tests;
