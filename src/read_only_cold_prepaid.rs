//! PRIVATE whole cold construction. No entry/child/parts are exported.
//! This is not first-party Source eligibility or a complete Source64 factory.
use super::*;
use crate::tree_store::page_store::{
    cold_read_fixed_capacity_bytes, native_cold_fixed_error_work_bytes,
    prepare_cold_read_admission, prepare_cold_read_error_work, prepare_cold_read_fixed,
};
use crate::tree_store::{
    NativeControlAllocation, NativeFixedObservationAdmission, NativeNumericalEpoch,
};

// All owned error/native values precede every construction allowance. Neither
// success nor failure exposes a maintenance read, table, raw Arc or byte alias.
struct ColdParts {
    error: Option<ReadOnlyReopenError>,
    database: Option<ReadOnlyDatabase>,
    memory: Option<Arc<TransactionalMemory>>,
    tracker: Option<Arc<TransactionTracker>>,
    planned: Option<PlannedReadOnlyDatabase>,
    plan: Option<ReadOnlyReopenResourcePlan>,
    custody: Option<ReadOnlyReopenFileCustody>,
    incoming: Option<File>,
    database_resources: Option<NativePageResources>,
    fixed: Option<NativeControlAllocation>,
    error_work: Option<NativeControlAllocation>,
    admission: Option<NativeFixedObservationAdmission>,
    resources: NativePageResources,
    #[cfg(test)]
    retired_phase_observation: Option<(usize, usize, usize)>,
}
impl Drop for ColdParts {
    fn drop(&mut self) {
        drop(self.error.take());
        drop(self.database.take());
        drop(self.tracker.take());
        drop(self.memory.take());
        drop(self.planned.take());
        drop(self.plan.take());
        drop(self.custody.take());
        drop(self.incoming.take());
        drop(self.database_resources.take());
        // All native/raw Arc shells and owned errors have now physically retired.
        drop(self.fixed.take());
        drop(self.error_work.take());
    }
}
struct PrepaidColdFailure {
    parts: ColdParts,
}
struct PrepaidMaintenanceOwner {
    parts: ColdParts,
}
struct PrepaidPlannedOwner {
    parts: ColdParts,
}

impl ColdParts {
    fn new(file: File, resources: NativePageResources) -> Self {
        Self {
            error: None,
            database: None,
            memory: None,
            tracker: None,
            planned: None,
            plan: None,
            custody: None,
            incoming: Some(file),
            database_resources: None,
            fixed: None,
            error_work: None,
            admission: None,
            resources,
            #[cfg(test)]
            retired_phase_observation: None,
        }
    }
    fn fail(mut self, error: ReadOnlyReopenError) -> PrepaidColdFailure {
        self.error = Some(error);
        PrepaidColdFailure { parts: self }
    }
    fn require_current(&self) -> ReopenResult<()> {
        self.admission
            .as_ref()
            .ok_or(ReadOnlyReopenError::Unsupported)?
            .require_for(&self.resources)
            .map_err(ReadOnlyReopenError::Storage)
    }
    fn prepare_fixed(&mut self, profile: Option<ReadOnlyResourceProfile>) -> ReopenResult<()> {
        self.require_current()?;
        if self.fixed.is_some() {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        self.fixed = Some(prepare_cold_read_fixed(&self.resources, profile)?);
        Ok(())
    }
    // Shared normal readonly construction after a real prepaid custody lease.
    // The actual memory is installed here BEFORE the first fallible table/type
    // decode; its admitted definition/error work survives any returned failure.
    fn construct_database(&mut self, profile: Option<ReadOnlyResourceProfile>) -> ReopenResult<()> {
        self.require_current()?;
        if self.memory.is_some() || self.database.is_some() || self.tracker.is_some() {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let (page_size, custody) = match &self.plan {
            Some(plan) => (plan.page_size, &plan.custody),
            None => (
                crate::tree_store::PAGE_SIZE,
                self.custody
                    .as_ref()
                    .ok_or(ReadOnlyReopenError::Unsupported)?,
            ),
        };
        let lease = custody.lease().map_err(StorageError::from)?;
        let backend = Box::new(ReadOnlyBackend::new(Box::new(lease)));
        let mem = TransactionalMemory::new_prepaid_read_resources(
            backend,
            page_size,
            &self.resources,
            profile,
        )?;
        self.memory = Some(Arc::new(mem));
        let mem = self.memory.as_ref().unwrap();
        // Sole original table/type/staleness algorithm; normal native errors stay native.
        let tree = crate::Database::get_allocator_state_table(mem)?
            .ok_or(ReadOnlyReopenError::Native(DatabaseError::RepairAborted))?;
        mem.load_allocator_state_source(&tree, &self.resources)?;
        drop(tree);
        let next = mem.get_last_committed_transaction_id()?.next();
        self.tracker = Some(Arc::new(TransactionTracker::new(next)));
        // All fallible checks end before the first field take.
        self.require_current()?;
        let reopened_custody = match &self.plan {
            Some(plan) => Some(plan.custody.clone_file_custody()),
            None => self.custody.take(),
        };
        let (Some(mem), Some(transaction_tracker)) = (self.memory.take(), self.tracker.take())
        else {
            unreachable!("private construction owns the completed memory and tracker")
        };
        self.database = Some(ReadOnlyDatabase {
            mem,
            transaction_tracker,
            reopen_custody: reopened_custody,
            page_resources: Some(self.resources.clone()),
        });
        Ok(())
    }
    fn retire_maintenance_phase(&mut self) -> ReopenResult<()> {
        self.require_current()?;
        let db = self
            .database
            .as_ref()
            .ok_or(ReadOnlyReopenError::Unsupported)?;
        let plan = self.plan.as_ref().ok_or(ReadOnlyReopenError::Unsupported)?;
        db.validate_read_only_reopen_handoff(plan)?;
        // This SOURCE-only whole-chain owner has NO native maintenance child or
        // byte escape. Shared native binding/strong-count checks still execute.
        let aliases = db.mem.readonly_alias_drain();
        let Some(ReadOnlyDatabase {
            mem,
            transaction_tracker,
            reopen_custody,
            page_resources,
        }) = self.database.take()
        else {
            unreachable!("validated private maintenance database is present")
        };
        // Infallible transfer into the intact transition guard before a drain.
        self.memory = Some(mem);
        self.tracker = Some(transaction_tracker);
        self.custody = reopen_custody;
        self.database_resources = page_resources;
        drop(self.tracker.take());
        drop(self.memory.take());
        drop(self.database_resources.take());
        aliases.try_into_drained()?;
        drop(self.custody.take());
        self.plan.as_ref().unwrap().verify_exclusive_handoff()?;
        // The original aggregate is intentionally NOT drained: the real plan's
        // prepaid custody and caller's original epoch retain it. Only this
        // closed old phase's actual graph has retired, before a new allocation.
        drop(self.fixed.take());
        #[cfg(test)]
        {
            let (bytes, _, buffers, _) = self.resources.materialized_observation_for_test();
            self.retired_phase_observation = Some((self.resources.current(), bytes, buffers));
        }
        Ok(())
    }
}

fn open_prepaid_maintenance_file(
    file: File,
    original_epoch: &NativeNumericalEpoch,
) -> Result<PrepaidMaintenanceOwner, PrepaidColdFailure> {
    let mut parts = ColdParts::new(file, original_epoch.resources().clone());
    let result = (|| -> ReopenResult<()> {
        original_epoch.require_open()?;
        if cfg!(any(feature = "logging", feature = "experimental-api-5")) {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        parts.admission = Some(prepare_cold_read_admission(original_epoch)?);
        // This error floor precedes even the first native lock/lease failure.
        parts.error_work = Some(prepare_cold_read_error_work(&parts.resources)?);
        parts.prepare_fixed(None)?;
        let file = parts.incoming.take().unwrap();
        parts.custody = Some(ReadOnlyReopenFileCustody::acquire_prepaid(
            file,
            &parts.resources,
        )?);
        parts.construct_database(None)?;
        Ok(())
    })();
    match result {
        Ok(()) => Ok(PrepaidMaintenanceOwner { parts }),
        Err(error) => Err(parts.fail(error)),
    }
}
impl PrepaidMaintenanceOwner {
    fn into_planned_read_only(self) -> Result<PrepaidPlannedOwner, PrepaidColdFailure> {
        let Self { mut parts } = self;
        let result = (|| -> ReopenResult<()> {
            parts.require_current()?;
            parts.plan = Some(parts.database.as_ref().unwrap().plan_read_only_reopen()?);
            parts.retire_maintenance_phase()?;
            // Same original epoch/counter. No new numerical pool and no admission
            // token can be caller fabricated from the completed plan's numbers.
            let plan = parts.plan.as_ref().unwrap();
            plan.verify_exclusive_handoff()?;
            let profile = plan.profile;
            parts.prepare_fixed(Some(profile))?;
            parts.construct_database(Some(profile))?;
            let db = parts.database.as_ref().unwrap();
            db.mem.readonly_policy_is_empty()?;
            if db.mem.allocator_reopen_component()?.facts() != profile.allocator_facts() {
                return Err(ReadOnlyReopenError::Stale);
            }
            parts.plan.as_ref().unwrap().verify_file()?;
            parts.require_current()?;
            let (Some(inner), Some(plan)) = (parts.database.take(), parts.plan.take()) else {
                unreachable!("private planned construction owns both checked values")
            };
            parts.planned = Some(PlannedReadOnlyDatabase { inner, plan });
            Ok(())
        })();
        match result {
            Ok(()) => Ok(PrepaidPlannedOwner { parts }),
            Err(error) => Err(parts.fail(error)),
        }
    }
}

#[cfg(test)]
impl core::fmt::Debug for PrepaidColdFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("PrepaidColdFailure")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, TableDefinition};
    use std::fs::OpenOptions;
    const DATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_values");
    const LIMIT: usize = 64 * 1024 * 1024; // unchanged aggregate ceiling; numerical component only
    fn fixture() -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::create(file.path()).unwrap();
        let write = db.begin_write().unwrap();
        write
            .open_table(DATA)
            .unwrap()
            .insert(b"key".as_slice(), b"actual".as_slice())
            .unwrap();
        write.commit().unwrap();
        drop(db);
        file
    }
    fn outsider_refused(path: &std::path::Path) {
        assert!(matches!(
            Database::open(path),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
        let sibling = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert!(matches!(
            sibling.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
    }
    fn pool(limit: usize) -> crate::NativeNumericalPool {
        crate::NativeNumericalPool::from_retained_data(limit, ()).unwrap()
    }

    // req: STO-020, REC-001, REC-003, REC-005, PRJ-004
    #[test]
    fn native_prepaid_cold_chain_fixed_n_minus_one_refuses_before_file_custody() {
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let error = native_cold_fixed_error_work_bytes().unwrap();
        let fixed = cold_read_fixed_capacity_bytes(None).unwrap();
        let limit = error.checked_add(fixed).unwrap() - 1;
        let pool = pool(limit);
        let epoch = pool.begin_managed_epoch(limit).unwrap();
        let failure = match open_prepaid_maintenance_file(File::open(file.path()).unwrap(), &epoch)
        {
            Ok(_) => panic!("n-1 must refuse before custody/boxes/cache"),
            Err(failure) => failure,
        };
        assert!(failure.parts.custody.is_none());
        assert!(failure.parts.memory.is_none());
        assert!(failure.parts.database.is_none());
        assert!(failure.parts.fixed.is_none());
        assert!(
            matches!(failure.parts.error.as_ref(), Some(ReadOnlyReopenError::Storage(StorageError::Io(e))) if e.kind()==std::io::ErrorKind::OutOfMemory)
        );
        assert_eq!(pool.reserved_bytes(), error);
        drop(failure);
        assert_eq!(pool.reserved_bytes(), 0);
        drop(epoch);
        drop(ReadOnlyReopenFileCustody::acquire(File::open(file.path()).unwrap()).unwrap());
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }

    // req: STO-020, REC-001, REC-003, REC-005, PRJ-004
    #[test]
    fn native_prepaid_cold_chain_real_allocator_plan_reopens_same_pool_after_old_phase_retirement()
    {
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let pool = pool(LIMIT);
        let epoch = pool.begin_managed_epoch(LIMIT).unwrap();
        let owner =
            open_prepaid_maintenance_file(File::open(file.path()).unwrap(), &epoch).unwrap();
        let db = owner.parts.database.as_ref().unwrap();
        let facts = db.mem.allocator_reopen_component().unwrap().facts();
        assert!(facts.parse_facts().region_records() > 0);
        assert!(facts.parsed().buffer_count() > 0);
        outsider_refused(file.path());
        let planned = owner.into_planned_read_only().unwrap();
        let (paid_persistent, old_page_bytes, old_page_buffers) =
            planned.parts.retired_phase_observation.unwrap();
        assert!(
            paid_persistent > 0,
            "actual file custody/error work remain paid across D4"
        );
        assert_eq!(old_page_bytes, 0);
        assert_eq!(old_page_buffers, 0);
        assert!(
            planned
                .parts
                .planned
                .as_ref()
                .unwrap()
                .inner
                .mem
                .allocator_reopen_component()
                .unwrap()
                .facts()
                .parse_facts()
                .region_records()
                > 0
        );
        assert!(pool.reserved_bytes() > paid_persistent);
        outsider_refused(file.path());
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
        drop(planned);
        assert_eq!(pool.reserved_bytes(), 0);
        drop(epoch);
        drop(ReadOnlyReopenFileCustody::acquire(File::open(file.path()).unwrap()).unwrap());
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }

    // req: STO-020, REC-001, REC-003, REC-005, PRJ-004
    #[test]
    fn native_prepaid_cold_chain_owned_empty_file_error_retains_custody_and_refunds_after_error_drop()
     {
        let file = tempfile::NamedTempFile::new().unwrap();
        let pool = pool(LIMIT);
        let epoch = pool.begin_managed_epoch(LIMIT).unwrap();
        let failure = match open_prepaid_maintenance_file(File::open(file.path()).unwrap(), &epoch)
        {
            Ok(_) => panic!("empty readonly file must preserve the native refusal"),
            Err(failure) => failure,
        };
        assert!(
            matches!(failure.parts.error.as_ref(), Some(ReadOnlyReopenError::Native(DatabaseError::Storage(StorageError::Io(e)))) if e.kind()==std::io::ErrorKind::InvalidData)
        );
        assert!(failure.parts.custody.is_some());
        assert!(pool.reserved_bytes() >= native_cold_fixed_error_work_bytes().unwrap());
        assert!(matches!(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(file.path())
                .unwrap()
                .try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        assert_eq!(file.as_file().metadata().unwrap().len(), 0);
        drop(failure);
        assert_eq!(pool.reserved_bytes(), 0);
        let sibling = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        sibling.try_lock().unwrap();
        sibling.unlock().unwrap();
        assert_eq!(file.as_file().metadata().unwrap().len(), 0);
    }

    // req: STO-020, REC-001, REC-003, REC-005, PRJ-004
    #[test]
    fn native_prepaid_cold_chain_epoch_close_refuses_transition_and_keeps_actual_graph_until_failure_drop()
     {
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let pool = pool(LIMIT);
        let epoch = pool.begin_managed_epoch(LIMIT).unwrap();
        let owner =
            open_prepaid_maintenance_file(File::open(file.path()).unwrap(), &epoch).unwrap();
        let paid = pool.reserved_bytes();
        let closed = epoch.close().unwrap();
        let failure = match owner.into_planned_read_only() {
            Ok(_) => panic!("a closed original serial must refuse before new construction"),
            Err(failure) => failure,
        };
        assert!(failure.parts.database.is_some());
        assert!(failure.parts.plan.is_none());
        assert_eq!(pool.reserved_bytes(), paid);
        outsider_refused(file.path());
        drop(failure);
        assert_eq!(pool.reserved_bytes(), 0);
        drop(closed);
        drop(ReadOnlyReopenFileCustody::acquire(File::open(file.path()).unwrap()).unwrap());
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }
}
