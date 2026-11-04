// req: STO-020, REC-001, REC-003, REC-005, PRJ-004
use super::*;
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::{Database, ReadOnlyDatabase, ReadableDatabase, TableDefinition};
use std::fs::OpenOptions;
use std::sync::mpsc;

const VALUES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("source_values");
fn open(path: &std::path::Path) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}
fn fixture() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let database = Database::create(file.path()).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(VALUES)
        .unwrap()
        .insert(b"key".as_slice(), b"original".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    file
}
fn reader(custody: &ReadOnlyReopenFileCustody) -> ReadOnlyDatabase {
    ReadOnlyDatabase::new(
        Box::new(custody.lease().unwrap()),
        4096,
        None,
        0,
        crate::db::ConcurrencyMode::ExclusiveWriter,
    )
    .unwrap()
}
fn outsider_refused(path: &std::path::Path) {
    let observer = FileBackend::new(open(path)).unwrap();
    assert!(
        !observer
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    observer.close().unwrap();
    assert!(matches!(
        open(path).try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
}

#[test]
fn native_readonly_custody_preserves_exclusion_through_actual_cache_close_and_reopen() {
    let file = fixture();
    let before = std::fs::read(file.path()).unwrap();
    let custody = ReadOnlyReopenFileCustody::acquire(open(file.path())).unwrap();
    let binding = custody.actual_file_binding().unwrap();
    for _ in 0..2 {
        let database = reader(&custody);
        let read = database.begin_read().unwrap();
        assert_eq!(
            read.open_table(VALUES)
                .unwrap()
                .get(b"key".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"original"
        );
        outsider_refused(file.path());
        drop(read);
        drop(database);
        assert!(custody.is_drained().unwrap());
        // Actual ordinary native memory/cache destruction has completed here.
        // The enclosing file-only owner continues to exclude every outsider.
        outsider_refused(file.path());
        assert_eq!(custody.actual_file_binding().unwrap(), binding);
    }
    assert_eq!(std::fs::read(file.path()).unwrap(), before);
    let inherited = custody.shared.file.0.try_clone().unwrap();
    drop(custody);
    let observer = FileBackend::new(open(file.path())).unwrap();
    assert!(
        observer
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    observer.close().unwrap();
    let legacy = open(file.path());
    legacy.try_lock().unwrap();
    legacy.unlock().unwrap();
    assert_eq!(inherited.metadata().unwrap().len(), before.len() as u64);
}

#[test]
fn native_readonly_custody_refuses_surviving_real_read_transaction() {
    let file = fixture();
    let custody = ReadOnlyReopenFileCustody::acquire(open(file.path())).unwrap();
    let database = reader(&custody);
    let read = database.begin_read().unwrap();
    drop(database);
    assert!(!custody.is_drained().unwrap());
    assert_eq!(
        custody.lease().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    outsider_refused(file.path());
    // This tests the actual native read owner. Bare page guards additionally
    // require the factory's final-page counter; this test does not claim that.
    assert_eq!(
        read.open_table(VALUES)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    drop(read);
    assert!(custody.is_drained().unwrap());
    drop(reader(&custody));
    outsider_refused(file.path());
}

#[test]
fn native_readonly_custody_fixed_subleases_reject_overflow_and_stale_close() {
    let file = fixture();
    let custody = ReadOnlyReopenFileCustody::acquire(open(file.path())).unwrap();
    let first = custody.lease().unwrap();
    for n in 0..ENGINE_RANGES as u64 {
        assert!(
            first
                .try_lock_shared_range(Bound::Included(n), Bound::Included(n))
                .unwrap()
        );
        assert!(
            first
                .try_lock_shared_range(Bound::Included(n), Bound::Included(n))
                .unwrap()
        );
    }
    let overflow = first
        .try_lock_shared_range(Bound::Included(100), Bound::Included(100))
        .unwrap_err();
    let overflow: io::Error = overflow.into();
    assert_eq!(overflow.kind(), io::ErrorKind::StorageFull);
    assert!(
        first
            .try_lock_shared_range(Bound::Excluded(u64::MAX), Bound::Unbounded)
            .is_err()
    );
    assert!(
        first
            .query_lock_range(Bound::Included(0), Bound::Included(0))
            .is_err()
    );
    assert!(
        !first
            .query_lock_range(Bound::Included(100), Bound::Unbounded)
            .unwrap()
    );
    first.close().unwrap();
    let second = custody.lease().unwrap();
    first.close().unwrap();
    assert!(first.len().is_err());
    assert!(second.len().is_ok());
    assert!(!custody.is_drained().unwrap());
    assert_eq!(
        second.write(0, b"invalid").unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        second.set_len(1).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        second.sync_data().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    drop(first);
    assert!(second.len().is_ok());
    outsider_refused(file.path());
    drop(second);
    assert!(custody.is_drained().unwrap());
}

#[test]
fn native_readonly_custody_close_joins_actual_synchronous_access_without_sleep() {
    let file = fixture();
    let custody = ReadOnlyReopenFileCustody::acquire(open(file.path())).unwrap();
    let first = custody.lease().unwrap();
    let access = first.access().unwrap();
    let concurrent = ReadOnlyReopenBackend {
        shared: first.shared.clone(),
        generation: first.generation,
    };
    let (attempt_tx, attempt_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let closer = std::thread::spawn(move || {
        attempt_tx.send(()).unwrap();
        concurrent.close().unwrap();
        done_tx.send(()).unwrap();
    });
    attempt_rx.recv().unwrap();
    assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    // The same actual mutex guard used across backend.read() excludes close.
    assert_eq!(access.active, Some(first.generation));
    drop(access);
    done_rx.recv().unwrap();
    closer.join().unwrap();
    assert!(first.read(0, &mut [0; 1]).is_err());
    assert!(custody.is_drained().unwrap());
    drop(custody.lease().unwrap());
    outsider_refused(file.path());
}

#[test]
fn native_readonly_custody_refusal_releases_own_legacy_lock_and_preserves_foreign_native_lock() {
    let file = fixture();
    let foreign = FileBackend::new(open(file.path())).unwrap();
    assert!(
        foreign
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    assert!(ReadOnlyReopenFileCustody::acquire(open(file.path())).is_err());
    let legacy = open(file.path());
    legacy.try_lock().unwrap();
    legacy.unlock().unwrap();
    let observer = FileBackend::new(open(file.path())).unwrap();
    assert!(
        !observer
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    foreign.close().unwrap();
    assert!(
        observer
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    observer.close().unwrap();
}

#[test]
fn native_readonly_custody_rechecks_actual_file_and_never_accepts_foreign_owner() {
    let first_file = fixture();
    let second_file = fixture();
    let first = ReadOnlyReopenFileCustody::acquire(open(first_file.path())).unwrap();
    let second = ReadOnlyReopenFileCustody::acquire(open(second_file.path())).unwrap();
    let alias = first.clone_file_custody();
    assert!(first.same_owner(&alias));
    assert!(!first.same_owner(&second));
    let layout = (
        std::mem::size_of::<ReadOnlyFileCore>() + 2 * std::mem::size_of::<usize>(),
        std::mem::align_of::<ReadOnlyFileCore>().max(std::mem::align_of::<usize>()),
    );
    assert_eq!(
        first.control_capacity_bytes().unwrap(),
        crate::tree_store::ReadOnlyResourceProfile::allocation(layout.0, layout.1).unwrap()
    );
    assert!(layout.0 >= std::mem::size_of::<ReadOnlyFileCore>());
    assert!(layout.1 >= std::mem::align_of::<ReadOnlyFileCore>());
    let mut header = [0; 64];
    first.read_header(&mut header).unwrap();
    assert!(header.iter().any(|byte| *byte != 0));
    assert!(first.read_header(&mut [0; 4097]).is_err());
    // Simulates a non-cooperating write; descriptor identity is still equal,
    // but the existing immutable continuity observation must refuse it.
    let old = first_file.as_file().metadata().unwrap().len();
    first_file.as_file().set_len(old + 4096).unwrap();
    assert!(first.actual_file_binding().is_err());
    assert!(alias.lease().is_err());
    assert!(second.actual_file_binding().is_ok());
}

// req: STO-020, REC-001, REC-003, REC-005, PRJ-004
#[cfg(target_os = "linux")]
#[test]
fn native_readonly_original_descriptor_final_real_read_alias_retains_both_exclusions() {
    let file = fixture();
    let before = std::fs::read(file.path()).unwrap();
    let custody = ReadOnlyReopenFileCustody::acquire(File::open(file.path()).unwrap()).unwrap();
    let database = reader(&custody);
    let read = database.begin_read().unwrap();
    assert_eq!(
        read.open_table(VALUES)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    drop(database);
    drop(custody);
    // This is an actual recoverable native read alias, not a synthetic numeric
    // token. Its memory/file owner must retain both exclusions after outer drop.
    outsider_refused(file.path());
    assert!(matches!(
        open(file.path()).try_lock_shared(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    assert!(matches!(
        Database::open(file.path()),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));
    assert_eq!(
        read.open_table(VALUES)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    drop(read);
    let native_writer = FileBackend::new(open(file.path())).unwrap();
    assert!(
        native_writer
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    native_writer.close().unwrap();
    let legacy_writer = open(file.path());
    legacy_writer.try_lock().unwrap();
    legacy_writer.unlock().unwrap();
    legacy_writer.try_lock_shared().unwrap();
    legacy_writer.unlock().unwrap();
    assert_eq!(std::fs::read(file.path()).unwrap(), before);
    eprintln!("actual_readonly_final_read_alias_exclusion_and_release_complete");
}

// Numerical constructor component only. Normal native commits produce this
// fixture; file opening, original pool and diagnostic controls are inputs.
#[cfg(target_os = "linux")]
mod prepaid_component {
    use super::*;
    use crate::tree_store::{NativeOwnedShared, NativePageResources, ReadOnlyResourceProfile};
    use crate::{ReadOnlyReopenError, StorageError};

    fn assert_capacity_refused(result: Result<ReadOnlyReopenFileCustody, ReadOnlyReopenError>) {
        assert!(matches!(result,
            Err(ReadOnlyReopenError::Native(DatabaseError::Storage(StorageError::Io(ref error))))
                if error.kind() == io::ErrorKind::OutOfMemory));
    }
    fn exclusions_released(path: &std::path::Path) {
        let observer = FileBackend::new(open(path)).unwrap();
        assert!(
            observer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        observer.close().unwrap();
        let legacy = open(path);
        legacy.try_lock().unwrap();
        legacy.unlock().unwrap();
    }

    // req: STO-020, REC-001, REC-003
    #[test]
    fn native_prepaid_readonly_custody_n_minus_one_refuses_before_backend_and_core_allocation() {
        let needed = ReadOnlyReopenFileCustody::prepaid_capacity_bytes().unwrap();
        let pool = NativePageResources::from_retained_pool_data(needed - 1, ()).unwrap();
        let epoch = pool.begin_managed_epoch(needed - 1).unwrap();
        let resources = epoch.resources();
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let original = File::open(file.path()).unwrap();
        let inherited = original.try_clone().unwrap();
        let trace = Arc::new(CustodyConstructionTrace::new(None));
        assert_capacity_refused(ReadOnlyReopenFileCustody::acquire_prepaid_observed(
            original,
            resources,
            Some(trace.clone()),
        ));
        assert_eq!(trace.stages.load(Ordering::Acquire), 0);
        assert_eq!(resources.current(), 0);
        assert_eq!(resources.admitted_high_water_for_test(), 0);
        exclusions_released(file.path());
        assert_eq!(inherited.metadata().unwrap().len(), before.len() as u64);
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }

    // req: STO-020, REC-001, REC-003, REC-005
    #[test]
    fn native_prepaid_readonly_custody_real_alias_and_sublease_keep_both_exclusions_until_final_drop()
     {
        let needed = ReadOnlyReopenFileCustody::prepaid_capacity_bytes().unwrap();
        let pool = NativePageResources::from_retained_pool_data(needed, ()).unwrap();
        let epoch = pool.begin_managed_epoch(needed).unwrap();
        let resources = epoch.resources();
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let original = File::open(file.path()).unwrap();
        let inherited = original.try_clone().unwrap();
        let trace = Arc::new(CustodyConstructionTrace::new(None));
        let custody = ReadOnlyReopenFileCustody::acquire_prepaid_observed(
            original,
            resources,
            Some(trace.clone()),
        )
        .unwrap();
        assert_eq!(resources.current(), needed);
        assert_eq!(resources.admitted_high_water_for_test(), needed);
        assert_eq!(trace.stages.load(Ordering::Acquire), 3);
        assert_eq!(
            custody.control_capacity_bytes().unwrap(),
            NativeOwnedShared::<ReadOnlyFileCore>::source_capacity_bytes().unwrap() as u64
        );
        let alias = custody.clone_file_custody();
        assert!(custody.same_owner(&alias));
        assert!(!custody.is_exclusive_handoff_owner().unwrap());
        let lease = custody.lease().unwrap();
        assert!(
            lease
                .try_lock_shared_range(Bound::Included(0), Bound::Included(0))
                .unwrap()
        );
        let mut header = [0_u8; 64];
        lease.read(0, &mut header).unwrap();
        assert_eq!(&header, &before[..64]);
        assert!(header.iter().any(|byte| *byte != 0));
        drop(custody);
        outsider_refused(file.path());
        drop(alias);
        assert_eq!(resources.current(), needed);
        outsider_refused(file.path());
        assert_eq!(lease.len().unwrap(), before.len() as u64);
        drop(lease);
        assert_eq!(trace.retired_at.load(Ordering::Acquire), needed);
        assert_eq!(resources.current(), 0);
        exclusions_released(file.path());
        assert_eq!(inherited.metadata().unwrap().len(), before.len() as u64);
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }

    // req: STO-020, REC-001, REC-003
    #[test]
    fn native_prepaid_readonly_custody_partial_native_tracker_retires_before_same_pool_refund() {
        let needed = ReadOnlyReopenFileCustody::prepaid_capacity_bytes().unwrap();
        let pool = NativePageResources::from_retained_pool_data(needed, ()).unwrap();
        let epoch = pool.begin_managed_epoch(needed).unwrap();
        let resources = epoch.resources();
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let original = File::open(file.path()).unwrap();
        let inherited = original.try_clone().unwrap();
        // Real FULL_RANGE insertion exists, but no core Arc was constructed.
        let trace = Arc::new(CustodyConstructionTrace::new(Some(2)));
        assert_capacity_refused(ReadOnlyReopenFileCustody::acquire_prepaid_observed(
            original,
            resources,
            Some(trace.clone()),
        ));
        assert_eq!(trace.stages.load(Ordering::Acquire), 2);
        assert_eq!(trace.retired_at.load(Ordering::Acquire), needed);
        assert_eq!(resources.admitted_high_water_for_test(), needed);
        assert_eq!(resources.current(), 0);
        exclusions_released(file.path());
        assert_eq!(inherited.metadata().unwrap().len(), before.len() as u64);
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }

    // req: STO-020, REC-001, REC-003
    #[test]
    fn native_prepaid_readonly_custody_foreign_exclusion_and_stale_serial_refuse_without_adoption()
    {
        let needed = ReadOnlyReopenFileCustody::prepaid_capacity_bytes().unwrap();
        let pool = NativePageResources::from_retained_pool_data(needed, ()).unwrap();
        let epoch = pool.begin_managed_epoch(needed).unwrap();
        let resources = epoch.resources().clone();
        let file = fixture();
        let before = std::fs::read(file.path()).unwrap();
        let foreign = FileBackend::new(open(file.path())).unwrap();
        assert!(
            foreign
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        assert!(matches!(
            ReadOnlyReopenFileCustody::acquire_prepaid(
                File::open(file.path()).unwrap(),
                &resources
            ),
            Err(ReadOnlyReopenError::Native(
                DatabaseError::DatabaseAlreadyOpen
            ))
        ));
        assert_eq!(resources.current(), 0);
        let legacy = open(file.path());
        legacy.try_lock().unwrap();
        legacy.unlock().unwrap();
        let observer = FileBackend::new(open(file.path())).unwrap();
        assert!(
            !observer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        foreign.close().unwrap();
        assert!(
            observer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        observer.close().unwrap();
        drop(epoch);
        let next_epoch = pool.begin_managed_epoch(needed).unwrap();
        let trace = Arc::new(CustodyConstructionTrace::new(None));
        assert_capacity_refused(ReadOnlyReopenFileCustody::acquire_prepaid_observed(
            File::open(file.path()).unwrap(),
            &resources,
            Some(trace.clone()),
        ));
        assert_eq!(trace.stages.load(Ordering::Acquire), 0);
        assert_eq!(resources.current(), 0);
        let current = ReadOnlyReopenFileCustody::acquire_prepaid(
            File::open(file.path()).unwrap(),
            next_epoch.resources(),
        )
        .unwrap();
        assert_eq!(resources.current(), needed);
        drop(current);
        assert_eq!(resources.current(), 0);
        exclusions_released(file.path());
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
        let ordinary =
            ReadOnlyReopenFileCustody::acquire(File::open(file.path()).unwrap()).unwrap();
        assert_eq!(
            ordinary.control_capacity_bytes().unwrap(),
            ReadOnlyResourceProfile::arc_allocation::<ReadOnlyFileCore>().unwrap()
        );
    }
}
