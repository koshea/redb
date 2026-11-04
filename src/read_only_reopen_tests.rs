// req: STO-020, REC-001, REC-003, REC-005, PRJ-004
use super::*;
use crate::Database;
use std::fs::OpenOptions;
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
fn descriptor(path: &std::path::Path) -> File {
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
    {
        let mut rows = write.open_table(ROWS).unwrap();
        for i in 0..300u32 {
            rows.insert(i.to_be_bytes().as_slice(), vec![7u8; 300].as_slice())
                .unwrap();
        }
    }
    write
        .open_table(META)
        .unwrap()
        .insert("identity", b"native".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    file
}
fn maintenance(path: &std::path::Path) -> ReadOnlyDatabase {
    let database = Builder::new()
        .open_read_only_maintenance_file(descriptor(path))
        .unwrap();
    // Actual normal maintenance traversal, no synthesized page-size facts.
    {
        let read = database.begin_read().unwrap();
        let rows = read.open_table(ROWS).unwrap();
        #[cfg(not(feature = "experimental-api-5"))]
        let range = rows.range_owned::<&[u8]>(..).unwrap();
        #[cfg(feature = "experimental-api-5")]
        let range = rows.range_owned(..).unwrap();
        for row in range {
            row.unwrap();
        }
        read.open_table(META)
            .unwrap()
            .get_owned("identity")
            .unwrap()
            .unwrap();
    }
    database
}
#[test]
fn native_readonly_plan_reopens_same_artifact_and_keeps_reads_exclusive_and_exact() {
    let file = fixture();
    let before = std::fs::read(file.path()).unwrap();
    let database = maintenance(file.path());
    let plan = database.plan_read_only_reopen().unwrap();
    assert!(plan.peak_bytes() >= plan.retained_bytes());
    assert!(plan.peak_bytes() > plan.profile.page_bytes() as u64);
    let handoff = database.into_read_only_reopen_handoff(plan).unwrap();
    assert!(matches!(
        File::open(file.path()).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    let mut database = Builder::new()
        .open_read_only_planned_handoff(handoff)
        .unwrap();
    database.inner.mem.readonly_policy_is_empty().unwrap();
    {
        let mut read = database.begin_read().unwrap();
        {
            let mut meta = read.open_str_table("meta").unwrap();
            let value = meta.get("identity").unwrap().unwrap();
            assert_eq!(value.value(), b"native");
        }
        let mut rows = read.open_bytes_table("rows").unwrap();
        let mut range = rows
            .range_from(Some(100u32.to_be_bytes().as_slice()))
            .unwrap();
        let mut count = 0;
        while let Some(row) = range.next().unwrap() {
            assert_eq!(row.key(), (100u32 + count).to_be_bytes());
            assert_eq!(row.value(), [7; 300]);
            count += 1u32;
        }
        assert_eq!(count, 200);
    }
    database.inner.mem.readonly_policy_is_empty().unwrap();
    assert_eq!(std::fs::read(file.path()).unwrap(), before);
    drop(database);
    let independently_opened = crate::ReadOnlyDatabase::open(file.path()).unwrap();
    drop(independently_opened);
    assert_eq!(std::fs::read(file.path()).unwrap(), before);
}
#[test]
fn native_readonly_handoff_refuses_bare_page_alias_after_transaction_teardown() {
    let file = fixture();
    let database = maintenance(file.path());
    let guard = {
        let read = database.begin_read().unwrap();
        let rows = read.open_table(ROWS).unwrap();
        #[cfg(not(feature = "experimental-api-5"))]
        {
            rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap()
        }
        #[cfg(feature = "experimental-api-5")]
        {
            rows.get_owned(0u32.to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
        }
    };
    let plan = database.plan_read_only_reopen().unwrap();
    assert!(matches!(
        database.into_read_only_reopen_handoff(plan),
        Err(ReadOnlyReopenError::Unsupported)
    ));
    assert_eq!(guard.value(), [7; 300]);
    drop(guard);
}
#[test]
fn native_readonly_plan_refuses_equal_bytes_from_foreign_actual_owner() {
    let first = fixture();
    let second = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(first.path(), second.path()).unwrap();
    let left = maintenance(first.path());
    let right = maintenance(second.path());
    let foreign = left.plan_read_only_reopen().unwrap();
    assert!(matches!(
        right.into_read_only_reopen_handoff(foreign),
        Err(ReadOnlyReopenError::Stale)
    ));
    drop(left);
}
#[test]
fn native_readonly_plan_refuses_unobserved_large_page_before_payload_allocation() {
    let file = fixture();
    {
        let database = Database::open(file.path()).unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(ROWS)
            .unwrap()
            .insert(b"large".as_slice(), vec![9u8; 128 * 1024].as_slice())
            .unwrap();
        write.commit().unwrap();
        drop(database);
    }
    // Normal opening observes only allocator/system paths. No read of the
    // large application leaf is smuggled into the maintenance observations.
    let database = Builder::new()
        .open_read_only_maintenance_file(descriptor(file.path()))
        .unwrap();
    let plan = database.plan_read_only_reopen().unwrap();
    assert!(plan.profile.maximum_observed_leaf_bytes() < 128 * 1024);
    let handoff = database.into_read_only_reopen_handoff(plan).unwrap();
    let mut database = Builder::new()
        .open_read_only_planned_handoff(handoff)
        .unwrap();
    let resources = database.inner.page_resources.as_ref().unwrap().clone();
    let before = resources.current();
    {
        let mut read = database.begin_read().unwrap();
        let mut rows = read.open_bytes_table("rows").unwrap();
        assert!(
            matches!(rows.get(b"large"), Err(ReadOnlyReopenError::Storage(StorageError::Io(ref e))) if e.kind()==std::io::ErrorKind::OutOfMemory)
        );
    }
    assert_eq!(resources.current(), before);
}
#[test]
fn native_readonly_plan_rejects_key_capacity_before_native_range_allocation() {
    let file = fixture();
    let database = maintenance(file.path());
    let plan = database.plan_read_only_reopen().unwrap();
    let handoff = database.into_read_only_reopen_handoff(plan).unwrap();
    let mut database = Builder::new()
        .open_read_only_planned_handoff(handoff)
        .unwrap();
    let mut read = database.begin_read().unwrap();
    let mut rows = read.open_bytes_table("rows").unwrap();
    assert!(matches!(
        rows.range_from(Some(&[0; KEY_BYTES + 1])),
        Err(ReadOnlyReopenError::Capacity)
    ));
    assert!(rows.get(&0u32.to_be_bytes()).unwrap().is_some());
}

#[cfg(not(feature = "experimental-api-5"))]
#[test]
fn native_readonly_handoff_refuses_zero_bytes_before_final_page_control_retirement() {
    use std::sync::Barrier;
    let file = fixture();
    let database = Builder::new()
        .set_cache_size(0)
        .open_read_only_maintenance_file(descriptor(file.path()))
        .unwrap();
    let guard = {
        let read = database.begin_read().unwrap();
        let rows = read.open_table(ROWS).unwrap();
        rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap()
    };
    let plan = database.plan_read_only_reopen().unwrap();
    let arrived = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    database
        .page_resources
        .as_ref()
        .unwrap()
        .pause_final_release_for_test(arrived.clone(), resume.clone());
    let release = std::thread::spawn(move || drop(guard));
    arrived.wait();
    // The real final page destructor has refunded its numeric payload count,
    // but is paused before releasing its Counter and debug read-map controls.
    assert_eq!(database.page_resources.as_ref().unwrap().current(), 0);
    let result = database.into_read_only_reopen_handoff(plan);
    let refused = matches!(&result, Err(ReadOnlyReopenError::Unsupported));
    drop(result);
    // Release and join even in the old-owner RED, before asserting its result.
    resume.wait();
    release.join().unwrap();
    assert!(
        refused,
        "zero bytes must not replace physical final-owner retirement"
    );
    drop(crate::ReadOnlyDatabase::open(file.path()).unwrap());
}
#[test]
fn native_readonly_handoff_refuses_second_public_plan_retaining_file_control() {
    let file = fixture();
    let database = maintenance(file.path());
    let first = database.plan_read_only_reopen().unwrap();
    let second = database.plan_read_only_reopen().unwrap();
    let result = database.into_read_only_reopen_handoff(first);
    let refused = matches!(&result, Err(ReadOnlyReopenError::Unsupported));
    drop(result);
    assert!(matches!(
        File::open(file.path()).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(second);
    drop(crate::ReadOnlyDatabase::open(file.path()).unwrap());
    assert!(
        refused,
        "a separately retained native core cannot cross the Source refund boundary"
    );
}

// req: STO-020, REC-001, REC-003, REC-005, PRJ-004
#[test]
fn native_readonly_original_backup_readonly_descriptor_supports_same_mode_handoff() {
    use crate::{ReadableTable, TableHandle};
    let path = std::env::var_os("RIFFDB_PRIVATE_ORIGINAL_BACKUP_READONLY_PATH")
        .map(std::path::PathBuf::from)
        .expect("root-owned genuine immutable backup descriptor fixture");
    let before = std::fs::read(&path).unwrap();
    // File::open is the same read-only access mode used by the original
    // PinnedDirectory::open_file(read=true) backup producer. No chmod, proc-FD
    // path, read/write reopening or alternate backend is allowed here.
    let original_readonly = File::open(&path).unwrap();
    assert_eq!(
        original_readonly.metadata().unwrap().len(),
        before.len() as u64
    );
    eprintln!(
        "actual_original_backup_readonly_descriptor_opened access=File::open bytes={}",
        before.len()
    );
    let opened = Builder::new().open_read_only_maintenance_file(original_readonly);
    let database = match opened {
        Ok(database) => database,
        Err(error) => {
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before,
                "original backup bytes unchanged on native open refusal"
            );
            let (class, errno) = match &error {
                ReadOnlyReopenError::Native(DatabaseError::Storage(StorageError::Io(io))) => {
                    ("native_io", io.raw_os_error())
                }
                ReadOnlyReopenError::Storage(StorageError::Io(io)) => {
                    ("storage_io", io.raw_os_error())
                }
                _ => ("other_typed_error", None),
            };
            eprintln!(
                "actual_original_backup_readonly_handoff_failure stage=maintenance_open class={class} errno={errno:?} bytes_unchanged=true"
            );
            panic!(
                "original read-only backup descriptor must support the same-mode closed maintenance handoff"
            );
        }
    };
    eprintln!("actual_original_backup_readonly_maintenance_open_succeeded");
    let expected_identity;
    {
        let read = database.begin_read().unwrap();
        // Genuine prephase maintenance reads every actual table/page, like the
        // original complete readonly validator. This does not mint Source proof.
        for handle in read.list_tables().unwrap() {
            let name = handle.name();
            if name == "meta" {
                let table = read
                    .open_table(TableDefinition::<&str, &[u8]>::new(name))
                    .unwrap();
                for row in table.iter().unwrap() {
                    row.unwrap();
                }
            } else {
                let table = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                    .unwrap();
                for row in table.iter().unwrap() {
                    row.unwrap();
                }
            }
        }
        expected_identity = read
            .open_table(META)
            .unwrap()
            .get_owned("database_id")
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
    }
    let plan = database.plan_read_only_reopen().unwrap();
    let peak = plan.peak_bytes();
    assert!(peak > 0);
    let handoff = database.into_read_only_reopen_handoff(plan).unwrap();
    eprintln!("actual_original_backup_readonly_maintenance_engine_joined peak_bytes={peak}");
    let mut planned = Builder::new()
        .open_read_only_planned_handoff(handoff)
        .unwrap();
    {
        let mut read = planned.begin_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        let identity = meta.get("database_id").unwrap().unwrap();
        assert_eq!(identity.value(), expected_identity.as_slice());
    }
    drop(planned);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    eprintln!(
        "actual_original_backup_readonly_same_mode_handoff_complete bytes_unchanged=true SourceAuthority=false"
    );
}

// req: STO-020, REC-001, REC-003, REC-005, PRJ-004
#[cfg(target_os = "linux")]
#[test]
fn native_readonly_original_descriptor_excludes_native_and_legacy_writers_through_handoff() {
    use crate::tree_store::file_backend::FileBackend;
    use crate::{ReadableTable, StorageBackend, TableHandle};
    use std::ops::Bound;
    fn excluded(path: &std::path::Path) {
        // Only an independent writer probe opens read/write. The actual
        // maintenance and planned readers use the original File::open below.
        let native_writer = FileBackend::new(descriptor(path)).unwrap();
        assert!(
            !native_writer
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
        );
        native_writer.close().unwrap();
        let legacy_writer = descriptor(path);
        assert!(matches!(
            legacy_writer.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        assert!(matches!(
            legacy_writer.try_lock_shared(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
    }
    let path = std::env::var_os("RIFFDB_PRIVATE_ORIGINAL_BACKUP_READONLY_PATH")
        .map(std::path::PathBuf::from)
        .expect("root-owned genuine immutable backup descriptor fixture");
    let before = std::fs::read(&path).unwrap();
    let database = Builder::new()
        .open_read_only_maintenance_file(File::open(&path).unwrap())
        .unwrap();
    excluded(&path);
    let expected_identity;
    {
        let read = database.begin_read().unwrap();
        for handle in read.list_tables().unwrap() {
            let name = handle.name();
            if name == "meta" {
                let table = read
                    .open_table(TableDefinition::<&str, &[u8]>::new(name))
                    .unwrap();
                for row in table.iter().unwrap() {
                    row.unwrap();
                }
            } else {
                let table = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                    .unwrap();
                for row in table.iter().unwrap() {
                    row.unwrap();
                }
            }
        }
        expected_identity = read
            .open_table(META)
            .unwrap()
            .get_owned("database_id")
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
    }
    let plan = database.plan_read_only_reopen().unwrap();
    excluded(&path);
    let handoff = database.into_read_only_reopen_handoff(plan).unwrap();
    // The old engine and its cache have been synchronously destroyed, and
    // the new engine has not been constructed. File custody still excludes both.
    excluded(&path);
    let mut planned = Builder::new()
        .open_read_only_planned_handoff(handoff)
        .unwrap();
    excluded(&path);
    {
        let mut read = planned.begin_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        assert_eq!(
            meta.get("database_id").unwrap().unwrap().value(),
            expected_identity.as_slice()
        );
        excluded(&path);
    }
    drop(planned);
    let native_writer = FileBackend::new(descriptor(&path)).unwrap();
    assert!(
        native_writer
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    native_writer.close().unwrap();
    let legacy_writer = descriptor(&path);
    legacy_writer.try_lock().unwrap();
    legacy_writer.unlock().unwrap();
    legacy_writer.try_lock_shared().unwrap();
    legacy_writer.unlock().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), before);
    eprintln!("actual_readonly_native_and_legacy_exclusion_complete bytes_unchanged=true");
}
