//! Actual native SYSTEM inspection exclusion and failure/drop ownership.
// req: STO-020, PRJ-004, REC-001
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::db::TransactionGuard;
use crate::transaction_tracker::NativeResourcePlanningRefusal;
use crate::transactions::ReadTransaction;
use crate::tree_store::NativeKeyPathError;
use crate::{Database, ReadableDatabase, TableDefinition};
use std::sync::{Arc, Barrier, mpsc::sync_channel};
const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("system_inspection_race");
fn fixture() -> (tempfile::NamedTempFile, Database, ReadTransaction) {
    let file = crate::create_tempfile();
    let database = Database::create(file.path()).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0, [1u8; 8192].as_slice())
        .unwrap();
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    (file, database, read)
}
fn actual_tracker(read: &ReadTransaction) -> Arc<crate::transaction_tracker::TransactionTracker> {
    let TransactionGuard::Read { tracker, .. } = &**read.tree.transaction_guard() else {
        panic!("genuine native read required")
    };
    tracker.clone()
}
#[test]
fn native_system_inspection_blocks_actual_writer_until_cached_roots_drop() {
    let (_file, database, read) = fixture();
    let database = Arc::new(database);
    let tracker = actual_tracker(&read);
    let epoch = database.capture_native_resource_epoch().unwrap();
    let before = tracker.resource_scalars_for_test();
    let system = read.capture_native_system_read(&epoch).unwrap();
    assert_eq!(
        tracker.resource_scalars_for_test(),
        before,
        "inspection issues no native ID/generation"
    );
    assert!(matches!(
        epoch.require_current(),
        Err(NativeResourcePlanningRefusal::Busy)
    ));
    system.require_current().unwrap();
    let (waiting, arrived) = sync_channel(1);
    tracker.notify_inspection_wait_for_test(waiting);
    let start = Arc::new(Barrier::new(2));
    let worker_start = start.clone();
    let other = database.clone();
    let writer = std::thread::spawn(move || {
        worker_start.wait();
        let write = other.begin_write().unwrap();
        write
            .open_table(ROWS)
            .unwrap()
            .insert(0, [2u8; 8192].as_slice())
            .unwrap();
        write.commit().unwrap();
    });
    start.wait();
    // This notification is emitted at the ordinary writer's real Condvar
    // contention, not an artificial test pause or a timing inference.
    arrived.recv().unwrap();
    assert_eq!(tracker.resource_scalars_for_test(), before);
    #[cfg(debug_assertions)]
    assert!(
        read.mem
            .native_page_has_read_alias_for_test(read.mem.get_system_root().unwrap().root)
    );
    drop(system); // All SYSTEM pages die before native lease notification.
    writer.join().unwrap();
    assert!(matches!(
        epoch.require_current(),
        Err(NativeResourcePlanningRefusal::Stale)
    ));
    drop(epoch);
    drop(read);
    let current = database.begin_read().unwrap();
    assert_eq!(
        current
            .open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        2
    );
}
#[test]
fn native_system_inspection_drop_and_panic_preserve_native_ids_and_release_writer() {
    let (_file, database, read) = fixture();
    let tracker = actual_tracker(&read);
    let epoch = database.capture_native_resource_epoch().unwrap();
    let before = tracker.resource_scalars_for_test();
    {
        let system = read.capture_native_system_read(&epoch).unwrap();
        system.require_current().unwrap();
        drop(system);
    }
    epoch.require_current().unwrap();
    assert_eq!(tracker.resource_scalars_for_test(), before);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _system = read.capture_native_system_read(&epoch).unwrap();
        panic!("intentional SYSTEM inspection cancellation");
    }));
    assert!(caught.is_err());
    epoch.require_current().unwrap();
    assert_eq!(tracker.resource_scalars_for_test(), before);
    drop(epoch);
    database.begin_write().unwrap().abort().unwrap();
}
#[test]
fn native_system_inspection_serial_exhaustion_refuses_only_optional_inspection() {
    let (_file, database, read) = fixture();
    let tracker = actual_tracker(&read);
    tracker.exhaust_inspection_serial_for_test();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let before = tracker.resource_scalars_for_test();
    assert!(matches!(
        read.capture_native_system_read(&epoch),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Exhausted
        ))
    ));
    epoch.require_current().unwrap();
    assert_eq!(tracker.resource_scalars_for_test(), before);
    drop(epoch);
    database.begin_write().unwrap().abort().unwrap();
}
#[test]
fn native_system_inspection_epoch_cannot_be_adopted_as_normal_writer_claim() {
    let (_file, database, read) = fixture();
    let tracker = actual_tracker(&read);
    let epoch = database.capture_native_resource_epoch().unwrap();
    let before = tracker.resource_scalars_for_test();
    let (lease, private_epoch) = tracker.claim_system_inspection(&epoch).unwrap();
    private_epoch.require_current().unwrap();
    assert!(matches!(
        tracker.try_claim_resource_write_slot(private_epoch),
        Err(NativeResourcePlanningRefusal::Unsupported)
    ));
    assert!(matches!(
        epoch.require_current(),
        Err(NativeResourcePlanningRefusal::Busy)
    ));
    assert_eq!(tracker.resource_scalars_for_test(), before);
    drop(lease);
    epoch.require_current().unwrap();
    assert_eq!(tracker.resource_scalars_for_test(), before);
    drop(epoch);
    database.begin_write().unwrap().abort().unwrap();
}
#[test]
fn native_system_inspection_deferred_database_close_runs_after_system_release() {
    let (file, database, read) = fixture();
    // Derive the epoch from the SAME genuine read's actual tracker. This
    // allows the real Database owner to drop while the native read stays live.
    let tracker = actual_tracker(&read);
    let epoch = tracker.capture_resource_epoch().unwrap();
    let before = tracker.resource_scalars_for_test();
    let system = read.capture_native_system_read(&epoch).unwrap();
    drop(database); // Actual native DatabaseDrop installs its deferred close.
    assert_eq!(tracker.resource_scalars_for_test(), before);
    system.require_current().unwrap();
    drop(system); // Physical cached roots first, then the real close recipe.
    drop(epoch);
    drop(read);
    let reopened = Database::open(file.path()).unwrap();
    assert_eq!(
        reopened
            .begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        1
    );
    reopened.begin_write().unwrap().abort().unwrap();
}
