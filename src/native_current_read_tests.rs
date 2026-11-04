// req: STO-020, REC-001, REC-003, REC-005, PRJ-004
use super::*;
use crate::TableDefinition;
use crate::db::ReadableDatabase;
use std::sync::{Arc, Barrier};
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows");
fn fixture() -> (tempfile::NamedTempFile, Database) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let database = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = database.begin_write().unwrap();
    {
        let mut rows = write.open_table(ROWS).unwrap();
        for i in 0..256u32 {
            rows.insert(i.to_be_bytes().as_slice(), vec![7u8; 1536].as_slice())
                .unwrap();
        }
    }
    write.commit().unwrap();
    observe(&database);
    (file, database)
}
fn observe(database: &Database) {
    let read = database.begin_read().unwrap();
    let rows = read.open_table(ROWS).unwrap();
    for row in rows.range_owned::<&[u8]>(..).unwrap() {
        row.unwrap();
    }
}
fn write_value(database: &Database, bytes: &[u8]) {
    let write = database.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0u32.to_be_bytes().as_slice(), bytes)
        .unwrap();
    write.commit().unwrap();
}
fn assert_old(planned: &mut PlannedReadTransaction<'_>) {
    let mut rows = planned.open_bytes_table("rows").unwrap();
    let value = rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap();
    assert_eq!(value.value(), [7; 1536]);
}
#[test]
fn native_current_planned_read_pins_same_id_without_shared_registration_growth() {
    let (_file, database) = fixture();
    let old = database.begin_read().unwrap();
    let before = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    let plan = database.plan_current_read_context().unwrap();
    assert!(plan.peak_bytes() > Database::current_read_plan_capture_bytes().unwrap());
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test(),
        before
    );
    let mut planned = database.begin_planned_read_context(plan).unwrap();
    assert!(
        planned.context_current_for_test() > 0,
        "first namespace fetch must be admitted"
    );
    let after = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert_eq!(after.ordinary_entries, before.ordinary_entries);
    assert_eq!(
        after.ordinary_references, before.ordinary_references,
        "planned current read mutated the ordinary registration map"
    );
    assert!(after.planned_id.is_some());
    assert!(after.ordinary_at_slot >= 1);
    drop(old);
    let only = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert_eq!(only.planned_id, after.planned_id);
    assert_eq!(only.ordinary_at_slot + 1, after.ordinary_at_slot);
    assert!(
        database
            .transaction_tracker
            .any_user_read_reference_exists()
    );
    assert_old(&mut planned);
    drop(planned);
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
}
#[test]
fn native_current_planned_read_release_does_not_remove_ordinary_same_id_pin() {
    let (_file, database) = fixture();
    let old = database.begin_read().unwrap();
    let before = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    let planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    drop(planned);
    let after = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert_eq!(after, before);
    assert_eq!(
        old.open_table(ROWS)
            .unwrap()
            .get_owned(0u32.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [7; 1536]
    );
}
#[test]
fn native_current_planned_read_survives_real_writes_reclamation_and_nondurable_horizons() {
    let (_file, database) = fixture();
    let mut write = database.begin_write().unwrap();
    write.set_durability(crate::Durability::None).unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0u32.to_be_bytes().as_slice(), b"nondurable".as_slice())
        .unwrap();
    write.commit().unwrap();
    let mut planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let captured = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert!(
        captured.planned_matches_pending,
        "actual explicit nondurable snapshot must be pending"
    );
    assert_eq!(
        captured.ordinary_at_slot, 0,
        "only the planned slot may supply this pending read"
    );
    assert_eq!(
        database
            .transaction_tracker
            .oldest_live_read_nondurable_transaction(),
        captured.planned_id
    );
    for _ in 0..6 {
        write_value(&database, b"new");
    }
    let live = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert_eq!(live.planned_id, captured.planned_id);
    assert_eq!(live.oldest, live.planned_id);
    {
        let mut rows = planned.open_bytes_table("rows").unwrap();
        let value = rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap();
        assert_eq!(value.value(), b"nondurable");
    }
    drop(planned);
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
    for _ in 0..3 {
        write_value(&database, b"after");
    }
    observe(&database);
}
#[test]
fn native_current_planned_read_stale_foreign_and_aborted_write_proposals_refuse() {
    let (_file, database) = fixture();
    let (_other_file, other) = fixture();
    let plan = database.plan_current_read_context().unwrap();
    assert!(matches!(
        other.begin_planned_read_context(plan),
        Err(ReadOnlyReopenError::Stale)
    ));
    let before = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    let plan = database.plan_current_read_context().unwrap();
    database.begin_write().unwrap().abort().unwrap();
    assert!(matches!(
        database.begin_planned_read_context(plan),
        Err(ReadOnlyReopenError::Stale)
    ));
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test(),
        before
    );
    let plan = database.plan_current_read_context().unwrap();
    write_value(&database, b"new");
    assert!(matches!(
        database.begin_planned_read_context(plan),
        Err(ReadOnlyReopenError::Stale)
    ));
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
    let mut fresh = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let mut rows = fresh.open_bytes_table("rows").unwrap();
    assert_eq!(
        rows.get(0u32.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"new"
    );
}
#[test]
fn native_current_planned_read_two_claim_schedule_has_one_inline_owner() {
    let (_file, database) = fixture();
    let barrier = Arc::new(Barrier::new(2));
    fn attempt(database: &Database, barrier: &Barrier) -> bool {
        let plan = database.plan_current_read_context().unwrap();
        barrier.wait();
        let result = database.begin_planned_read_context(plan);
        let won = result.is_ok();
        if !won {
            assert!(matches!(&result, Err(ReadOnlyReopenError::Capacity)));
        }
        barrier.wait(); // both actual attempts finish before either owner drops
        drop(result);
        won
    }
    let winners = std::thread::scope(|scope| {
        let a = scope.spawn(|| attempt(&database, &barrier));
        let b = scope.spawn(|| attempt(&database, &barrier));
        u8::from(a.join().unwrap()) + u8::from(b.join().unwrap())
    });
    assert_eq!(winners, 1);
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
}
#[test]
fn native_current_planned_read_savepoint_release_and_cancel_preserve_other_pins() {
    let (_file, database) = fixture();
    let write = database.begin_write().unwrap();
    let savepoint = write.ephemeral_savepoint().unwrap();
    write.abort().unwrap();
    let planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let captured = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert!(database.transaction_tracker.any_savepoint_exists());
    drop(savepoint);
    assert!(!database.transaction_tracker.any_savepoint_exists());
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id,
        captured.planned_id
    );
    drop(planned); // genuine cancellation/destruction, no raw clear token
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
}
#[test]
fn native_current_planned_read_forgotten_child_keeps_slot_and_bounds_new_capture() {
    let (_file, database) = fixture();
    let mut planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let mut rows = planned.open_bytes_table("rows").unwrap();
    let value = rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap();
    core::mem::forget(value);
    assert!(matches!(
        rows.get(1u32.to_be_bytes().as_slice()),
        Err(ReadOnlyReopenError::Capacity)
    ));
    drop(rows);
    drop(planned);
    let captured = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert!(captured.planned_id.is_some());
    for _ in 0..3 {
        assert!(matches!(
            database.plan_current_read_context(),
            Err(ReadOnlyReopenError::Capacity)
        ));
    }
    write_value(&database, b"ordinary");
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id,
        captured.planned_id
    );
}
#[test]
#[cfg(debug_assertions)]
fn native_current_planned_read_private_metadata_and_capacity_failure_preserve_actual_slot() {
    let (_file, database) = fixture();
    let old = database.begin_read().unwrap();
    let rows = old.open_table(ROWS).unwrap();
    write_value(&database, b"new");
    let mut retained = Vec::new();
    let mut full = false;
    for key in 0..256u32 {
        retained.push(
            rows.get_owned(key.to_be_bytes().as_slice())
                .unwrap()
                .unwrap(),
        );
        let (len, capacity) = database.mem.debug_read_capacity_for_test();
        if len == capacity {
            full = true;
            break;
        }
    }
    assert!(full, "genuine old pages must fill actual shared debug map");
    let before = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    // Fresh copied namespace pages have their own preadmitted metadata. A full
    // ordinary debug map must neither veto them nor be changed by their reads.
    let map_before = database.mem.debug_read_capacity_for_test();
    let mut planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let installed = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert!(installed.planned_id.is_some());
    assert_eq!(installed.ordinary_entries, before.ordinary_entries);
    assert_eq!(installed.ordinary_references, before.ordinary_references);
    assert_eq!(
        planned
            .open_bytes_table("rows")
            .unwrap()
            .get(0u32.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"new"
    );
    assert_eq!(database.mem.debug_read_capacity_for_test(), map_before);
    drop(planned);
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test(),
        before
    );
    // Retain the ORIGINAL constructor-failure release contract using genuine
    // copied-page admission. Only this owning-module fixture may shorten the
    // physical pool; no production limit or metadata allowance is increased.
    let mut short = database.plan_current_read_context().unwrap();
    short.limits.page_bytes = 0;
    let result = database.begin_planned_read_context(short);
    // Native TableTree construction runs AFTER actual guard/slot installation;
    // its page OOM is wrapped by new_with_read_context as Transaction::Storage.
    // A pre-registration planning refusal cannot satisfy this exact variant.
    assert!(matches!(&result,
        Err(ReadOnlyReopenError::Transaction(crate::TransactionError::Storage(
            crate::StorageError::Io(error))))
            if error.kind() == std::io::ErrorKind::OutOfMemory));
    drop(result);
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test(),
        before,
        "failed namespace page allocation leaked or removed another native pin"
    );
    assert_eq!(database.mem.debug_read_capacity_for_test(), map_before);
    drop(retained);
    drop(rows);
    drop(old);
    let planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    drop(planned);
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
}

#[test]
fn native_current_planned_read_persistent_savepoint_reopen_delete_keeps_slot() {
    let (file, database) = fixture();
    let write = database.begin_write().unwrap();
    let savepoint = write.persistent_savepoint().unwrap();
    write.commit().unwrap();
    drop(database);
    let database = crate::Builder::new()
        .set_cache_size(0)
        .open(file.path())
        .unwrap();
    observe(&database);
    assert!(
        database
            .transaction_tracker
            .any_persistent_savepoint_exists()
    );
    let mut planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let captured = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    let write = database.begin_write().unwrap();
    assert!(write.delete_persistent_savepoint(savepoint).unwrap());
    write.commit().unwrap();
    assert!(
        !database
            .transaction_tracker
            .any_persistent_savepoint_exists()
    );
    let after = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert_eq!(after.planned_id, captured.planned_id);
    assert_eq!(after.planned_serial, captured.planned_serial);
    assert_old(&mut planned);
    drop(planned);
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
}

fn observe_system(database: &Database) {
    let read = database.begin_read().unwrap();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = crate::transactions::empty_reclamation_budget_for_native_test();
    let observed = system.observe_freed_paths(&mut budget).unwrap();
    observed.require_current().unwrap();
}

#[test]
fn native_planned_read_checks_real_namespace_and_system_paths_under_separate_inspection() {
    let (_file, database) = fixture();
    observe_system(&database);
    let before = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    let planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let pinned = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    assert_eq!(pinned.ordinary_entries, before.ordinary_entries);
    assert_eq!(pinned.ordinary_references, before.ordinary_references);
    let actual = planned.actual_read_for_system_test();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let keys = [crate::tree_store::NativePreparedNormalKey::Str("rows")];
    let paths = actual
        .inspect_prepared_resource_table_definitions(&keys, &epoch)
        .expect("checked planned pin was refused by the namespace resource observer");
    assert!(paths.visited() > 0);
    drop(paths);
    let user_charge = planned.context_current_for_test();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    assert!(matches!(
        epoch.require_current(),
        Err(crate::transaction_tracker::NativeResourcePlanningRefusal::Busy)
    ));
    system.require_current().unwrap();
    assert!(planned.context_current_for_test() > user_charge);
    let mut budget = crate::transactions::empty_reclamation_budget_for_native_test();
    let prepared = system.prepare_reclamation(&mut budget).unwrap();
    prepared.require_before_write().unwrap();
    assert_eq!(planned.context_current_for_test(), user_charge);
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test(),
        pinned
    );
    drop(prepared);
    drop(epoch);
    drop(planned);
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test(),
        before
    );
}

#[test]
fn native_planned_read_system_inspection_blocks_writer_but_old_data_pin_remains_valid() {
    let (_file, database) = fixture();
    observe_system(&database);
    let mut planned = database
        .begin_planned_read_context(database.plan_current_read_context().unwrap())
        .unwrap();
    let captured = database
        .transaction_tracker
        .planned_read_snapshot_for_test();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let system = planned
        .actual_read_for_system_test()
        .capture_native_system_read(&epoch)
        .unwrap();
    let before = database.transaction_tracker.resource_scalars_for_test();
    let (waiting, arrived) = std::sync::mpsc::sync_channel(1);
    database
        .transaction_tracker
        .notify_inspection_wait_for_test(waiting);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| write_value(&database, b"after-system-inspection"));
        // Emitted at the actual native writer Condvar, not a delay or fake pause.
        arrived.recv().unwrap();
        assert_eq!(
            database.transaction_tracker.resource_scalars_for_test(),
            before
        );
        system.require_current().unwrap();
        drop(system);
        writer.join().unwrap();
    });
    assert_eq!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id,
        captured.planned_id
    );
    assert_old(&mut planned);
    drop(epoch);
    let current_epoch = database.capture_native_resource_epoch().unwrap();
    assert!(matches!(
        planned
            .actual_read_for_system_test()
            .capture_native_system_read(&current_epoch),
        Err(crate::tree_store::NativeKeyPathError::ForeignOwner)
    ));
    drop(current_epoch);
    drop(planned);
    assert!(
        database
            .transaction_tracker
            .planned_read_snapshot_for_test()
            .planned_id
            .is_none()
    );
    write_value(&database, b"ordinary-after-release");
}
