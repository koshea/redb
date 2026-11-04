//! Actual native writer lifecycle; no complete memory-plan/Source authority claim.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::transaction_tracker::NativeResourcePlanningRefusal;
use crate::{Durability, ReadableDatabase, TableDefinition};
use std::sync::mpsc;

const ROWS: TableDefinition<u64, u64> = TableDefinition::new("resource_generation_rows");

fn database() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS).unwrap().insert(0, 1).unwrap();
    tx.commit().unwrap();
    (file, db)
}

#[test]
fn native_resource_generation_rejects_aborted_writer_with_unchanged_roots() {
    let (_file, mut db) = database();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let roots = (db.mem.get_data_root(), db.mem.get_system_root());
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS).unwrap().insert(0, 2).unwrap();
    tx.abort().unwrap();
    assert_eq!(roots, (db.mem.get_data_root(), db.mem.get_system_root()));
    assert_eq!(
        epoch.require_current(),
        Err(NativeResourcePlanningRefusal::Stale)
    );
    assert!(matches!(
        db.transaction_tracker.try_claim_resource_write_slot(epoch),
        Err(NativeResourcePlanningRefusal::Stale)
    ));
    let fresh = db.transaction_tracker.capture_resource_epoch().unwrap();
    let slot = db
        .transaction_tracker
        .try_claim_resource_write_slot(fresh)
        .unwrap();
    assert!(db.transaction_tracker.write_transaction_live());
    drop(slot);
    assert!(!db.transaction_tracker.write_transaction_live());
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_generation_rejects_actual_inflight_writer_then_stale_epoch() {
    let (_file, db) = database();
    let db = Arc::new(db);
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        let tx = writer_db.begin_write().unwrap();
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        tx.abort().unwrap();
    });
    entered_rx.recv().unwrap();
    let busy = epoch.require_current();
    let capture_busy = matches!(
        db.transaction_tracker.capture_resource_epoch(),
        Err(NativeResourcePlanningRefusal::Busy)
    );
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(busy, Err(NativeResourcePlanningRefusal::Busy));
    assert!(capture_busy);
    assert_eq!(
        epoch.require_current(),
        Err(NativeResourcePlanningRefusal::Stale)
    );
    drop(epoch);
    let mut db = Arc::try_unwrap(db).ok().unwrap();
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_generation_claim_is_one_turn_and_drop_releases_normal_slot() {
    let (_file, mut db) = database();
    let first = db.transaction_tracker.capture_resource_epoch().unwrap();
    let second = db.transaction_tracker.capture_resource_epoch().unwrap();
    let slot = db
        .transaction_tracker
        .try_claim_resource_write_slot(first)
        .unwrap();
    assert_eq!(
        second.require_current(),
        Err(NativeResourcePlanningRefusal::Busy)
    );
    assert!(matches!(
        db.transaction_tracker.capture_resource_epoch(),
        Err(NativeResourcePlanningRefusal::Busy)
    ));
    drop(slot);
    assert_eq!(
        second.require_current(),
        Err(NativeResourcePlanningRefusal::Stale)
    );
    assert!(matches!(
        db.transaction_tracker.try_claim_resource_write_slot(second),
        Err(NativeResourcePlanningRefusal::Stale)
    ));
    let fresh = db.transaction_tracker.capture_resource_epoch().unwrap();
    drop(
        db.transaction_tracker
            .try_claim_resource_write_slot(fresh)
            .unwrap(),
    );
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS).unwrap().insert(0, 3).unwrap();
    tx.commit().unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        3
    );
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_generation_exhaustion_refuses_only_optional_planning() {
    let (_file, mut db) = database();
    db.transaction_tracker
        .exhaust_resource_generation_for_test();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    assert!(matches!(
        db.transaction_tracker.try_claim_resource_write_slot(epoch),
        Err(NativeResourcePlanningRefusal::Exhausted)
    ));
    assert!(!db.transaction_tracker.write_transaction_live());
    assert!(matches!(
        db.transaction_tracker.capture_resource_epoch(),
        Err(NativeResourcePlanningRefusal::Exhausted)
    ));
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS).unwrap().insert(0, 4).unwrap();
    tx.commit().unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        4
    );
    drop(read);
    assert!(matches!(
        db.transaction_tracker.capture_resource_epoch(),
        Err(NativeResourcePlanningRefusal::Exhausted)
    ));
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_generation_refuses_foreign_owner_without_taking_either_slot() {
    let (_one, mut first) = database();
    let (_two, mut second) = database();
    let epoch = first.transaction_tracker.capture_resource_epoch().unwrap();
    assert!(matches!(
        second
            .transaction_tracker
            .try_claim_resource_write_slot(epoch),
        Err(NativeResourcePlanningRefusal::Stale)
    ));
    assert!(!first.transaction_tracker.write_transaction_live());
    assert!(!second.transaction_tracker.write_transaction_live());
    first.begin_write().unwrap().abort().unwrap();
    second.begin_write().unwrap().abort().unwrap();
    assert!(first.check_integrity().unwrap());
    assert!(second.check_integrity().unwrap());
}

#[test]
fn native_resource_tracker_profile_refuses_savepoint_and_multiple_pending_turns() {
    let (_file, mut db) = database();
    let before = db.transaction_tracker.capture_resource_epoch().unwrap();
    let latest = db.mem.get_last_committed_transaction_id().unwrap();
    let durable = db.mem.get_last_durable_transaction_id().unwrap();
    if let Some((id, ancestor)) = before.single_pending() {
        assert_eq!((id, ancestor), (latest, durable));
        assert_eq!(ancestor.raw_id().checked_add(1), Some(id.raw_id()));
    } else {
        assert_eq!(latest, durable);
    }
    // These are actual fixed metadata observations, not a proof that native
    // pending pages are post-only or that their retained capacities are admitted.
    let _actual_pending_id_count = before.unprocessed_freed_ids();
    drop(before);
    let tx = db.begin_write().unwrap();
    let savepoint = tx.ephemeral_savepoint().unwrap();
    tx.abort().unwrap();
    assert!(matches!(
        db.transaction_tracker.capture_resource_epoch(),
        Err(NativeResourcePlanningRefusal::Unsupported)
    ));
    drop(savepoint);
    assert!(db.transaction_tracker.capture_resource_epoch().is_ok());
    // Genuine ordinary non-durable turns, rather than a structural fake profile.
    for value in [5_u64, 6] {
        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::None).unwrap();
        tx.open_table(ROWS).unwrap().insert(0, value).unwrap();
        tx.commit().unwrap();
    }
    assert!(matches!(
        db.transaction_tracker.capture_resource_epoch(),
        Err(NativeResourcePlanningRefusal::Unsupported)
    ));
    db.begin_write().unwrap().commit().unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        6
    );
    drop(read);
    assert!(db.transaction_tracker.capture_resource_epoch().is_ok());
    assert!(db.check_integrity().unwrap());
}
