//! Fixed actual native prepared reclamation. No Source admission/factory claim.
// req: STO-020, PRJ-004, REC-001
use super::native_prepared_reclamation_test_support::*;
use super::*;
use crate::ReadableDatabase;
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::tree_store::NativeKeyPathError;

#[test]
fn native_prepared_reclamation_preserves_scope_when_old_reader_releases() {
    let (_file, mut db, old) = backlog_from_empty_reader();
    let read = db.begin_read().unwrap();
    let cut = read_cut(&read);
    let before = count_before(&db, cut);
    assert!(before >= 3);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    let prepared = system.prepare_reclamation(&mut quote).unwrap();
    assert_eq!(prepared.selected_records(), 0);
    drop(old); // Actual eligibility grows without a native write generation.
    prepared.require_before_write().unwrap();
    let mut tx = db.begin_write().unwrap();
    prepared.install_into(&mut tx).unwrap();
    tx.commit().unwrap();
    drop(epoch);
    let after = count_before(&db, cut);
    assert_eq!(
        after, before,
        "newly eligible work waits for a fresh admitted plan"
    );
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_prepared_reclamation_fresh_plan_progress_is_one_whole_entry_and_new_reader_survives() {
    let (_file, mut db, old) = backlog_from_empty_reader();
    drop(old);
    let read = db.begin_read().unwrap();
    let cut = read_cut(&read);
    let before = count_before(&db, cut);
    assert!(before >= 3);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    #[cfg(debug_assertions)]
    let observed_system_root = read.mem.get_system_root().unwrap().root;
    #[cfg(debug_assertions)]
    assert!(
        read.mem
            .native_page_has_read_alias_for_test(observed_system_root)
    );
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    let prepared = system.prepare_reclamation(&mut quote).unwrap();
    #[cfg(debug_assertions)]
    assert!(
        !read
            .mem
            .native_page_has_read_alias_for_test(observed_system_root),
        "consuming preparation destroys the actual SYSTEM cached root before the writer"
    );
    assert_eq!(prepared.selected_records(), 1);
    let newcomer = db.begin_read().unwrap();
    prepared.require_before_write().unwrap();
    let mut tx = db.begin_write().unwrap();
    prepared.install_into(&mut tx).unwrap();
    tx.open_table(ROWS)
        .unwrap()
        .insert(0, [9_u8; 8192].as_slice())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        newcomer
            .open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        6
    );
    drop(epoch);
    drop(newcomer);
    assert_eq!(
        count_before(&db, cut),
        before - 1,
        "one budget spans precommit and epilogue"
    );
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_prepared_reclamation_prefix_keeps_unselected_same_id_pagination() {
    let (_file, mut db) = seeded(false);
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    #[cfg(debug_assertions)]
    let observed_system_root = read.mem.get_system_root().unwrap().root;
    #[cfg(debug_assertions)]
    assert!(
        read.mem
            .native_page_has_read_alias_for_test(observed_system_root)
    );
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(2);
    let prepared = system.prepare_reclamation(&mut quote).unwrap();
    #[cfg(debug_assertions)]
    assert!(
        !read
            .mem
            .native_page_has_read_alias_for_test(observed_system_root),
        "consuming preparation destroys the actual SYSTEM cached root before the writer"
    );
    assert_eq!(prepared.selected_records(), 2);
    prepared.require_before_write().unwrap();
    let mut tx = db.begin_write().unwrap();
    prepared.install_into(&mut tx).unwrap();
    tx.commit().unwrap();
    drop(epoch);
    drop(read);
    assert!(!has_key(&db, false, 0));
    assert!(!has_key(&db, true, 0));
    assert!(has_key(&db, false, 1));
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_prepared_reclamation_stale_turn_refuses_before_fence_installation() {
    let (_file, mut db) = seeded(false);
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    let prepared = system.prepare_reclamation(&mut quote).unwrap();
    assert_eq!(prepared.selected_records(), 1);
    db.begin_write().unwrap().abort().unwrap();
    assert!(prepared.require_before_write().is_err());
    let mut tx = db.begin_write().unwrap();
    assert!(prepared.install_into(&mut tx).is_err());
    assert!(tx.reclamation.lock().unwrap().is_none());
    tx.abort().unwrap();
    drop(epoch);
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_prepared_reclamation_foreign_and_dirty_actual_writers_refuse() {
    let (_file, mut db) = seeded(false);
    let (_otherfile, mut other) = seeded(false);
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    let prepared = system.prepare_reclamation(&mut quote).unwrap();
    assert_eq!(prepared.selected_records(), 1);
    let mut tx = other.begin_write().unwrap();
    assert!(matches!(
        prepared.install_into(&mut tx),
        Err(NativeKeyPathError::ForeignOwner)
    ));
    assert!(tx.reclamation.lock().unwrap().is_none());
    tx.abort().unwrap();
    drop(epoch);
    drop(read);
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    let prepared = system.prepare_reclamation(&mut quote).unwrap();
    assert_eq!(prepared.selected_records(), 1);
    let mut tx = db.begin_write().unwrap();
    tx.open_table(ROWS)
        .unwrap()
        .insert(0, [7_u8; 8].as_slice())
        .unwrap();
    assert!(matches!(
        prepared.install_into(&mut tx),
        Err(NativeKeyPathError::UnsupportedState)
    ));
    assert!(tx.reclamation.lock().unwrap().is_none());
    tx.abort().unwrap();
    drop(epoch);
    drop(read);
    assert!(db.check_integrity().unwrap());
    assert!(other.check_integrity().unwrap());
}

#[test]
fn native_prepared_reclamation_requires_immediate_no_quick_repair_profile() {
    let (_file, mut db) = seeded(false);
    for quick in [false, true] {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
        let prepared = system.prepare_reclamation(&mut quote).unwrap();
        assert_eq!(prepared.selected_records(), 1);
        let mut tx = db.begin_write().unwrap();
        if quick {
            tx.set_quick_repair(true);
        } else {
            tx.set_durability(Durability::None).unwrap();
        }
        assert!(matches!(
            prepared.install_into(&mut tx),
            Err(NativeKeyPathError::UnsupportedState)
        ));
        assert!(tx.reclamation.lock().unwrap().is_none());
        tx.abort().unwrap();
        drop(epoch);
        drop(read);
    }
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_prepared_reclamation_unsupported_tail_refuses_before_any_writer() {
    let (_file, mut db) = seeded(true);
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut quote = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    assert!(matches!(
        system.prepare_reclamation(&mut quote),
        Err(NativeKeyPathError::UnsupportedState)
    ));
    epoch.require_current().unwrap();
    drop(epoch);
    drop(read);
    assert!(has_key(&db, false, 0) && has_key(&db, false, 1));
    assert!(db.check_integrity().unwrap());
}
