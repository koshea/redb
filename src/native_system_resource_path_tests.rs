//! Actual native current SYSTEM/root/whole-entry observation controls.
//! Seeded empty native PageLists are syntax controls, never Source authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::transaction_tracker::NativeResourcePlanningRefusal;
use crate::tree_store::NativeKeyPathError;
use crate::{Database, ReadableDatabase, TableDefinition};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("system_resource_rows");
fn seed_empty(tx: &WriteTransaction, system: bool, pagination: u64, capacity: usize) {
    let mut tables = tx.system_tables.lock().unwrap();
    let mut table = tables
        .open_system_table(if system {
            SYSTEM_FREED_TABLE
        } else {
            DATA_FREED_TABLE
        })
        .unwrap();
    let key = TransactionIdWithPagination {
        transaction_id: 0,
        pagination_id: pagination,
    };
    let mut value = table
        .insert_reserve(&key, PageList::required_bytes(capacity))
        .unwrap();
    value.as_mut().clear();
}
fn seeded(
    data_entries: u64,
    system_capacity: Option<usize>,
) -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let mut tx = db.begin_write().unwrap();
    tx.set_finite_reclamation_for_test(0);
    for index in 0..data_entries {
        seed_empty(&tx, false, index, 400);
    }
    if let Some(capacity) = system_capacity {
        seed_empty(&tx, true, 0, capacity);
    }
    tx.commit().unwrap();
    (file, db)
}
fn retained_reader_backlog() -> (tempfile::NamedTempFile, Database, ReadTransaction) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(ROWS).unwrap();
        for key in 0..64 {
            table.insert(key, [0_u8; 8192].as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let old = db.begin_read().unwrap();
    for version in 1..=6_u8 {
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(ROWS).unwrap();
            for key in 0..64 {
                table.insert(key, [version; 8192].as_slice()).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    (file, db, old)
}

#[test]
fn native_system_paths_share_one_budget_across_exact_data_and_system_entries() {
    let (_file, mut db) = seeded(1, Some(200));
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = bounded_reclamation::ReclamationBudget::for_emitted_entries(2);
    let before = budget.remaining_for_observation_test();
    let mut observed = system.observe_freed_paths(&mut budget).unwrap();
    assert!(observed.next().unwrap());
    assert_eq!(observed.last_data().unwrap().transaction_id, 0);
    assert!(observed.last_system().is_none());
    assert!(observed.next().unwrap());
    assert_eq!(observed.last_system().unwrap().transaction_id, 0);
    assert!(!observed.next().unwrap());
    assert_eq!(
        observed.boundary(),
        Some(NativeFreedBoundary::BudgetExhausted)
    );
    assert_eq!(observed.selected_records(), 2);
    assert_eq!(observed.selected_page_numbers(), 0);
    assert_eq!(observed.selected_weighted_pages(), 0);
    assert_eq!(
        observed.selected_encoded_bytes(),
        2 * 16 + PageList::required_bytes(400) + PageList::required_bytes(200)
    );
    assert!((1..=64).contains(&observed.maximum_depth()));
    assert!(observed.path_bytes() > 0 && observed.path_visits() > 0);
    observed.require_current().unwrap();
    drop(observed);
    let after = budget.remaining_for_observation_test();
    assert_eq!(after.0, 0); // One removal budget shared by both families.
    assert_eq!(before.1 - after.1, 6); // Two heads plus selected re-probe each.
    assert_eq!(before.2, after.2); // Empty lists have no decoded PageNumbers.
    assert!(
        before.3 - after.3
            >= 2 * (2 * 16 + PageList::required_bytes(400) + PageList::required_bytes(200))
    );
    drop(system);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_system_paths_stop_before_unsupported_whole_entry_without_mutation() {
    let (_file, mut db) = seeded(1, Some(401));
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = bounded_reclamation::ReclamationBudget::for_emitted_entries(2);
    let mut observed = system.observe_freed_paths(&mut budget).unwrap();
    assert!(!observed.next().unwrap());
    assert_eq!(
        observed.boundary(),
        Some(NativeFreedBoundary::UnsupportedWholeEntry)
    );
    assert_eq!(observed.boundary_records(), 1);
    assert_eq!(observed.selected_records(), 0);
    assert_eq!(observed.selected_page_numbers(), 0);
    assert!(observed.last_data().is_none() && observed.last_system().is_none());
    drop(observed);
    drop(system);
    drop(read);
    drop(epoch);
    let tx = db.begin_write().unwrap();
    {
        let mut tables = tx.system_tables.lock().unwrap();
        let table = tables.open_system_table(SYSTEM_FREED_TABLE).unwrap();
        let value = table
            .get(&TransactionIdWithPagination {
                transaction_id: 0,
                pagination_id: 0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(value.value().data.len(), PageList::required_bytes(401));
    }
    tx.abort().unwrap();
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_system_paths_successor_crosses_real_freed_leaf_boundaries() {
    let (_file, mut db) = seeded(80, None);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = bounded_reclamation::ReclamationBudget::for_emitted_entries(3);
    let mut observed = system.observe_freed_paths(&mut budget).unwrap();
    for index in 0..3 {
        assert!(observed.next().unwrap());
        let key = observed.last_data().unwrap();
        assert_eq!((key.transaction_id, key.pagination_id), (0, index));
    }
    assert!(!observed.next().unwrap());
    assert_eq!(
        observed.boundary(),
        Some(NativeFreedBoundary::BudgetExhausted)
    );
    assert_eq!(observed.selected_records(), 3);
    assert_eq!(
        observed.selected_encoded_bytes(),
        3 * (16 + PageList::required_bytes(400))
    );
    assert!(
        observed.maximum_depth() >= 2,
        "genuine branch/leaf successor required"
    );
    drop(observed);
    drop(system);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_system_paths_reader_release_stales_horizon_without_a_new_writer() {
    let (_file, mut db, old) = retained_reader_backlog();
    assert_eq!(
        old.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        0
    );
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let old_horizon = system.observed_horizon();
    system.require_current().unwrap();
    drop(old);
    assert!(matches!(
        epoch.require_current(),
        Err(NativeResourcePlanningRefusal::Busy)
    ));
    // The original ordinary epoch remains Free-only while inspection owns the
    // slot. Releasing a reader still changes no native write generation.
    assert!(matches!(
        system.require_current(),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    drop(system);
    epoch.require_current().unwrap(); // no write generation changed
    drop(read);
    drop(epoch);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    assert!(system.observed_horizon() > old_horizon);
    let mut budget = bounded_reclamation::ReclamationBudget::for_emitted_entries(1);
    let mut observed = system.observe_freed_paths(&mut budget).unwrap();
    assert!(observed.next().unwrap());
    assert_eq!(observed.selected_records(), 1);
    drop(observed);
    drop(system);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_system_paths_old_read_and_foreign_tracker_cannot_mint_current_system_owner() {
    let (_file, mut db) = seeded(1, None);
    let (_otherfile, mut other) = seeded(1, None);
    let wrong = other.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    assert!(matches!(
        read.capture_native_system_read(&wrong),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    drop(wrong);
    let tx = db.begin_write().unwrap();
    tx.abort().unwrap();
    // An abort changes the generation but not the committed read root. A
    // committed write is required to make the old actual read root stale.
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS)
        .unwrap()
        .insert(0, [9_u8; 8].as_slice())
        .unwrap();
    tx.commit().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    assert!(matches!(
        read.capture_native_system_read(&epoch),
        Err(NativeKeyPathError::ForeignOwner)
    ));
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
    assert!(other.check_integrity().unwrap());
}

#[test]
fn native_system_paths_next_native_id_includes_aborted_writer_advancement() {
    let (_file, mut db) = seeded(1, None);
    let tx = db.begin_write().unwrap();
    let aborted = tx.transaction_id;
    tx.abort().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let proposal = system.next_write_proposal();
    assert_eq!(proposal.raw_id(), aborted.raw_id() + 1);
    drop(system);
    drop(read);
    drop(epoch);
    let tx = db.begin_write().unwrap();
    assert_eq!(tx.transaction_id, proposal);
    tx.abort().unwrap();
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_system_paths_page_number_limit_defers_real_retired_pages_whole() {
    let (_file, mut db, old) = retained_reader_backlog();
    drop(old);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = bounded_reclamation::ReclamationBudget::for_emitted_entries(32);
    budget.limit_page_numbers_for_test(0);
    let mut observed = system.observe_freed_paths(&mut budget).unwrap();
    while observed.next().unwrap() {}
    assert_eq!(
        observed.boundary(),
        Some(NativeFreedBoundary::PageNumbersExhausted)
    );
    assert!(observed.boundary_records() >= 1);
    assert_eq!(observed.selected_page_numbers(), 0);
    assert_eq!(observed.selected_weighted_pages(), 0);
    observed.require_current().unwrap();
    drop(observed);
    drop(system);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}
