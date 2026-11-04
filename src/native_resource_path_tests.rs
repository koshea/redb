//! Actual native read/epoch-bound resource paths, never a complete write quote.
// req: STO-020, PRJ-004, REC-001
use super::*;
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::transaction_tracker::NativeResourcePlanningRefusal;
use crate::tree_store::{CompiledPathVisits, NativeKeyPathError, NativePreparedNormalKey};
use crate::{ReadableDatabase, TableDefinition};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("resource_path_rows");
fn database() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(ROWS).unwrap();
        for index in 0_u64..2048 {
            table
                .insert(index.to_be_bytes().as_slice(), [7_u8; 128].as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
    (file, db)
}

#[test]
fn native_resource_paths_read_actual_keys_and_account_observed_page_orders() {
    let (_file, mut db) = database();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let keys = [
        0_u64.to_be_bytes(),
        1024_u64.to_be_bytes(),
        2047_u64.to_be_bytes(),
        4096_u64.to_be_bytes(),
    ];
    let prepared = keys
        .each_ref()
        .map(|key| NativePreparedNormalKey::Bytes(key));
    let mut visits = CompiledPathVisits::for_prepared_keys(&prepared).unwrap();
    for key in keys {
        let path = table
            .inspect_prepared_resource_key(
                &NativePreparedNormalKey::Bytes(&key),
                &epoch,
                &mut visits,
            )
            .unwrap();
        let cost = path.cost();
        assert!((1..=64).contains(&cost.depth()));
        assert!(cost.observed_base_pages() >= u64::try_from(cost.depth()).unwrap());
        assert_eq!(
            cost.observed_page_bytes(),
            cost.observed_base_pages() * u64::try_from(db.mem.get_page_size()).unwrap()
        );
        path.require_current().unwrap();
    }
    assert!((4..=4 * 64).contains(&visits.visited()));
    assert_eq!(
        table
            .get(0_u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [7_u8; 128]
    );
    assert!(
        table
            .get(4096_u64.to_be_bytes().as_slice())
            .unwrap()
            .is_none()
    );
    drop(table);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_paths_refuse_foreign_epoch_before_reading_any_path() {
    let (_one, mut one) = database();
    let (_two, mut two) = database();
    let wrong = two.transaction_tracker.capture_resource_epoch().unwrap();
    let read = one.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let mut visits =
        CompiledPathVisits::for_prepared_keys(&[NativePreparedNormalKey::Bytes(&[])]).unwrap();
    assert!(matches!(
        table.inspect_prepared_resource_key(
            &NativePreparedNormalKey::Bytes(&0_u64.to_be_bytes()),
            &wrong,
            &mut visits
        ),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    assert_eq!(visits.visited(), 0);
    drop(table);
    drop(read);
    drop(wrong);
    assert!(one.check_integrity().unwrap());
    assert!(two.check_integrity().unwrap());
}

#[test]
fn native_resource_paths_refuse_stale_epoch_while_old_read_remains_valid() {
    let (_file, mut db) = database();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS)
        .unwrap()
        .insert(0_u64.to_be_bytes().as_slice(), [9_u8; 128].as_slice())
        .unwrap();
    tx.commit().unwrap();
    let mut visits =
        CompiledPathVisits::for_prepared_keys(&[NativePreparedNormalKey::Bytes(&[])]).unwrap();
    assert!(matches!(
        table.inspect_prepared_resource_key(
            &NativePreparedNormalKey::Bytes(&0_u64.to_be_bytes()),
            &epoch,
            &mut visits
        ),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    assert_eq!(visits.visited(), 0);
    assert_eq!(
        table
            .get(0_u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [7_u8; 128]
    );
    drop(table);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_paths_exhausted_visit_budget_refuses_before_page_fetch() {
    let (_file, mut db) = database();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let mut visits = CompiledPathVisits::for_prepared_keys(&[]).unwrap();
    assert!(matches!(
        table.inspect_prepared_resource_key(
            &NativePreparedNormalKey::Bytes(&0_u64.to_be_bytes()),
            &epoch,
            &mut visits
        ),
        Err(NativeKeyPathError::VisitsExceeded)
    ));
    assert_eq!(visits.visited(), 0);
    assert!(matches!(
        table.inspect_prepared_resource_key(
            &NativePreparedNormalKey::Str("wrong-key-kind"),
            &epoch,
            &mut visits
        ),
        Err(NativeKeyPathError::UnsupportedKey)
    ));
    assert_eq!(visits.visited(), 0);
    drop(table);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_paths_batch_borrows_exact_prepared_keys_and_remains_epoch_bound() {
    let (_file, mut db) = database();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let first = 0_u64.to_be_bytes();
    let absent = 4096_u64.to_be_bytes();
    let keys = [
        NativePreparedNormalKey::Bytes(&first),
        NativePreparedNormalKey::Bytes(&absent),
    ];
    let batch = table.inspect_prepared_resource_keys(&keys, &epoch).unwrap();
    assert_eq!(batch.prepared_keys().len(), 2);
    assert!((1..=64).contains(&batch.maximum_depth()));
    assert!((2..=128).contains(&batch.visited()));
    assert_eq!(
        batch.observed_page_bytes(),
        batch.observed_base_pages() * u64::try_from(db.mem.get_page_size()).unwrap()
    );
    batch.require_current().unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS)
        .unwrap()
        .insert(first.as_slice(), [9_u8; 128].as_slice())
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        batch.require_current(),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    assert_eq!(
        table.get(first.as_slice()).unwrap().unwrap().value(),
        [7_u8; 128]
    );
    drop(batch);
    drop(table);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_paths_old_read_cannot_borrow_a_new_current_epoch() {
    let (_file, mut db) = database();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let tx = db.begin_write().unwrap();
    tx.open_table(ROWS)
        .unwrap()
        .insert(0_u64.to_be_bytes().as_slice(), [9_u8; 128].as_slice())
        .unwrap();
    tx.commit().unwrap();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let keys = [NativePreparedNormalKey::Bytes(&[])];
    let mut visits = CompiledPathVisits::for_prepared_keys(&keys).unwrap();
    assert!(matches!(
        table.inspect_prepared_resource_key(
            &NativePreparedNormalKey::Bytes(&0_u64.to_be_bytes()),
            &epoch,
            &mut visits
        ),
        Err(NativeKeyPathError::ForeignOwner)
    ));
    assert_eq!(visits.visited(), 0);
    assert!(matches!(
        table.inspect_prepared_resource_keys(&keys, &epoch),
        Err(NativeKeyPathError::ForeignOwner)
    ));
    assert_eq!(
        table
            .get(0_u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [7_u8; 128]
    );
    drop(table);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_resource_paths_include_actual_namespace_definition_tree() {
    let (_file, mut db) = database();
    let epoch = db.transaction_tracker.capture_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let names = [
        NativePreparedNormalKey::Str("resource_path_rows"),
        NativePreparedNormalKey::Str("absent_resource_table"),
    ];
    let namespace = read
        .inspect_prepared_resource_table_definitions(&names, &epoch)
        .unwrap();
    assert_eq!(namespace.prepared_keys().len(), 2);
    assert!((1..=64).contains(&namespace.maximum_depth()));
    assert!((2..=128).contains(&namespace.visited()));
    assert_eq!(
        namespace.observed_page_bytes(),
        namespace.observed_base_pages() * u64::try_from(db.mem.get_page_size()).unwrap()
    );
    namespace.require_current().unwrap();
    let wrong = [NativePreparedNormalKey::Bytes(b"resource_path_rows")];
    assert!(matches!(
        read.inspect_prepared_resource_table_definitions(&wrong, &epoch),
        Err(NativeKeyPathError::UnsupportedKey)
    ));
    assert!(read.open_table(ROWS).is_ok());
    drop(namespace);
    drop(read);
    drop(epoch);
    assert!(db.check_integrity().unwrap());
}
