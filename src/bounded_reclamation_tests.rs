//! Actual legacy cleanup prerequisite: a released native reader exposes a
//! backlog independent of the next transaction's new logical mutation count.
use super::*;
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::{Database, ReadableDatabase, TableDefinition};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("bounded_gc_rows");

fn pending_before(db: &Database, before: TransactionId) -> usize {
    let tx = db.begin_write().unwrap();
    let count = {
        let mut tables = tx.system_tables.lock().unwrap();
        let mut count = 0;
        for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
            if tables.get_system_table_root(definition).unwrap().is_none() {
                continue;
            }
            let table = tables.open_system_table(definition).unwrap();
            let end = TransactionIdWithPagination {
                transaction_id: before.raw_id(),
                pagination_id: 0,
            };
            for entry in table.range(..end).unwrap() {
                entry.unwrap();
                count += 1;
            }
        }
        count
    };
    tx.abort().unwrap();
    count
}

fn released_reader_backlog() -> (tempfile::NamedTempFile, Database, TransactionId) {
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
    let reader = db.begin_read().unwrap();
    // Six independent real versions create several pagination entries behind
    // one genuine old-reader horizon. The following cleanup has no user writes.
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
    assert_eq!(
        reader
            .open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        0
    );
    drop(reader);
    let tx = db.begin_write().unwrap();
    let before = tx.transaction_id;
    tx.abort().unwrap();
    (file, db, before)
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_released_reader_backlog_is_bounded() {
    let (_file, db, horizon) = released_reader_backlog();
    let before = pending_before(&db, horizon);
    assert!(before >= 3, "genuine native historical backlog is required");
    // One private test limit, shared across both families and both passes.
    // Production opt-in remains absent until complete native plans qualify.
    let mut tx = db.begin_write().unwrap();
    tx.set_finite_reclamation_for_test(1);
    tx.commit().unwrap();
    let after = pending_before(&db, horizon);
    eprintln!("native_released_reader_actual_entries before={before} after={after}");
    assert_eq!(
        before - after,
        1,
        "one whole-entry finite cleanup must preserve the remaining backlog"
    );
}

fn first_pending_data_count(db: &Database, horizon: TransactionId) -> usize {
    let tx = db.begin_write().unwrap();
    let count = {
        let mut tables = tx.system_tables.lock().unwrap();
        // This real fixture's old backlog is DATA, so the chosen whole entry
        // has actual pages and the n-1 test cannot be satisfied by an empty key.
        if tables
            .get_system_table_root(SYSTEM_FREED_TABLE)
            .unwrap()
            .is_some()
        {
            let system = tables.open_system_table(SYSTEM_FREED_TABLE).unwrap();
            let end = TransactionIdWithPagination {
                transaction_id: horizon.raw_id(),
                pagination_id: 0,
            };
            assert!(system.range(..end).unwrap().next().is_none());
        }
        let data = tables.open_system_table(DATA_FREED_TABLE).unwrap();
        let end = TransactionIdWithPagination {
            transaction_id: horizon.raw_id(),
            pagination_id: 0,
        };
        let (_, value) = data.range(..end).unwrap().next().unwrap().unwrap();
        value.value().len()
    };
    tx.abort().unwrap();
    count
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_defers_whole_oversized_entry_and_makes_bounded_progress() {
    let (_file, mut db, horizon) = released_reader_backlog();
    let before = pending_before(&db, horizon);
    assert!((3..=12).contains(&before));
    let pages = first_pending_data_count(&db, horizon);
    assert!(pages > 0);
    let mut tx = db.begin_write().unwrap();
    tx.set_finite_reclamation_for_test(1);
    tx.reclamation
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .limit_page_numbers_for_test(pages - 1);
    tx.commit().unwrap();
    assert_eq!(pending_before(&db, horizon), before);
    for remaining in (0..before).rev() {
        let mut tx = db.begin_write().unwrap();
        tx.set_finite_reclamation_for_test(1);
        tx.commit().unwrap();
        assert_eq!(pending_before(&db, horizon), remaining);
    }
    assert!(db.check_integrity().unwrap());
}
