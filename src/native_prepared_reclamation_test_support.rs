//! Small real native reader-release history; full traversals below are TEST ONLY.
use super::*;
use crate::{Database, ReadableDatabase, TableDefinition};
pub(super) const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("prepared_native_rows");
pub(super) fn backlog_from_empty_reader() -> (tempfile::NamedTempFile, Database, ReadTransaction) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let old = db.begin_read().unwrap();
    for version in 0..=6_u8 {
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx.open_table(ROWS).unwrap();
            for key in 0..64 {
                rows.insert(key, [version; 8192].as_slice()).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    assert!(
        old.open_table(ROWS).is_err(),
        "the real pin precedes all user material"
    );
    (file, db, old)
}
pub(super) fn read_cut(read: &ReadTransaction) -> TransactionId {
    TransactionId::new(
        read.tree
            .transaction_guard()
            .id()
            .raw_id()
            .checked_add(1)
            .unwrap(),
    )
}
pub(super) fn count_before(db: &Database, end: TransactionId) -> usize {
    let tx = db.begin_write().unwrap();
    let count = {
        let mut tables = tx.system_tables.lock().unwrap();
        let mut count = 0;
        for definition in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE] {
            if tables.get_system_table_root(definition).unwrap().is_none() {
                continue;
            }
            let table = tables.open_system_table(definition).unwrap();
            let last = TransactionIdWithPagination {
                transaction_id: end.raw_id(),
                pagination_id: 0,
            };
            for row in table.range(..last).unwrap() {
                row.unwrap();
                count += 1;
                assert!(count <= 256, "bounded native fixture inventory");
            }
        }
        count
    };
    tx.abort().unwrap();
    count
}
pub(super) fn seed(tx: &WriteTransaction, system: bool, pagination: u64, capacity: usize) {
    let mut tables = tx.system_tables.lock().unwrap();
    let mut table = tables
        .open_system_table(if system {
            SYSTEM_FREED_TABLE
        } else {
            DATA_FREED_TABLE
        })
        .unwrap();
    let mut value = table
        .insert_reserve(
            &TransactionIdWithPagination {
                transaction_id: 0,
                pagination_id: pagination,
            },
            PageList::required_bytes(capacity),
        )
        .unwrap();
    value.as_mut().clear();
}
pub(super) fn seeded(unsupported_tail: bool) -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let mut tx = db.begin_write().unwrap();
    tx.set_finite_reclamation_for_test(0);
    seed(&tx, false, 0, 400);
    seed(&tx, false, 1, if unsupported_tail { 401 } else { 400 });
    if !unsupported_tail {
        seed(&tx, true, 0, 200);
    }
    tx.commit().unwrap();
    (file, db)
}
pub(super) fn has_key(db: &Database, system: bool, pagination: u64) -> bool {
    let tx = db.begin_write().unwrap();
    let exists = {
        let mut tables = tx.system_tables.lock().unwrap();
        let table = tables
            .open_system_table(if system {
                SYSTEM_FREED_TABLE
            } else {
                DATA_FREED_TABLE
            })
            .unwrap();
        let exists = table
            .get(&TransactionIdWithPagination {
                transaction_id: 0,
                pagination_id: pagination,
            })
            .unwrap()
            .is_some();
        exists
    };
    tx.abort().unwrap();
    exists
}
