//! Structural native-table controls complement the genuine old-reader commit
//! test. Empty native lists have no page references and grant no Source evidence.
use super::*;
use crate::Database;
use crate::tree_store::Page;

fn insert_empty(tx: &WriteTransaction, system: bool, transaction: u64, pagination: u64) {
    let mut tables = tx.system_tables.lock().unwrap();
    let definition = if system {
        SYSTEM_FREED_TABLE
    } else {
        DATA_FREED_TABLE
    };
    let mut table = tables.open_system_table(definition).unwrap();
    let key = TransactionIdWithPagination {
        transaction_id: transaction,
        pagination_id: pagination,
    };
    let capacity = if system { 200 } else { 400 };
    let mut value = table
        .insert_reserve(&key, PageList::required_bytes(capacity))
        .unwrap();
    value.as_mut().clear();
}

fn keys(tx: &WriteTransaction, system: bool) -> Vec<(u64, u64)> {
    let mut tables = tx.system_tables.lock().unwrap();
    let definition = if system {
        SYSTEM_FREED_TABLE
    } else {
        DATA_FREED_TABLE
    };
    if tables.get_system_table_root(definition).unwrap().is_none() {
        return Vec::new();
    }
    let table = tables.open_system_table(definition).unwrap();
    let keys = table
        .range::<TransactionIdWithPagination>(..)
        .unwrap()
        .map(|row| {
            let (key, _) = row.unwrap();
            let key = key.value();
            (key.transaction_id, key.pagination_id)
        })
        .collect();
    keys
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_budget_is_shared_across_tables_and_passes() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    insert_empty(&tx, false, 1, 0);
    insert_empty(&tx, true, 2, 0);
    insert_empty(&tx, false, 3, 0);
    insert_empty(&tx, true, 3, 0);
    let mut budget = ReclamationBudget::for_emitted_entries(3);
    let mut decoded = 0;
    {
        let mut tables = tx.system_tables.lock().unwrap();
        let first = extract(
            &mut tables,
            TransactionId::new(3),
            true,
            &mut budget,
            |_| decoded += 1,
        )
        .unwrap();
        assert_eq!(first.removed, 2);
        let second = extract(
            &mut tables,
            TransactionId::new(4),
            false,
            &mut budget,
            |_| decoded += 1,
        )
        .unwrap();
        assert_eq!(second.removed, 1);
        assert!(second.completed.is_empty()); // Same ID still has SYSTEM work.
        let final_pass = extract(
            &mut tables,
            TransactionId::new(4),
            true,
            &mut budget,
            |_| decoded += 1,
        )
        .unwrap();
        assert_eq!(final_pass.removed, 0);
    }
    assert_eq!(decoded, 0);
    assert!(keys(&tx, false).is_empty());
    assert_eq!(keys(&tx, true), [(3, 0)]);
    tx.abort().unwrap();
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_keeps_partial_transaction_pagination_pending() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    insert_empty(&tx, false, 1, 0);
    insert_empty(&tx, false, 1, 1);
    insert_empty(&tx, true, 1, 0);
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    let extracted = extract(
        &mut tx.system_tables.lock().unwrap(),
        TransactionId::new(2),
        true,
        &mut budget,
        |_| {},
    )
    .unwrap();
    assert_eq!(extracted.removed, 1);
    assert!(extracted.completed.is_empty());
    // The first pagination was removed; the second is not renumbered or lost.
    assert_eq!(keys(&tx, false), [(1, 1)]);
    assert_eq!(keys(&tx, true), [(1, 0)]);
    tx.abort().unwrap();
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_empty_record_charges_full_reserved_capacity() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    insert_empty(&tx, true, 1, 0);
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    let before = budget.remaining.encoded_bytes;
    let extracted = extract(
        &mut tx.system_tables.lock().unwrap(),
        TransactionId::new(2),
        true,
        &mut budget,
        |_| {},
    )
    .unwrap();
    assert_eq!(extracted.removed, 1);
    assert_eq!(budget.remaining.page_numbers, 400);
    // Its head observation and exact extraction each see the entire 200-slot
    // value. Count zero cannot hide its real native KV capacity.
    assert_eq!(
        before - budget.remaining.encoded_bytes,
        2 * (KEY_BYTES + PageList::required_bytes(200))
    );
    tx.abort().unwrap();
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_preserves_strict_transaction_horizon() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    insert_empty(&tx, false, 1, 0);
    insert_empty(&tx, false, 2, 0);
    insert_empty(&tx, false, 2, u64::MAX);
    insert_empty(&tx, true, 2, 0);
    let mut budget = ReclamationBudget::for_emitted_entries(4);
    let extracted = extract(
        &mut tx.system_tables.lock().unwrap(),
        TransactionId::new(2),
        true,
        &mut budget,
        |_| {},
    )
    .unwrap();
    assert_eq!(extracted.removed, 1);
    assert_eq!(keys(&tx, false), [(2, 0), (2, u64::MAX)]);
    assert_eq!(keys(&tx, true), [(2, 0)]);
    tx.abort().unwrap();
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_refuses_unsupported_capacity_without_removing_entry() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut tables = tx.system_tables.lock().unwrap();
        let mut table = tables.open_system_table(SYSTEM_FREED_TABLE).unwrap();
        let key = TransactionIdWithPagination {
            transaction_id: 1,
            pagination_id: 0,
        };
        // Structurally valid native padding beyond this TEST support class.
        // Count zero must not hide the capacity or make it canonical corruption.
        let mut value = table
            .insert_reserve(&key, PageList::required_bytes(401))
            .unwrap();
        value.as_mut().clear();
    }
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    let result = extract(
        &mut tx.system_tables.lock().unwrap(),
        TransactionId::new(2),
        true,
        &mut budget,
        |_| {},
    );
    assert!(matches!(result, Err(StorageError::ValueTooLarge(_))));
    assert_eq!(keys(&tx, true), [(1, 0)]);
    assert_eq!(budget.remaining.removed, 1);
    tx.abort().unwrap();
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_reclamation_rejects_truncated_count_before_page_decode() {
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    assert!(budget.begin_probe());
    let truncated = PageList { data: &[1, 0] };
    assert!(matches!(
        budget.observe(&truncated),
        Err(StorageError::Corrupted(_))
    ));
    assert_eq!(budget.remaining.removed, 1);
    assert_eq!(budget.remaining.page_numbers, 400);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_nondurable_cleanup_visits_surviving_pagination_after_gap() {
    let file = crate::create_tempfile();
    let mut db = Database::create(file.path()).unwrap();
    let mut tx = db.begin_write().unwrap();
    let allocator = tx.page_allocator();
    let page = allocator
        .allocate(crate::tree_store::PAGE_SIZE, &PageTracker::ignore())
        .unwrap();
    let number = page.get_page_number();
    drop(page);
    // Establish the actual native unpersisted state for one allocated but
    // unreachable page, using the same native publication/tracker primitives
    // as non_durable_commit. No fabricated PageNumber is freed by this test.
    let durable_ancestor = tx.mem.get_last_durable_transaction_id().unwrap();
    tx.mem
        .non_durable_commit(
            tx.mem.get_data_root(),
            tx.mem.get_system_root(),
            tx.transaction_id,
            allocator.take_allocated_since_commit(),
            false,
        )
        .unwrap();
    tx.transaction_tracker
        .register_non_durable_commit(&tx.mem, tx.transaction_id, durable_ancestor, true)
        .unwrap();
    assert!(tx.mem.unpersisted(number));
    {
        let mut tables = tx.system_tables.lock().unwrap();
        let mut table = tables.open_system_table(SYSTEM_FREED_TABLE).unwrap();
        let key = TransactionIdWithPagination {
            transaction_id: tx.transaction_id.raw_id(),
            pagination_id: 1,
        };
        let mut value = table
            .insert_reserve(&key, PageList::required_bytes(1))
            .unwrap();
        value.as_mut().clear();
        value.as_mut().push_back(number);
    }
    let processed = tx
        .process_freed_pages_nondurable_helper(tx.transaction_id.next(), SYSTEM_FREED_TABLE)
        .unwrap();
    assert_eq!(processed, [tx.transaction_id]);
    assert!(!tx.mem.unpersisted(number));
    assert!(keys(&tx, true).is_empty());
    tx.transaction_tracker
        .mark_non_durable_freed_pages_processed(processed);
    tx.transaction_tracker
        .clear_pending_non_durable_commits(&tx.mem);
    tx.abort().unwrap();
    drop(allocator);
    // Return the genuine native root to its ordinary durable state and verify
    // the allocator graph after the helper freed the actual unpersisted page.
    db.begin_write().unwrap().commit().unwrap();
    assert!(db.check_integrity().unwrap());
}
