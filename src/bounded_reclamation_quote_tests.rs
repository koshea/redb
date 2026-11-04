//! Real native rewrites; numerical component only, not Source/plan authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
#[cfg(feature = "experimental-api-5")]
use crate::ReadableTable;
use crate::transactions::TransactionIdWithPagination;
use crate::{Database, Durability, ReadableDatabase, TableDefinition};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("quoted_rewrite_rows");
const ROW_COUNT: usize = 64;
fn envelope(puts: usize, deletes: usize) -> CompiledEnvelope<1> {
    CompiledEnvelope {
        tables: [NormalOperations {
            name: "quoted_rewrite_rows",
            puts,
            deletes,
            max_key_bytes: 8,
            max_value_bytes: 8192,
        }],
    }
}
fn rewrite(tx: &WriteTransaction, value: u8) {
    let mut table = tx.open_table(ROWS).unwrap();
    for key in 0_u64..64 {
        table.insert(key, [value; 8192].as_slice()).unwrap();
    }
}
fn database() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    rewrite(&tx, 0);
    tx.commit().unwrap();
    (file, db)
}
#[derive(Default, Debug)]
struct Stock {
    records: usize,
    eligible: usize,
    weighted: u64,
    allocated: u64,
    current_data_records: usize,
    current_data_pages: usize,
    current_epilogue_records: usize,
}
// This is an explicitly test-only full native census under one owned transaction.
// It does not implement bounded production resource inspection.
fn stock(tx: &WriteTransaction, current: Option<TransactionId>) -> Stock {
    let mut answer = Stock {
        allocated: tx.mem.count_allocated_pages().unwrap(),
        ..Stock::default()
    };
    let horizon = {
        let hold = tx.mem.header_hold(None).unwrap();
        tx.mem
            .oldest_active_transaction(
                tx.transaction_tracker.oldest_local_referenced_transaction(),
                &hold,
            )
            .unwrap()
            .map_or(tx.transaction_id, TransactionId::next)
    };
    let mut tables = tx.system_tables.lock().unwrap();
    for (system, definition) in [(false, DATA_FREED_TABLE), (true, SYSTEM_FREED_TABLE)] {
        if tables.get_system_table_root(definition).unwrap().is_none() {
            continue;
        }
        let table = tables.open_system_table(definition).unwrap();
        for row in table.range::<TransactionIdWithPagination>(..).unwrap() {
            let (key, pages) = row.unwrap();
            let key = key.value();
            let pages = pages.value();
            answer.records += 1;
            answer.eligible += usize::from(key.transaction_id < horizon.raw_id());
            for index in 0..pages.len() {
                answer.weighted += pages.get(index).page_size_bytes(1);
            }
            if let Some(id) = current {
                if !system && key.transaction_id == id.raw_id() {
                    answer.current_data_records += 1;
                    answer.current_data_pages += pages.len();
                }
                if system && key.transaction_id == id.next().raw_id() {
                    answer.current_epilogue_records += 1;
                }
            }
        }
    }
    answer
}
fn observe(db: &Database, id: Option<TransactionId>) -> Stock {
    let tx = db.begin_write().unwrap();
    let result = stock(&tx, id);
    tx.abort().unwrap();
    result
}

#[test]
fn native_derived_quote_keeps_up_with_continuing_real_rewrites_after_reader_release() {
    let (file, mut db) = database();
    let reader = db.begin_read().unwrap();
    for version in 1_u8..=6 {
        let tx = db.begin_write().unwrap();
        rewrite(&tx, version);
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
    let held = observe(&db, None);
    assert!(held.records >= 6 && held.weighted > 0);
    drop(reader);
    let mut saturated = 0;
    let mut observed_production = 0;
    let mut adopted_turns = 0;
    let mut feedback_turns = 0;
    let mut record_peak = 0;
    let mut allocated_peak = 0;
    let mut file_first_half_peak = 0;
    let mut file_second_half_peak = 0;
    for version in 7_u8..=134 {
        let tx = db.begin_write().unwrap();
        let before = stock(&tx, None);
        let operations = envelope(ROW_COUNT, 0);
        let view = SupportedView::inspect_for_test(&tx, &operations).unwrap();
        if view.adopted_pages != 0 {
            adopted_turns += 1;
            assert!(view.adopted_weighted_pages >= u64::try_from(view.adopted_pages).unwrap());
            assert!(view.adopted_capacity_slots >= view.adopted_pages * 2);
        }
        assert!(view.prior_tracker_ids <= before.records + 1);
        let limits = view.quote(&operations).unwrap().install_for_test().unwrap();
        assert!(limits.removed > limits.produced_data_records);
        assert!(limits.examined >= limits.removed);
        assert!(limits.page_numbers >= limits.retired_data_pages);
        assert!(limits.encoded_bytes >= limits.removed * (16 + PageList::required_bytes(400)));
        let id = tx.transaction_id;
        rewrite(&tx, version);
        tx.commit().unwrap();
        let after = observe(&db, Some(id));
        assert!(after.current_data_records <= limits.produced_data_records);
        assert!(after.current_data_pages <= limits.retired_data_pages);
        assert!(after.current_epilogue_records <= limits.system_tail_records);
        if before.eligible >= limits.removed {
            saturated += 1;
            // Exact continuing-producer stock equation. The legacy unlimited
            // negative control violates the per-turn removal bound here.
            let old_removed = before.records + after.current_data_records - after.records;
            assert_eq!(
                old_removed, limits.removed,
                "derived whole-entry turn bound"
            );
            assert!(after.records < before.records);
            assert_eq!(after.current_epilogue_records, 0);
            assert!(after.current_data_records > 0);
            observed_production += 1;
        }
        if after.current_epilogue_records != 0 {
            feedback_turns += 1;
        }
        assert!(after.records <= held.records.max(limits.steady_record_stock));
        record_peak = record_peak.max(after.records);
        allocated_peak = allocated_peak.max(after.allocated);
        let length = std::fs::metadata(file.path()).unwrap().len();
        if version < 71 {
            file_first_half_peak = file_first_half_peak.max(length);
        } else {
            file_second_half_peak = file_second_half_peak.max(length);
        }
    }
    assert!(saturated > 0 && observed_production > 0);
    assert!(adopted_turns > 0 && feedback_turns > 0);
    let final_stock = observe(&db, None);
    assert!(
        final_stock.weighted < held.weighted,
        "old weighted backlog is reclaimed despite continuing production"
    );
    assert!(
        final_stock.allocated < held.allocated,
        "actual native allocated pages are reused after pin release"
    );
    assert!(
        file_second_half_peak <= file_first_half_peak,
        "observed rewrite file high-water stabilizes"
    );
    let last = db.begin_read().unwrap();
    assert_eq!(
        last.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        134
    );
    drop(last);
    assert!(db.check_integrity().unwrap());
    eprintln!(
        "native_derived_keepup saturated={saturated} adopted={adopted_turns} feedback={feedback_turns} record_peak={record_peak} allocated_peak={allocated_peak} file_first_peak={file_first_half_peak} file_second_peak={file_second_half_peak}"
    );
}

#[test]
fn native_derived_quote_refuses_real_savepoint_and_user_nondurable_state() {
    let (_file, mut db) = database();
    let tx = db.begin_write().unwrap();
    let savepoint = tx.ephemeral_savepoint().unwrap();
    assert!(matches!(
        SupportedView::inspect_for_test(&tx, &envelope(1, 0)),
        Err(Refusal::Unsupported)
    ));
    tx.abort().unwrap();
    drop(savepoint);
    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None).unwrap();
    rewrite(&tx, 1);
    tx.commit().unwrap();
    let tx = db.begin_write().unwrap();
    assert!(!tx.mem.reclamation_quote_snapshot_for_test().post_only);
    assert!(matches!(
        SupportedView::inspect_for_test(&tx, &envelope(1, 0)),
        Err(Refusal::Unsupported)
    ));
    assert!(tx.reclamation.lock().unwrap().is_none());
    tx.commit().unwrap();
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_derived_quote_rejects_wide_tail_before_budget_installation() {
    let (_file, mut db) = database();
    let tx = db.begin_write().unwrap();
    {
        let mut tables = tx.system_tables.lock().unwrap();
        let mut table = tables.open_system_table(SYSTEM_FREED_TABLE).unwrap();
        let key = TransactionIdWithPagination {
            transaction_id: tx.transaction_id.raw_id(),
            pagination_id: 9,
        };
        let mut pages = table
            .insert_reserve(&key, PageList::required_bytes(401))
            .unwrap();
        pages.as_mut().clear();
    }
    tx.commit().unwrap();
    let tx = db.begin_write().unwrap();
    assert!(matches!(
        SupportedView::inspect_for_test(&tx, &envelope(1, 0)),
        Err(Refusal::Native(StorageError::ValueTooLarge(_)))
    ));
    assert!(tx.reclamation.lock().unwrap().is_none());
    tx.abort().unwrap();
    // The ordinary native default still handles this valid wider artifact.
    db.begin_write().unwrap().commit().unwrap();
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_derived_quote_overflow_and_second_install_leave_actual_owner_unchanged() {
    let (_file, db) = database();
    let tx = db.begin_write().unwrap();
    let operations = envelope(usize::MAX, 1);
    let view = SupportedView::inspect_for_test(&tx, &operations).unwrap();
    assert!(matches!(view.quote(&operations), Err(Refusal::Overflow)));
    assert!(tx.reclamation.lock().unwrap().is_none());
    let operations = envelope(1, 0);
    let view = SupportedView::inspect_for_test(&tx, &operations).unwrap();
    let wrong_table = CompiledEnvelope {
        tables: [NormalOperations {
            name: "different_table",
            ..operations.tables[0]
        }],
    };
    assert!(matches!(
        view.quote(&wrong_table),
        Err(Refusal::Unsupported)
    ));
    assert!(tx.reclamation.lock().unwrap().is_none());
    let first = SupportedView::inspect_for_test(&tx, &operations)
        .unwrap()
        .quote(&operations)
        .unwrap();
    first.install_for_test().unwrap();
    let second = SupportedView::inspect_for_test(&tx, &operations)
        .unwrap()
        .quote(&operations)
        .unwrap();
    assert!(matches!(second.install_for_test(), Err(Refusal::Stale)));
    assert!(tx.reclamation.lock().unwrap().is_some());
    tx.abort().unwrap();
}
