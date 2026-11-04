//! Actual native durable histories and real commit consumer. The numerical
//! fixture marker/ceiling is not a SourceReservation or a complete write quote.
// req: STO-020, REC-001, PRJ-004
use super::*;
use crate::transactions::bounded_reclamation::ReclamationBudget;
use crate::tree_store::NativeOwnedTxnIds;
use crate::{
    Database, NativeNumericalPool, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("allocated_prefix_rows");
// A finite native-only fixture allowance. It is not a proposed Source64
// policy or the missing whole-recipe/transient demand theorem.
const FIXTURE_CEILING: usize = 64 * 1024 * 1024;
struct Marker(Arc<AtomicUsize>);
impl Drop for Marker {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn historical() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = db.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0, b"original".as_slice())
        .unwrap();
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    let savepoint = write.ephemeral_savepoint().unwrap();
    write.commit().unwrap();
    for version in [1_u8, 2] {
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for key in 0..512_u64 {
                rows.insert(key, [version; 8192].as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
    }
    drop(savepoint); // Historical durable allocated rows still exist now.
    (file, db)
}
fn allocated_keys(db: &Database) -> Vec<(u64, u64)> {
    // Read-only SYSTEM inspection; no writer/ID advance can invalidate a plan.
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let Some(tree) = system
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(DATA_ALLOCATED_TABLE.name())
        .unwrap()
    else {
        return Vec::new();
    };
    let mut result = Vec::new();
    for row in tree.range::<_, TransactionIdWithPagination>(&..).unwrap() {
        let entry = row.unwrap();
        let key = entry.key();
        result.push(order(&key));
        let pages = entry.value();
        for index in 0..pages.len() {
            assert!(
                read.mem.is_allocated(pages.get(index)),
                "historical allocation must remain allocated"
            );
        }
        assert!(result.len() <= 32, "bounded real fixture census");
    }
    result
}
fn observation(
    db: &Database,
) -> (
    ReadTransaction,
    NativeNumericalPool,
    crate::tree_store::NativeNumericalEpoch,
    Arc<AtomicUsize>,
) {
    // Genuine ordinary shape observation for this component only. It is not
    // a claimed cold Source entry or a Source-attributed baseline allocation.
    {
        let read = db.begin_read().unwrap();
        let table = read.open_table(ROWS).unwrap();
        assert!(table.get(0).unwrap().is_some());
        drop(table);
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        let mut budget = ReclamationBudget::for_emitted_entries(0);
        system
            .prepare_profiled_system_resources(&mut budget)
            .unwrap();
    }
    let plan = db.plan_current_read_context().unwrap();
    assert!(usize::try_from(plan.peak_bytes()).unwrap() <= FIXTURE_CEILING);
    let retired = Arc::new(AtomicUsize::new(0));
    let pool =
        NativeNumericalPool::from_retained_data(FIXTURE_CEILING, Marker(retired.clone())).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    (read, pool, numerical, retired)
}
fn budget(numerical: &crate::tree_store::NativeNumericalEpoch) -> ReclamationBudget {
    let mut result = ReclamationBudget::for_emitted_entries(3);
    result
        .install_completed_storage(
            [
                NativeOwnedTxnIds::prepare_source(3, numerical.resources()).unwrap(),
                NativeOwnedTxnIds::prepare_source(3, numerical.resources()).unwrap(),
            ],
            numerical.resources(),
        )
        .unwrap();
    result
}

#[test]
fn native_allocated_prefix_real_same_id_pagination_keeps_named_pages_allocated() {
    let (_file, mut db) = historical();
    let before = allocated_keys(&db);
    assert!(before.len() >= 4);
    assert_eq!(
        before[0].0, before[1].0,
        "genuine emitted pagination, not injected keys"
    );
    let old = db.begin_read().unwrap();
    let (read, pool, numerical, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut budget = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut budget,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    let token = prepared
        .budget
        .as_ref()
        .unwrap()
        .allocated
        .as_ref()
        .unwrap();
    assert_eq!(token.points.len(), 1);
    assert_eq!(order(token.first.as_ref().unwrap()), before[0]);
    assert_eq!(order(token.after.as_ref().unwrap()), before[1]);
    assert!(token.frontier.is_some());
    assert!(token.path_visits > 0 && token.path_bytes > 0);
    let horizon = prepared
        .budget
        .as_ref()
        .unwrap()
        .detachment_state_for_test()
        .0;
    let mut write = db.begin_write().unwrap();
    prepared.install_into(&mut write).unwrap();
    assert_eq!(
        write
            .reclamation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .detachment_state_for_test()
            .0,
        horizon
    );
    write.commit().unwrap();
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before[1..]);
    assert_eq!(
        old.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        [2; 8192]
    );
    drop(old);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(pool);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_allocated_prefix_abort_preserves_exact_historical_rows_and_retry_progresses() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    let mut write = db.begin_write().unwrap();
    prepared.install_into(&mut write).unwrap();
    write.abort().unwrap();
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(pool);
    // Fresh actual root/generation, not reuse of a stale numerical token.
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    let mut write = db.begin_write().unwrap();
    prepared.install_into(&mut write).unwrap();
    write.commit().unwrap();
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before[1..]);
    assert_eq!(pool.reserved_bytes(), 0);
}

#[test]
fn native_allocated_prefix_detachment_keeps_original_fence_and_same_pool_after_read_drop() {
    let (_file, db) = historical();
    let (read, pool, numerical, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_profiled_system_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    let before = pool.reserved_bytes();
    assert!(before > 0);
    let detached = match prepared.into_detached(&db, &epoch) {
        Ok(value) => value,
        Err(error) => panic!("unexpected component refusal: {:?}", error.error()),
    };
    drop(read);
    drop(epoch);
    detached.require_current().unwrap();
    drop(numerical);
    drop(pool);
    assert_eq!(retired.load(Ordering::SeqCst), 0);
    drop(detached);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}

#[test]
fn native_allocated_prefix_closed_serial_refuses_before_install() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    numerical.close().unwrap();
    let mut write = db.begin_write().unwrap();
    assert!(prepared.install_into(&mut write).is_err());
    assert!(write.reclamation.lock().unwrap().is_none());
    write.abort().unwrap();
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(pool.reserved_bytes(), 0);
    let (_read, _pool, _numerical, _) = observation(&db);
}

#[test]
fn native_allocated_prefix_interrupted_before_main_publication_rolls_back_historical_deletion() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    let mut write = db.begin_write().unwrap();
    prepared.install_into(&mut write).unwrap();
    let horizon = write
        .flush_data_allocated_pages(crate::tree_store::NativeOwnedPageVec::default())
        .unwrap();
    assert_eq!(horizon, before[1].0);
    assert_eq!(
        write
            .reclamation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .allocated
            .as_ref()
            .unwrap()
            .post_horizon,
        Some(horizon)
    );
    // Real native rollback after actual SYSTEM deletion, before any durable
    // publication. No synthetic result, recovered stamp or physical PN free.
    write.abort().unwrap();
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(pool.reserved_bytes(), 0);
}

#[test]
fn native_allocated_prefix_refuses_actual_inherited_volatile_data_before_capture() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let mut write = db.begin_write().unwrap();
    write.set_durability(crate::Durability::None).unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0, [9_u8; 8192].as_slice())
        .unwrap();
    write.commit().unwrap();
    let (read, pool, numerical, _) = observation(&db);
    assert!(!read.mem.reclamation_quote_snapshot_for_test().post_only);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let result = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        );
    assert!(result.is_err());
    drop(result);
    assert!(limits.allocated.is_none());
    drop(limits);
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(pool);
    // An ordinary Immediate commit retains its existing full semantics and
    // resolves its real volatile DATA records without Source allowance.
    db.begin_write().unwrap().commit().unwrap();
    assert!(allocated_keys(&db).is_empty());
}

#[test]
fn native_allocated_prefix_refuses_foreign_completed_buffer_pool_before_key_storage() {
    let (_file, db) = historical();
    let (read, pool, numerical, _) = observation(&db);
    let foreign = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let foreign_epoch = foreign.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let mut limits = budget(&foreign_epoch);
    let before = pool.reserved_bytes();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let result = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        );
    assert!(result.is_err());
    drop(result);
    assert!(limits.allocated.is_none());
    assert_eq!(pool.reserved_bytes(), before);
    assert!(foreign.reserved_bytes() > 0);
    drop(limits);
    assert_eq!(foreign.reserved_bytes(), 0);
    drop(epoch);
    drop(read);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(foreign_epoch);
}

#[test]
fn native_allocated_prefix_complete_historical_prefix_and_empty_successor_remove_cap() {
    let (_file, mut db) = historical();
    let before = allocated_keys(&db);
    assert!(!before.is_empty());
    let old = db.begin_read().unwrap();
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(before.len()),
        )
        .unwrap();
    let token = prepared
        .budget
        .as_ref()
        .unwrap()
        .allocated
        .as_ref()
        .unwrap();
    assert_eq!(token.points.len(), before.len());
    assert!(token.after.is_none(), "actual exact-end successor proof");
    let mut write = db.begin_write().unwrap();
    prepared.install_into(&mut write).unwrap();
    write.commit().unwrap();
    drop(epoch);
    drop(read);
    assert!(allocated_keys(&db).is_empty());
    assert_eq!(
        old.open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        [2; 8192]
    );
    drop(old);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(pool);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn native_allocated_prefix_stale_real_writer_generation_leaves_no_installed_owner() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        )
        .unwrap();
    db.begin_write().unwrap().abort().unwrap();
    let mut write = db.begin_write().unwrap();
    assert!(prepared.install_into(&mut write).is_err());
    assert!(write.reclamation.lock().unwrap().is_none());
    write.abort().unwrap();
    drop(epoch);
    drop(read);
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(pool.reserved_bytes(), 0);
}

fn deliberately_malformed_history(
    page: crate::tree_store::PageNumber,
    successor: bool,
) -> (tempfile::NamedTempFile, Database) {
    // Deliberate malformed metadata fixture, NOT a valid native writer output
    // or a corruption-causation claim. Native typed insertion computes the
    // actual envelope/checksum; only one PN in a real historical row changes.
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = db.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0, b"before".as_slice())
        .unwrap();
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    let savepoint = write.ephemeral_savepoint().unwrap();
    write.commit().unwrap();
    for version in [1_u8, 2] {
        let write = db.begin_write().unwrap();
        write
            .open_table(ROWS)
            .unwrap()
            .insert(0, [version; 64].as_slice())
            .unwrap();
        write.commit().unwrap();
    }
    let write = db.begin_write().unwrap();
    {
        let mut tables = write.system_tables.lock().unwrap();
        let mut table = tables.open_system_table(DATA_ALLOCATED_TABLE).unwrap();
        let key = {
            let mut entries = table.range::<TransactionIdWithPagination>(..).unwrap();
            let (key, _) = entries.nth(usize::from(successor)).unwrap().unwrap();
            key.value()
        };
        let mut value = table
            .insert_reserve(&key, PageList::allocated_capacity_bytes())
            .unwrap();
        value.as_mut().clear();
        value.as_mut().push_back(page);
    }
    // The genuine savepoint keeps the edited historical row out of ordinary
    // allocated purge. No validation is bypassed in the actual inspector.
    write.commit().unwrap();
    drop(savepoint);
    (file, db)
}

fn assert_malformed_row_still_present(
    read: &ReadTransaction,
    epoch: &crate::transaction_tracker::NativeResourceEpoch<'_>,
    bad: crate::tree_store::PageNumber,
    successor: bool,
) {
    let system = read.capture_native_system_read(epoch).unwrap();
    let tree = system
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(DATA_ALLOCATED_TABLE.name())
        .unwrap()
        .unwrap();
    let mut after = None;
    for index in 0..=usize::from(successor) {
        let mut visits = CompiledPathVisits::for_freed_probe();
        let (entry, _) = tree
            .read_native_freed_after(after.as_ref(), &system.epoch, &mut visits)
            .unwrap();
        let entry = entry.unwrap();
        if index == usize::from(successor) {
            assert_eq!(
                entry.page_list().get(0),
                bad,
                "refusal must not delete/normalize malformed metadata"
            );
        }
        after = Some(copy_key(entry.key()));
    }
}

#[test]
fn native_allocated_prefix_selected_unsupported_pn_order_refuses_without_open_or_deletion() {
    let bad = crate::tree_store::PageNumber::from_le_bytes((31_u64 << 59).to_le_bytes());
    let (_file, db) = deliberately_malformed_history(bad, false);
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let result = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        );
    assert!(
        matches!(&result, Err(E::Malformed)),
        "unsupported selected PN must fail the pure checked native layout walk"
    );
    drop(result);
    assert!(limits.allocated.is_none());
    assert_malformed_row_still_present(&read, &epoch, bad, false);
    drop(limits);
    drop(epoch);
    drop(read);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
}

#[test]
fn native_allocated_prefix_successor_out_of_layout_pn_refuses_without_open_or_deletion() {
    // Representable native region, far outside this tiny actual artifact.
    let bad = crate::tree_store::PageNumber::new(1_000_000, 0, 0);
    let (_file, db) = deliberately_malformed_history(bad, true);
    let (read, pool, numerical, _) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = budget(&numerical);
    let result = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(
            &mut limits,
            NativeAllocatedPrefixWork::for_test(1),
        );
    assert!(
        matches!(&result, Err(E::Malformed)),
        "first unselected PN must fail the same actual-layout walk"
    );
    drop(result);
    assert!(limits.allocated.is_none());
    assert_malformed_row_still_present(&read, &epoch, bad, true);
    drop(limits);
    drop(epoch);
    drop(read);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
}
