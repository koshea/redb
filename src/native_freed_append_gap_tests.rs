//! Actual typed native interval/component fixtures, not a whole Source writer.
//! The padded empty SYSTEM records below exercise real native separators and
//! checksums; they are not application history or future emission authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::transactions::bounded_reclamation::ReclamationBudget;
use crate::transactions::{DATA_FREED_TABLE, SYSTEM_FREED_TABLE};
use crate::tree_store::{NativeFreedAppendGapShape, NativeNumericalEpoch};
use crate::{
    Builder, Database, NativeNumericalPool, ReadTransaction, ReadableDatabase, ReadableTable,
    StorageError, TableDefinition,
};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("append_gap_rows");
const BASE: u64 = 255;
const SEEDED: u64 = 1024;
// Fixture-only finite numerical overallowance for at most four native paths
// and their actual copied pages/retained records. No Source quota theorem.
const CEILING: usize = 64 * 1024 * 1024;

type FreedTree = Btree<TransactionIdWithPagination, PageList<'static>>;
fn key(transaction_id: u64, pagination_id: u64) -> TransactionIdWithPagination {
    TransactionIdWithPagination {
        transaction_id,
        pagination_id,
    }
}
fn fixture() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let mut write = db.begin_write().unwrap();
    write.set_finite_reclamation_for_test(0);
    write
        .open_table(ROWS)
        .unwrap()
        .insert(b"key".as_slice(), b"old".as_slice())
        .unwrap();
    {
        let mut namespace = write.system_tables.lock().unwrap();
        let mut tree = namespace.open_system_table(SYSTEM_FREED_TABLE).unwrap();
        // Native ascending insert appends two padded values per 4KiB leaf.
        // Deleting alternate rows leaves a value >one third of a base page,
        // preserving native stale separator keys instead of forcing merges.
        for index in 0..SEEDED {
            let mut value = tree
                .insert_reserve(&key(BASE + index, 0), PageList::required_bytes(200))
                .unwrap();
            value.as_mut().clear();
        }
        for index in (1..SEEDED).step_by(2) {
            assert!(tree.remove(&key(BASE + index, 0)).unwrap().is_some());
        }
    }
    write.commit().unwrap();
    // A genuine ordinary replacement creates an older DATA_FREED record.
    // Finite zero service preserves it for the exact additional-key probe.
    let mut write = db.begin_write().unwrap();
    write.set_finite_reclamation_for_test(0);
    write
        .open_table(ROWS)
        .unwrap()
        .insert(b"key".as_slice(), b"new".as_slice())
        .unwrap();
    write.commit().unwrap();
    // Actual ordinary observations only. This is not a cold-entry proof.
    {
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(ROWS)
                .unwrap()
                .get(b"key".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"new"
        );
        let epoch = db.capture_native_resource_epoch().unwrap();
        let actual = read.capture_native_system_read(&epoch).unwrap();
        for name in [DATA_FREED_TABLE.name(), SYSTEM_FREED_TABLE.name()] {
            let tree = actual
                .open_normal::<TransactionIdWithPagination, PageList<'static>>(name)
                .unwrap()
                .unwrap();
            assert!(tree.get(&key(BASE, 0)).is_ok());
            assert!(tree.get(&key(BASE + SEEDED, 0)).is_ok());
        }
    }
    (file, db)
}
fn observation(db: &Database) -> (ReadTransaction, NativeNumericalPool, NativeNumericalEpoch) {
    let plan = db.plan_current_read_context().unwrap();
    let pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    (read, pool, numerical)
}
fn collect<'tree, 'epoch, 'tracker>(
    tree: &'tree FreedTree,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    start: &TransactionIdWithPagination,
) -> NativeFreedAppendGapShape {
    let recipe = NativeFreedAppendGapRecipe::for_native_interval(tree, epoch, start).unwrap();
    tree.prepare_freed_append_gap(&recipe, epoch)
        .unwrap()
        .collect()
        .unwrap()
}
fn oom(error: &E) -> bool {
    matches!(error, E::Native(StorageError::Io(error))
        if error.kind() == std::io::ErrorKind::OutOfMemory)
}

#[test]
fn native_freed_append_gap_same_leaf_retains_records_until_final_owner() {
    let (_file, db) = fixture();
    let (read, pool, _numerical) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = read.capture_native_system_read(&epoch).unwrap();
    let tree = actual
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(SYSTEM_FREED_TABLE.name())
        .unwrap()
        .unwrap();
    let lower = key(BASE + SEEDED, 1);
    let upper = key(BASE + SEEDED + 1, 0);
    let shape = collect(&tree, &actual.epoch, &lower);
    shape.require_interval_for_test(&lower, &upper).unwrap();
    assert_eq!(shape.old_leaves(), 1);
    assert!(shape.old_pages() > 1);
    assert!(shape.old_physical_bytes() >= 4096 * shape.old_pages() as u64);
    assert!(shape.observed_fetches_for_test() > shape.old_pages());
    drop(tree);
    drop(actual);
    drop(read);
    drop(epoch);
    assert!(
        pool.reserved_bytes() > 0,
        "retained original structural records remain charged"
    );
    drop(shape);
    assert_eq!(
        pool.reserved_bytes(),
        0,
        "no copied page survives final structural owner"
    );
}

#[test]
fn native_freed_append_gap_cross_parent_has_actual_checksum_edge_witness() {
    let (_file, db) = fixture();
    let (read, pool, _numerical) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = read.capture_native_system_read(&epoch).unwrap();
    let tree = actual
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(SYSTEM_FREED_TABLE.name())
        .unwrap()
        .unwrap();
    let baseline = pool.reserved_bytes();
    let mut found = false;
    // Bounded fixture discovery, never an unbounded production tree walk.
    for index in (1..SEEDED).step_by(2) {
        let start = key(BASE + index, 0);
        let shape = collect(&tree, &actual.epoch, &start);
        if let Some((left, left_checksum, left_edge, right, right_checksum, right_edge)) =
            shape.edge_witness_for_test()
        {
            if left != right {
                assert!(
                    shape.parent_checksums_for_test(),
                    "actual non-deferred parent checksums checked by owning walker"
                );
                let _actual_parent_checksums = (left_checksum, right_checksum);
                assert_eq!(shape.old_leaves(), 2);
                // The real different parent+edge witness, rather than height,
                // establishes this fixture crossed an original parent edge.
                assert_ne!((left, left_edge), (right, right_edge));
                found = true;
            }
        }
        drop(shape);
        assert_eq!(
            pool.reserved_bytes(),
            baseline,
            "repeated census returns all temporary capacity"
        );
        if found {
            break;
        }
    }
    assert!(
        found,
        "actual native stale-separator fixture must cross a parent"
    );
}

#[test]
fn native_freed_append_gap_refuses_foreign_tree_epoch_and_overflow() {
    let (_file, db) = fixture();
    let (read, _pool, _numerical) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let other_epoch = db.capture_native_resource_epoch().unwrap();
    let actual = read.capture_native_system_read(&epoch).unwrap();
    let tree = actual
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(SYSTEM_FREED_TABLE.name())
        .unwrap()
        .unwrap();
    let foreign = actual
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(DATA_FREED_TABLE.name())
        .unwrap()
        .unwrap();
    let recipe = NativeFreedAppendGapRecipe::for_native_interval(
        &tree,
        &actual.epoch,
        &key(BASE + SEEDED, 1),
    )
    .unwrap();
    assert!(matches!(
        recipe.require_for(&foreign, &actual.epoch),
        Err(E::ForeignOwner)
    ));
    assert!(matches!(
        recipe.require_for(&tree, &other_epoch),
        Err(E::ForeignOwner)
    ));
    assert!(matches!(
        NativeFreedAppendGapRecipe::for_native_interval(&tree, &actual.epoch, &key(u64::MAX, 0)),
        Err(E::Overflow)
    ));
    let shape = tree
        .prepare_freed_append_gap(&recipe, &actual.epoch)
        .unwrap()
        .collect()
        .unwrap();
    assert!(matches!(
        shape.require_interval_for_test(&key(BASE + SEEDED, 2), &key(BASE + SEEDED + 1, 0)),
        Err(E::ForeignOwner)
    ));
}

#[test]
fn native_freed_append_gap_record_pressure_refuses_and_refunds_before_writer() {
    let (_file, db) = fixture();
    let (read, pool, numerical) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = read.capture_native_system_read(&epoch).unwrap();
    let tree = actual
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(SYSTEM_FREED_TABLE.name())
        .unwrap()
        .unwrap();
    let baseline = pool.reserved_bytes();
    let recipe = NativeFreedAppendGapRecipe::for_native_interval(
        &tree,
        &actual.epoch,
        &key(BASE + SEEDED, 1),
    )
    .unwrap();
    let preparation = tree
        .prepare_freed_append_gap(&recipe, &actual.epoch)
        .unwrap();
    let resolver = read.tree.read_page_resolver();
    let pressure = resolver
        .admit_fixed_observation(CEILING - pool.reserved_bytes())
        .unwrap();
    let failure = match preparation.collect() {
        Err(error) => error,
        Ok(_) => panic!("record Vec allocation must be refused before reserve"),
    };
    assert!(oom(&failure));
    drop(failure);
    drop(pressure);
    assert_eq!(pool.reserved_bytes(), baseline);
    drop(recipe);
    drop(tree);
    drop(actual);
    drop(read);
    drop(epoch);
    // The genuine resolver retains its original context metadata, without a
    // read pin. Ordinary writer does not depend on remaining Source capacity.
    let pressure = resolver
        .admit_fixed_observation(CEILING - pool.reserved_bytes())
        .unwrap();
    assert_eq!(pool.reserved_bytes(), CEILING);
    db.begin_write().unwrap().abort().unwrap();
    drop(pressure);
    drop(resolver);
    assert_eq!(numerical.resources().current(), 0);
    assert_eq!(pool.reserved_bytes(), 0);
}

#[test]
fn native_freed_append_gap_extra_data_probe_spends_exact_original_examination_and_bytes() {
    let (_file, db) = fixture();
    let (read, _pool, _numerical) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(0);
    let mut observer = actual.observe_freed_paths(&mut budget).unwrap();
    assert!(!observer.next().unwrap());
    let positions = observer.inspect_append_positions().unwrap();
    let before = observer.budget.remaining_for_observation_test();
    observer
        .require_data_main_interval_absent(&positions)
        .unwrap();
    let after = observer.budget.remaining_for_observation_test();
    assert_eq!(after.0, before.0, "no removal allowance minted");
    assert_eq!(after.2, before.2, "no PageNumber service allowance minted");
    assert_eq!(before.1 - after.1, 1);
    assert_eq!(before.3 - after.3, 16 + PageList::required_bytes(400));
    drop(observer);
    // Actual full preparation needs exactly the new fifth probe: two append
    // positions, DATA collision and two family-tail probes. Same K0 service.
    let entry_bytes = 16 + PageList::required_bytes(400);
    let mut old = ReclamationBudget::for_emitted_entries(0);
    old.limit_planning_for_test(4, 4 * entry_bytes);
    assert!(
        matches!(read.capture_native_system_read(&epoch), Err(E::Epoch(_))),
        "existing live inspection excludes a second claim"
    );
    drop(actual);
    let failed = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_profiled_system_with_freed_frontiers(&mut old);
    assert!(matches!(failed, Err(E::UnsupportedState)));
    let mut new = ReclamationBudget::for_emitted_entries(0);
    let prepared = read
        .capture_native_system_read(&epoch)
        .unwrap()
        .prepare_profiled_system_with_freed_frontiers(&mut new)
        .unwrap();
    prepared.require_before_write().unwrap();
    assert_eq!(prepared.system_resources().selected_records(), 0);
}

#[test]
fn native_freed_append_gap_byte_pressure_precedes_actual_extra_probe() {
    let (_file, db) = fixture();
    let (read, _pool, _numerical) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = read.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(0);
    let mut observer = actual.observe_freed_paths(&mut budget).unwrap();
    assert!(!observer.next().unwrap());
    let positions = observer.inspect_append_positions().unwrap();
    let bytes = 16 + PageList::required_bytes(400);
    observer.budget.limit_planning_for_test(1, bytes - 1);
    let before = observer.budget.remaining_for_observation_test();
    assert!(matches!(
        observer.require_data_main_interval_absent(&positions),
        Err(E::UnsupportedState)
    ));
    assert_eq!(
        observer.budget.remaining_for_observation_test(),
        before,
        "insufficient complete-entry allowance refuses before examination"
    );
    observer.budget.limit_planning_for_test(1, bytes);
    observer
        .require_data_main_interval_absent(&positions)
        .unwrap();
    let after = observer.budget.remaining_for_observation_test();
    assert_eq!(after.1, 0);
    assert_eq!(after.3, 0);
    assert_eq!(after.0, 0);
    assert_eq!(after.2, 0);
}
