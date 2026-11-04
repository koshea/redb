//! Real native fixed execution. Unit numerical retainer and explicit finite
//! fixture demand; no actual Source T, whole15, cold-entry or progress claim.
// req: STO-020, REC-001, PRJ-004
use super::*;
use crate::transactions::{NativeSystemFamily, empty_reclamation_budget_for_native_test};
use crate::tree_store::{NativeFixedPoint, NativeOwnedFixedRecipe};
use crate::{NativeNumericalPool, ReadableDatabase, ReadableTable};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("fixed_cleanup_rows");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("fixed_cleanup_meta");
const FIXTURE_CEILING: usize = 64 * 1024 * 1024;
// Explicit numerical fixture bounds for two tiny puts, one whole historical
// deletion, the real finite SYSTEM pre/post work and namespace updates. This
// overallowance is NOT a production demand formula or a Source64 theorem.
const FIXTURE_REQUESTS: usize = 512;

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
        .insert(b"first".as_slice(), b"original".as_slice())
        .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert("state", b"old".as_slice())
        .unwrap();
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    let savepoint = write.ephemeral_savepoint().unwrap();
    write.commit().unwrap();
    for version in [1_u8, 2] {
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for index in 0..512_u64 {
                rows.insert(index.to_be_bytes().as_slice(), [version; 8192].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
    }
    drop(savepoint);
    // Genuine ordinary observations, explicitly a component fixture. Neither
    // this warmup nor its cache capacity is attributed to Source admission.
    {
        let read = db.begin_read().unwrap();
        assert!(
            read.open_table(ROWS)
                .unwrap()
                .get(0_u64.to_be_bytes().as_slice())
                .unwrap()
                .is_some()
        );
        assert!(
            read.open_table(META)
                .unwrap()
                .get("state")
                .unwrap()
                .is_some()
        );
        let epoch = db.capture_native_resource_epoch().unwrap();
        let mut budget = empty_reclamation_budget_for_native_test();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut budget)
            .unwrap();
    }
    (file, db)
}
fn allocated_keys(db: &Database) -> Vec<(u64, u64)> {
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    system.fixed_cleanup_allocated_keys_for_fixture()
}
fn input() -> [NativeFixedPoint<'static>; 2] {
    [
        NativeFixedPoint {
            table: ROWS.name(),
            mutation: NativePointMutation::Put {
                key: NativePointKey::Bytes(b"first"),
                value: b"replacement",
            },
        },
        NativeFixedPoint {
            table: META.name(),
            mutation: NativePointMutation::Put {
                key: NativePointKey::Str("state"),
                value: b"new",
            },
        },
    ]
}
fn metadata<'work>(
    db: &'work Database,
    numerical: &NativeNumericalEpoch,
) -> NativePreparedTxnMetadata<'work> {
    let mut system = NativeSystemFamily::all().map(NativeSystemFamily::name);
    system.sort_unstable();
    NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &system,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap()
}
fn data<'work>(
    db: &'work Database,
    read: &ReadTransaction,
    numerical: &NativeNumericalEpoch,
) -> NativeFixedDataCensus<'work> {
    let recipe = NativeOwnedFixedRecipe::prepare(&input(), numerical.resources()).unwrap();
    match NativeFixedDataCensus::capture(
        read,
        recipe,
        db.capture_native_resource_epoch().unwrap(),
        numerical,
    ) {
        Ok(data) => data,
        Err(_) => panic!("actual fixed DATA fixture capture refused"),
    }
}
fn combined<'work>(
    db: &'work Database,
    read: &ReadTransaction,
    numerical: &NativeNumericalEpoch,
) -> NativePreparedFixedCleanup<'work> {
    let data = data(db, read, numerical);
    match NativePreparedFixedCleanup::capture(
        db,
        read,
        data,
        metadata(db, numerical),
        empty_reclamation_budget_for_native_test(),
        numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "actual combined fixture capture refused: stage={:?}, class={}",
            failure.stage_for_test(),
            failure.error().class_for_test()
        ),
    }
}

#[test]
fn native_fixed_cleanup_checked_executor_services_one_historical_row_without_allocated_emission() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    assert!(before.len() >= 4, "real savepoint pagination debt");
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read); // No SYSTEM read/inspection crosses the real writer boundary.
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    assert!(
        result.is_ok(),
        "real checked fixed Immediate execution must succeed"
    );
    drop(result);
    let after = allocated_keys(&db);
    assert_eq!(
        after.as_slice(),
        &before[1..],
        "exact Ka1 historical service and ZERO new allocated rows"
    );
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"replacement"
    );
    assert_eq!(
        read.open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"new"
    );
}

#[test]
fn native_fixed_cleanup_production_allocated_tag_refuses_ordinary_install_before_mutation() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let data = data(&db, &read, &numerical);
    let mut metadata = metadata(&db, &numerical);
    let mut budget = empty_reclamation_budget_for_native_test();
    metadata
        .move_completed_into(&db.mem, numerical.resources(), &mut budget)
        .unwrap();
    let work = NativeAllocatedPrefixWork::for_checked_fixed_recipe(&data, &numerical).unwrap();
    let (_, epoch) = data.fixed_cleanup_parts();
    let prepared = read
        .capture_native_system_read(epoch)
        .unwrap()
        .prepare_reclamation_with_allocated_prefix(&mut budget, work)
        .unwrap();
    let mut ordinary = db.begin_write().unwrap();
    assert!(
        matches!(
            prepared.install_into(&mut ordinary),
            Err(E::UnsupportedState)
        ),
        "production policy requires constructor-proven Ignore, not no-savepoints alone"
    );
    ordinary.abort().unwrap();
    drop(metadata);
    drop(data);
    drop(read);
    assert_eq!(
        allocated_keys(&db),
        before,
        "refusal precedes historical deletion"
    );
    // Ordinary writer remains available without this numerical Source owner.
    let ordinary = db.begin_write().unwrap();
    ordinary
        .open_table(META)
        .unwrap()
        .insert("state", b"ordinary".as_slice())
        .unwrap();
    ordinary.commit().unwrap();
}

#[test]
fn native_fixed_cleanup_stale_writer_claim_preserves_recipe_and_historical_rows() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read);
    db.begin_write().unwrap().abort().unwrap(); // Real native generation advance.
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    let Err(failure) = result else {
        panic!("stale actual writer generation accepted")
    };
    assert!(matches!(
        failure.error(),
        NativeFixedCleanupError::Path(E::Epoch(_))
    ));
    assert!(
        pool.reserved_bytes() > 0,
        "original error/recipe/metadata guards retained"
    );
    drop(failure);
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"old"
    );
}

#[test]
fn native_fixed_cleanup_failure_after_first_real_point_rolls_back_and_retains_error_allowance() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let input = NativeFixedWriterInputs::for_fixture(
        context,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
    )
    .fail_after_first_real_point();
    let Err(failure) = owner.execute(&numerical, input) else {
        panic!("real first-point mutation fault must return owning failure")
    };
    assert!(
        matches!(failure.error(), NativeFixedCleanupError::InjectedAfterFirstPoint {
        materialized_bytes,
    } if *materialized_bytes > 0),
        "fault must follow successful real mutation with live physical bytes"
    );
    let held = pool.reserved_bytes();
    assert!(
        held > 0,
        "actual original/error/recipe allowance retained after rollback"
    );
    // Ordinary public reads see the original two values. The second put was
    // never reached; the first real put was rolled back by the real writer.
    {
        let ordinary = db.begin_read().unwrap();
        assert_eq!(
            ordinary
                .open_table(ROWS)
                .unwrap()
                .get(b"first".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"original"
        );
        assert_eq!(
            ordinary
                .open_table(META)
                .unwrap()
                .get("state")
                .unwrap()
                .unwrap()
                .value(),
            b"old"
        );
    }
    assert_eq!(
        allocated_keys(&db),
        before,
        "rollback preserves every historical row and emits no new allocation row"
    );
    drop(failure);
    assert!(
        pool.reserved_bytes() < held,
        "error owners release only after error destruction"
    );
    // Persistent Source-caused capacity is not refunded on logical rollback.
    // Genuine ordinary writer handoff physically replaces any such buffers.
    db.begin_write().unwrap().abort().unwrap();
    assert_eq!(
        pool.reserved_bytes(),
        0,
        "final physical handoff retires remaining numerical capacity"
    );
    assert_eq!(allocated_keys(&db), before);
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cleanup_census_derived_data_capacity_executes_real_freed_transfers() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let data = data(&db, &read, &numerical);
    let exact_data = data.old_data_page_numbers_upper().unwrap();
    assert!(exact_data > 0, "real committed original frontier");
    let mut system = NativeSystemFamily::all().map(NativeSystemFamily::name);
    system.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &system,
        exact_data,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data,
        metadata,
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "derived DATA capture refused: {:?}/{}",
            failure.stage_for_test(),
            failure.error().class_for_test()
        ),
    };
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    assert!(
        owner
            .execute(
                &numerical,
                NativeFixedWriterInputs::for_fixture(
                    context,
                    FIXTURE_REQUESTS,
                    FIXTURE_REQUESTS,
                    FIXTURE_REQUESTS
                )
            )
            .is_ok()
    );
    assert_eq!(allocated_keys(&db).as_slice(), &before[1..]);
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"replacement"
    );
    assert_eq!(
        read.open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"new"
    );
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cleanup_quarantined_actual_local_refuses_commit_and_rolls_back() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let failure = match owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        )
        .quarantine_before_consuming_commit(),
    ) {
        Err(failure) => failure,
        Ok(()) => panic!("quarantined real scratch must refuse before consuming commit"),
    };
    match failure.error() {
        NativeFixedCleanupError::RejectedQuarantinedLocal {
            error,
            materialized_bytes,
        } => {
            assert!(
                matches!(error, StorageError::Io(error) if error.kind() == std::io::ErrorKind::OutOfMemory),
                "actual poisoned-pool typed refusal"
            );
            assert!(
                *materialized_bytes > 0,
                "actual physical page mutation preceded pool refusal"
            );
        }
        _ => panic!("failure must be the actual poisoned-pool precommit check"),
    }
    assert_eq!(
        allocated_keys(&db),
        before,
        "normal rollback preserves historical rows; no new DATA allocation row"
    );
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    assert_eq!(
        read.open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"old"
    );
    drop(read);
    assert!(
        numerical.resources().current() > 0,
        "owning failure keeps original read/control allowance"
    );
    drop(failure);
    db.begin_write().unwrap().abort().unwrap();
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cleanup_undersized_data_metadata_capture_refuses_before_claim_and_refunds() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let baseline = numerical.resources().current();
    let data = data(&db, &read, &numerical);
    assert!(
        data.old_data_page_numbers_upper().unwrap() > 0,
        "actual committed DATA frontier"
    );
    let mut system = NativeSystemFamily::all().map(NativeSystemFamily::name);
    system.sort_unstable();
    // The actual original buffers exist and belong to the same pool; only the
    // DATA finite demand is missing. No direct capacity-predicate call/bypass.
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &system,
        0,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap();
    let failure = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data,
        metadata,
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Err(failure) => failure,
        Ok(_) => panic!("undersized actual DATA owner must refuse before writer claim"),
    };
    assert!(
        matches!(failure.stage_for_test(), NativeFixedCaptureStage::DataFinal),
        "must reach actual derived DATA demand check"
    );
    assert_eq!(failure.error().class_for_test(), "path_native_storage");
    assert!(
        numerical.resources().current() > baseline,
        "actual original failure owners stay charged"
    );
    assert_eq!(
        allocated_keys(&db),
        before,
        "capture never changes original historical rows"
    );
    {
        let ordinary = db.begin_read().unwrap();
        assert_eq!(
            ordinary
                .open_table(ROWS)
                .unwrap()
                .get(b"first".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"original"
        );
        assert_eq!(
            ordinary
                .open_table(META)
                .unwrap()
                .get("state")
                .unwrap()
                .unwrap()
                .value(),
            b"old"
        );
    }
    drop(failure);
    assert_eq!(
        numerical.resources().current(),
        baseline,
        "all failed capture buffers/control refund only after original owner retirement"
    );
    drop(read);
    db.begin_write().unwrap().abort().unwrap();
    assert_eq!(
        allocated_keys(&db),
        before,
        "ordinary writer remains available after capture failure"
    );
}

// req: STO-020, REC-001, PRJ-004
fn assert_actual_fixed_profile_drift_rolls_back(drift: NativeFixedProfileDrift) {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let failure = match owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        )
        .drift_profile_before_consuming_commit(drift),
    ) {
        Err(failure) => failure,
        Ok(()) => panic!("actual changed writer profile must refuse before consuming commit"),
    };
    match failure.error() {
        NativeFixedCleanupError::RejectedFixedCommitProfile {
            error,
            materialized_bytes,
        } => {
            assert!(
                matches!(error, StorageError::Io(error)
                if error.kind() == std::io::ErrorKind::OutOfMemory),
                "actual typed profile refusal"
            );
            assert!(
                *materialized_bytes > 0,
                "two genuine point mutations precede profile refusal"
            );
        }
        _ => panic!("failure must be the actual fixed profile check"),
    }
    assert_eq!(
        allocated_keys(&db),
        before,
        "no historical cleanup or allocation emission published"
    );
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    assert_eq!(
        read.open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"old"
    );
    drop(read);
    assert!(
        numerical.resources().current() > 0,
        "original failure allowance remains retained"
    );
    drop(failure);
    db.begin_write().unwrap().abort().unwrap();
    drop(db);
    assert_eq!(
        numerical.resources().current(),
        0,
        "physical native buffers and controls retired"
    );
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cleanup_actual_quick_repair_drift_refuses_after_mutation_and_rolls_back() {
    assert_actual_fixed_profile_drift_rolls_back(NativeFixedProfileDrift::QuickRepair);
}
// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cleanup_actual_durability_drift_refuses_after_mutation_and_rolls_back() {
    assert_actual_fixed_profile_drift_rolls_back(NativeFixedProfileDrift::NonDurable);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_fixed_cleanup_actual_data_main_collision_refuses_before_writer_and_refunds() {
    let (_file, db) = historical();
    let mut seed = db.begin_write().unwrap();
    let collision_id = seed.seed_fixed_data_main_collision_for_fixture();
    seed.commit().unwrap();
    let before = allocated_keys(&db);
    // Refresh genuine ordinary profile observations after the fixture write.
    {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        let actual = read.capture_native_system_read(&epoch).unwrap();
        assert_eq!(actual.next_write_for_fixture(), collision_id);
        let mut budget = empty_reclamation_budget_for_native_test();
        actual
            .prepare_profiled_system_resources(&mut budget)
            .unwrap();
    }
    let plan = db.plan_current_read_context().unwrap();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let baseline = pool.reserved_bytes();
    let failure = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data(&db, &read, &numerical),
        metadata(&db, &numerical),
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Err(failure) => failure,
        Ok(_) => panic!("actual DATA-main collision must refuse before any writer"),
    };
    assert!(matches!(
        failure.stage_for_test(),
        NativeFixedCaptureStage::SystemPreparation
    ));
    assert_eq!(failure.error().class_for_test(), "path_unsupported_state");
    assert!(
        pool.reserved_bytes() > baseline,
        "original owning error retains capture/control allowances"
    );
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"old"
    );
    drop(failure);
    assert_eq!(pool.reserved_bytes(), baseline);
    drop(read);
    db.begin_write().unwrap().abort().unwrap();
    assert_eq!(pool.reserved_bytes(), 0);
    assert_eq!(allocated_keys(&db), before);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_fixed_cleanup_gap_combined_capture_refuses_foreign_numerical_pool() {
    let (_file, db) = historical();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let first = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let first_epoch = first.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let second = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let second_epoch = second.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db
        .begin_fixed_recipe_observation(plan, &first_epoch)
        .unwrap();
    let baseline_first = first.reserved_bytes();
    let baseline_second = second.reserved_bytes();
    let failure = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data(&db, &read, &first_epoch),
        metadata(&db, &second_epoch),
        empty_reclamation_budget_for_native_test(),
        &second_epoch,
    ) {
        Err(failure) => failure,
        Ok(_) => panic!("real combined owner must reject foreign actual pool"),
    };
    assert!(matches!(
        failure.stage_for_test(),
        NativeFixedCaptureStage::DataAffinity
    ));
    assert!(first.reserved_bytes() > baseline_first);
    assert!(second.reserved_bytes() > baseline_second);
    assert_eq!(allocated_keys(&db), before);
    drop(failure);
    assert_eq!(first.reserved_bytes(), baseline_first);
    assert_eq!(second.reserved_bytes(), baseline_second);
    drop(read);
    assert_eq!(first.reserved_bytes(), 0);
    assert_eq!(second.reserved_bytes(), 0);
    db.begin_write().unwrap().abort().unwrap();
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_fixed_cleanup_gap_combined_owner_refuses_abort_generation_with_same_roots() {
    let (_file, db) = historical();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    let original_epoch = db.capture_native_resource_epoch().unwrap();
    let original_roots = (
        db.mem.get_data_root(),
        db.mem.get_system_root(),
        db.mem.get_last_committed_transaction_id().unwrap(),
    );
    drop(read);
    owner.system.require_current().unwrap();
    // Genuine ordinary work is allowed to allocate/retire its own metadata.
    db.begin_write().unwrap().abort().unwrap();
    let current_epoch = db.capture_native_resource_epoch().unwrap();
    assert!(!original_epoch.same_generation_for_test(&current_epoch));
    assert_eq!(
        (
            db.mem.get_data_root(),
            db.mem.get_system_root(),
            db.mem.get_last_committed_transaction_id().unwrap()
        ),
        original_roots
    );
    assert!(matches!(owner.system.require_current(), Err(E::Epoch(_))));
    // All counters are sampled AFTER that ordinary abort. They measure only
    // additional work by the stale executor, not the legitimate abort itself.
    let after_abort_history = allocated_keys(&db);
    let after_abort_allocator = (
        db.mem.count_allocated_pages().unwrap(),
        db.mem.count_free_pages().unwrap(),
        db.mem.get_layout(),
    );
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let inputs = NativeFixedWriterInputs::for_fixture(
        context,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
    );
    let before_execute_charge = pool.reserved_bytes();
    let before_execute_materialized = numerical.resources().materialized_observation_for_test();
    let failure = match owner.execute(&numerical, inputs) {
        Err(failure) => failure,
        Ok(()) => panic!("actual original generation must reject before another write claim"),
    };
    assert!(matches!(
        failure.error(),
        NativeFixedCleanupError::Path(E::Epoch(_))
    ));
    assert_eq!(
        pool.reserved_bytes(),
        before_execute_charge,
        "no new recipe/write-control allowance on stale execution"
    );
    assert_eq!(
        numerical.resources().materialized_observation_for_test(),
        before_execute_materialized
    );
    assert_eq!(
        (
            db.mem.count_allocated_pages().unwrap(),
            db.mem.count_free_pages().unwrap(),
            db.mem.get_layout()
        ),
        after_abort_allocator
    );
    assert_eq!(allocated_keys(&db), after_abort_history);
    assert_eq!(
        (
            db.mem.get_data_root(),
            db.mem.get_system_root(),
            db.mem.get_last_committed_transaction_id().unwrap()
        ),
        original_roots
    );
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"old"
    );
    assert!(
        pool.reserved_bytes() > 0,
        "original allowance lives with owning failure"
    );
    drop(failure);
    assert_eq!(pool.reserved_bytes(), 0);
}

fn historical_single_allocation_row() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = db.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(b"first".as_slice(), b"original".as_slice())
        .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert("state", b"old".as_slice())
        .unwrap();
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    let savepoint = write.ephemeral_savepoint().unwrap();
    write.commit().unwrap();
    for version in [1_u8, 2] {
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for index in 0..16_u64 {
                rows.insert(index.to_be_bytes().as_slice(), [version; 8192].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
    }
    drop(savepoint);
    // Genuine ordinary observations, explicitly a component fixture. Neither
    // this warmup nor its cache capacity is attributed to Source admission.
    {
        let read = db.begin_read().unwrap();
        assert!(
            read.open_table(ROWS)
                .unwrap()
                .get(0_u64.to_be_bytes().as_slice())
                .unwrap()
                .is_some()
        );
        assert!(
            read.open_table(META)
                .unwrap()
                .get("state")
                .unwrap()
                .is_some()
        );
        let epoch = db.capture_native_resource_epoch().unwrap();
        let mut budget = empty_reclamation_budget_for_native_test();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut budget)
            .unwrap();
    }
    (file, db)
}

// Genuine savepoint-generated A0 -> A1 cap advance. This is a nearest positive
// for the EXISTING fixed executor, not a new cleanup policy or physical theorem.
#[test]
fn native_fixed_cleanup_real_allocated_cap_advance_reaches_original_horizon_postpass() {
    let (_file, db) = historical_single_allocation_row();
    let before = allocated_keys(&db);
    assert!(before.len() >= 2, "actual historical allocated rows");
    assert_ne!(
        before[0].0, before[1].0,
        "Ka1 must advance the actual transaction cap"
    );
    let (original_h, freed_before) = {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        (
            system.observed_horizon().raw_id(),
            system.fixed_cleanup_freed_keys_for_fixture(),
        )
    };
    let pre_cap = before[0].0.checked_add(1).unwrap();
    let post_cap = before[1].0.checked_add(1).unwrap();
    assert!(
        freed_before
            .iter()
            .any(|(_, id, _)| *id >= pre_cap && *id < post_cap.min(original_h)),
        "genuine old FREED row becomes post-eligible"
    );
    let eligible = freed_before
        .iter()
        .filter(|(_, id, _)| *id < original_h)
        .count();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    // Both original completion buffers are preadmitted with the SAME actual
    // eligible-row bound as the original finite budget. The old K0 helper is
    // intentionally unchanged; the native exact-equality check remains intact.
    let budget = crate::transactions::reclamation_budget_for_actual_prefix_fixture(eligible);
    let mut system_names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    system_names.sort_unstable();
    let prepared_metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &system_names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        eligible,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data(&db, &read, &numerical),
        prepared_metadata,
        budget,
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "real post-cap capture refused: stage={:?}, class={}",
            failure.stage_for_test(),
            failure.error().class_for_test()
        ),
    };
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    let trace = numerical.resources().fixed_phases_for_test();
    assert!(
        result.is_ok(),
        "actual cap-advance fixed executor failed; phases={trace:?}"
    );
    drop(result);
    assert!(
        trace.iter().all(|row| row[0] == 1),
        "both passes and REAL post publication: {trace:?}"
    );
    assert!(
        trace.iter().all(|row| row[1] == original_h && row[10] == 1),
        "original H and SAME original budget survive all phases: {trace:?}"
    );
    assert_eq!(trace[0][2], pre_cap.min(original_h));
    assert_eq!(trace[2][2], post_cap.min(original_h));
    assert_eq!(
        trace[1][3..7],
        trace[2][3..7],
        "no budget reset between passes"
    );
    assert!(
        trace[3][8] > 0,
        "actual post-only extraction removes old rows: {trace:?}"
    );
    assert!(trace[3][3] < trace[2][3], "post removal spends SAME budget");
    assert_eq!(trace[0][7], 2);
    assert_eq!(trace[1][7], 1);
    assert_eq!(trace[3][7], 0, "both original completion slots consumed");
    assert_eq!(
        allocated_keys(&db).as_slice(),
        &before[1..],
        "Ka1, ZERO new allocated emission"
    );
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"replacement"
    );
    assert_eq!(
        read.open_table(META)
            .unwrap()
            .get("state")
            .unwrap()
            .unwrap()
            .value(),
        b"new"
    );
}

// req: STO-020, REC-001, PRJ-004
// Real native quick-repair commits create the SYSTEM rows. Iterations are not
// the witness: actual eligible keys and branch depth below must establish it.
fn historical_system_prefix_for_phase_fixture() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = db.begin_write().unwrap();
    drop(
        write
            .open_table(ROWS)
            .unwrap()
            .insert(b"first".as_slice(), b"original".as_slice())
            .unwrap(),
    );
    drop(
        write
            .open_table(META)
            .unwrap()
            .insert("state", b"old".as_slice())
            .unwrap(),
    );
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    let savepoint = write.ephemeral_savepoint().unwrap();
    write.commit().unwrap();
    for version in [1u8, 2] {
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for index in 0..16u64 {
                drop(
                    rows.insert(index.to_be_bytes().as_slice(), [version; 8192].as_slice())
                        .unwrap(),
                );
            }
        }
        write.commit().unwrap();
        if version == 1 {
            for _ in 0..4 {
                let mut write = db.begin_write().unwrap();
                write.set_quick_repair(true);
                write.commit().unwrap();
            }
        }
    }
    drop(savepoint);
    {
        let read = db.begin_read().unwrap();
        drop(
            read.open_table(ROWS)
                .unwrap()
                .get(b"first".as_slice())
                .unwrap(),
        );
        drop(read.open_table(META).unwrap().get("state").unwrap());
        let epoch = db.capture_native_resource_epoch().unwrap();
        let mut budget = empty_reclamation_budget_for_native_test();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut budget)
            .unwrap();
    }
    (file, db)
}

#[test]
// req: STO-020, REC-001, PRJ-004
fn native_fixed_system_phase_real_prefix_root_collapse_append_and_physical_incarnations() {
    let (_file, db) = historical_system_prefix_for_phase_fixture();
    let allocated_before = allocated_keys(&db);
    assert!(allocated_before.len() >= 2);
    assert_ne!(allocated_before[0].0, allocated_before[1].0);
    let pre_cap = allocated_before[0].0.checked_add(1).unwrap();
    let post_cap = allocated_before[1].0.checked_add(1).unwrap();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let (horizon, freed_before, witness) = {
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        (
            system.observed_horizon().raw_id(),
            system.fixed_cleanup_freed_keys_for_fixture(),
            system.fixed_phase_witness_for_fixture(&numerical).unwrap(),
        )
    };
    let system_before: Vec<_> = freed_before.iter().filter(|row| row.0).copied().collect();
    assert!(
        system_before.len() >= 3,
        "actual padded rows require a non-leaf original SYSTEM root"
    );
    assert!(
        system_before
            .iter()
            .all(|(_, id, _)| *id >= pre_cap && *id < post_cap.min(horizon)),
        "ALL actual old SYSTEM rows become eligible only post-main"
    );
    let eligible = freed_before
        .iter()
        .filter(|(_, id, _)| *id < horizon)
        .count();
    let budget = crate::transactions::reclamation_budget_for_actual_prefix_fixture(eligible);
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        eligible,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data(&db, &read, &numerical),
        metadata,
        budget,
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "real root-collapse phase capture refused: stage={:?}, class={}",
            failure.stage_for_test(),
            failure.error().class_for_test()
        ),
    };
    let phase_namespace_bound = owner
        .system_phase
        .namespace_incarnation_bytes_for_fixture()
        .unwrap();
    let legacy_original_only = owner
        .system_phase
        .legacy_original_namespace_only_for_fixture();
    let post_freed_upper = owner.system_phase.post_freed_upper_for_fixture();
    let post_rows_upper = owner.system_phase.post_rows_upper_for_fixture();
    assert!(owner.system_phase.post_rows_upper_for_fixture() > 0);
    assert!(
        owner.system_phase.original_system_depth_for_fixture() >= 2,
        "ACTUAL original native parent/leaf depth witness, not configured row count"
    );
    assert_eq!(
        owner.system_phase.selected_system_records_for_fixture(),
        system_before.len(),
        "ALL actual original SYSTEM rows belong to the selected prefix"
    );
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        )
        .with_phase_witness(witness.clone()),
    );
    let trace = numerical.resources().fixed_phases_for_test();
    assert!(
        result.is_ok(),
        "real root-collapse/right-append executor failed; phases={trace:?}"
    );
    drop(result);
    assert!(
        trace
            .iter()
            .all(|row| row[0] == 1 && row[1] == horizon && row[10] == 1)
    );
    assert!(
        trace[3][8] > 0 && trace[3][3] < trace[2][3],
        "nonempty same-budget post extraction"
    );
    {
        let post_read = db.begin_read().unwrap();
        let post_epoch = db.capture_native_resource_epoch().unwrap();
        let post_system = post_read.capture_native_system_read(&post_epoch).unwrap();
        let copies = witness.lock().unwrap();
        let (rows, numbers, main_namespace_retired) =
            post_system.actual_post_emission_for_phase_fixture(&copies);
        assert!(
            rows > 0 && rows <= post_rows_upper && numbers <= post_freed_upper,
            "actual emitted post rows/PNs within whole closed phase debt bound"
        );
        assert_eq!(
            main_namespace_retired, 1,
            "REAL main-born committed namespace is retired by actual post mutation"
        );
        assert!(
            copies.legacy_original_namespace_misses_actual_main_for_fixture(),
            "original-only closure cannot cover that ACTUAL later committed incarnation"
        );
    }
    let before_release = numerical.resources().materialized_observation_for_test();
    let copies = witness.lock().unwrap();
    copies.require_three_distinct_backings_for_fixture();
    let physical_copies = copies.live_bytes_for_fixture();
    // Honest incomplete-plan negative: an original-only namespace quote cannot
    // fund actual coexisting original/main/post native backings. No public API
    // or preexisting campaign corruption claim is inferred from this contrast.
    assert!(
        legacy_original_only < physical_copies,
        "legacy original-only regression must be genuine"
    );
    assert!(
        physical_copies <= phase_namespace_bound,
        "whole versioned namespace component"
    );
    assert!(before_release.0 >= physical_copies && before_release.2 >= 3);
    drop(copies);
    witness.lock().unwrap().drop_copies_for_fixture();
    let after_release = numerical.resources().materialized_observation_for_test();
    assert_eq!(
        before_release.0 - after_release.0,
        physical_copies,
        "refund only final backing retirement"
    );
    assert_eq!(before_release.2 - after_release.2, 3);
    drop(witness);
    assert_eq!(allocated_keys(&db).as_slice(), &allocated_before[1..]);
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let after = system.fixed_cleanup_freed_keys_for_fixture();
    assert!(
        after.iter().all(|row| !system_before.contains(row)),
        "whole old SYSTEM prefix removed"
    );
    assert!(
        after.iter().any(|(sys, id, _)| *sys && *id >= horizon),
        "REAL post SYSTEM append after root collapse"
    );
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"replacement"
    );
}

// Actual native commit ordering: a SYS mutation is already after the existing
// irreversible adoption boundary. These failures are latch/repair, NOT abort.
// req: STO-020, REC-001, PRJ-004
fn exercise_real_fixed_system_phase_failure(
    fault: crate::transactions::NativeFixedPhaseFault,
    code: u8,
) {
    let (file, db) = historical_system_prefix_for_phase_fixture();
    let allocated_before = allocated_keys(&db);
    let durable_before = db.mem.get_last_durable_transaction_id().unwrap();
    let user_before = db.mem.get_data_root();
    let system_before = db.mem.get_system_root();
    let eligible = {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        let horizon = system.observed_horizon().raw_id();
        system
            .fixed_cleanup_freed_keys_for_fixture()
            .iter()
            .filter(|(_, id, _)| *id < horizon)
            .count()
    };
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let budget = crate::transactions::reclamation_budget_for_actual_prefix_fixture(eligible);
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        eligible,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data(&db, &read, &numerical),
        metadata,
        budget,
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "actual mixed failure capture refused: stage={:?}, class={}",
            failure.stage_for_test(),
            failure.error().class_for_test()
        ),
    };
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        )
        .with_phase_fault(fault),
    );
    assert_eq!(
        numerical.resources().fixed_phase_fault_for_test(),
        code,
        "ACTUAL selected mutation boundary reached"
    );
    let failure = result
        .err()
        .expect("genuine injected native commit failure");
    assert!(matches!(
        failure.error(),
        NativeFixedCleanupError::Commit(_)
    ));
    assert!(
        db.mem.allocator_state_invalidated(),
        "normal native failure latch retained"
    );
    assert!(
        db.begin_write().is_err(),
        "fail-closed writer admission; no fresh quota escape"
    );
    assert!(
        numerical.resources().current() > 0,
        "owning error plus actual buffers retain admission"
    );
    let main_published = code == 2;
    if main_published {
        assert!(db.mem.get_last_durable_transaction_id().unwrap() > durable_before);
        assert_ne!(db.mem.get_data_root(), user_before);
    } else {
        assert_eq!(
            db.mem.get_last_durable_transaction_id().unwrap(),
            durable_before
        );
        assert_eq!(db.mem.get_data_root(), user_before);
        assert_eq!(db.mem.get_system_root(), system_before);
    }
    {
        let read = db.begin_read().unwrap();
        let rows = read.open_table(ROWS).unwrap();
        assert_eq!(
            rows.get(b"first".as_slice()).unwrap().unwrap().value(),
            if main_published {
                b"replacement".as_slice()
            } else {
                b"original".as_slice()
            }
        );
    }
    drop(failure); // native error/records/control before actual engine destruction
    drop(db);
    assert_eq!(
        numerical.resources().materialized_observation_for_test().0,
        0,
        "all actual physical buffers retired only after native engine closes"
    );
    let repaired = crate::Builder::new()
        .set_cache_size(0)
        .open(file.path())
        .unwrap();
    {
        let read = repaired.begin_read().unwrap();
        assert_eq!(
            read.open_table(ROWS)
                .unwrap()
                .get(b"first".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            if main_published {
                b"replacement".as_slice()
            } else {
                b"original".as_slice()
            }
        );
    }
    assert_eq!(
        allocated_keys(&repaired).as_slice(),
        if main_published {
            &allocated_before[1..]
        } else {
            allocated_before.as_slice()
        },
        "repair follows ACTUAL published native root and its exact historical prefix"
    );
    repaired.begin_write().unwrap().abort().unwrap();
}
#[test]
// req: STO-020, REC-001, PRJ-004
fn native_fixed_system_phase_failure_before_main_preserves_durable_head_and_repairs() {
    exercise_real_fixed_system_phase_failure(
        crate::transactions::NativeFixedPhaseFault::BeforeMainAfterDataAppend,
        1,
    );
}
#[test]
// req: STO-020, REC-001, PRJ-004
fn native_fixed_system_phase_failure_after_main_preserves_durable_main_and_repairs() {
    exercise_real_fixed_system_phase_failure(
        crate::transactions::NativeFixedPhaseFault::AfterMainAfterPostExtraction,
        2,
    );
}

#[test]
// req: STO-020, REC-001
fn native_fixed_system_phase_exact_key_cursor_can_increase_live_leaf_count() {
    // Genuine ordinary USER-table operations exercise the SAME native cursor
    // repacking algorithm. Value lengths equal legal PageList envelope sizes;
    // contents are ordinary application bytes, never fabricated SYSTEM rows,
    // historical PN lists, Source authority or an emitter-lifecycle fixture.
    const REPACK: TableDefinition<u128, &[u8]> = TableDefinition::new("phase_repack_user");
    // The authoritative native padded-400 envelope is 3202 bytes (u16 count
    // plus400 PageNumbers). Other USER value lengths below match legal native
    // padded shapes; this test does not mint or store a SYSTEM PageList.
    assert_eq!(
        crate::transactions::PageList::allocated_capacity_bytes(),
        3202
    );
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_page_size(4096)
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(REPACK).unwrap();
        // Insert key2 AFTER key3: it is interior, so the native ascending
        // append fastpath cannot bypass the required first balanced split.
        // The first split leaves [1010,1010] and [394,1610]. Replacement and
        // point insertion then create a native oversized multi-pair middle
        // leaf [394,3202,3202] plus the rightmost [402] leaf.
        for (key, length) in [
            (0_u128, 1010_usize),
            (1, 1010),
            (3, 1610),
            (2, 394),
            (3, 3202),
            (5, 402),
            (4, 3202),
        ] {
            let value = vec![key as u8; length];
            drop(table.insert(key, value.as_slice()).unwrap());
        }
        let stats = crate::ReadableTableMetadata::stats(&table).unwrap();
        assert_eq!(
            stats.leaf_pages(),
            3,
            "actual native split fixture has three leaves"
        );
        assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 6);
    }
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(REPACK).unwrap();
        let before = crate::ReadableTableMetadata::stats(&table)
            .unwrap()
            .leaf_pages();
        assert_eq!(before, 3);
        let shapes = table
            .fixed_phase_user_leaf_shapes_for_fixture()
            .unwrap()
            .expect("actual bounded one-level native shape");
        assert_eq!((shapes[0].0, shapes[0].1), (4096, 2));
        assert_eq!(&shapes[0].2[..2], &[(0, 1010), (1, 1010)]);
        assert_eq!(
            (shapes[1].0, shapes[1].1),
            (8192, 3),
            "actual native oversized MULTI-pair middle, not just three leaves"
        );
        assert_eq!(&shapes[1].2[..3], &[(2, 394), (3, 3202), (4, 3202)]);
        assert_eq!((shapes[2].0, shapes[2].1), (4096, 1));
        assert_eq!(shapes[2].2[0], (5, 402));
        assert!(shapes[3..].iter().all(|shape| shape.1 == 0));
        {
            let mut removed = table.extract_from_if(0_u128..=0_u128, |_, _| true).unwrap();
            let (key, value) = removed.next().unwrap().unwrap();
            assert_eq!(key.value(), 0);
            assert_eq!(value.value(), &[0_u8; 1010]);
            drop((key, value));
            // Match the production fixed exact-key route: next ONCE, then
            // close by dropping the cursor before any further observation.
            drop(removed);
        }
        let after = crate::ReadableTableMetadata::stats(&table)
            .unwrap()
            .leaf_pages();
        assert_eq!(
            after, 4,
            "exact-key sparse cursor repacks two original leaves into three"
        );
        let shapes = table
            .fixed_phase_user_leaf_shapes_for_fixture()
            .unwrap()
            .expect("actual native repacked leaf contents");
        assert_eq!((shapes[0].0, shapes[0].1), (4096, 2));
        assert_eq!(&shapes[0].2[..2], &[(1, 1010), (2, 394)]);
        for (index, expected) in [(1, (3, 3202)), (2, (4, 3202)), (3, (5, 402))] {
            assert_eq!((shapes[index].0, shapes[index].1), (4096, 1));
            assert_eq!(shapes[index].2[0], expected);
        }
        assert!(shapes[4..].iter().all(|shape| shape.1 == 0));
        assert!(
            after > before,
            "deletion leaf nonincrease is NOT a valid live-geometry premise"
        );
        assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 5);
        for (key, length) in [
            (1_u128, 1010_usize),
            (2, 394),
            (3, 3202),
            (4, 3202),
            (5, 402),
        ] {
            let value = table.get(key).unwrap().unwrap();
            assert_eq!(value.value().len(), length);
            assert!(value.value().iter().all(|byte| *byte == key as u8));
        }
        assert!(table.get(0_u128).unwrap().is_none());
        assert!(
            after <= crate::ReadableTableMetadata::len(&table).unwrap(),
            "one retained pair per nonempty final leaf bounds repacking"
        );
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(REPACK).unwrap();
    assert_eq!(
        crate::ReadableTableMetadata::stats(&table)
            .unwrap()
            .leaf_pages(),
        4
    );
    assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 5);
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cursor_checksum_n_minus_one_refuses_before_writer_claim_and_same_pool_reopens() {
    let (_file, db) = historical_single_allocation_row();
    let before_roots = (
        db.mem.get_data_root(),
        db.mem.get_system_root(),
        db.mem.get_last_committed_transaction_id().unwrap(),
    );
    let before_allocated = allocated_keys(&db);
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    // Measure the actual complete captured owner on this SAME immutable fixture;
    // this is a fixture ceiling derivation, not a production resource theorem.
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db
        .begin_fixed_recipe_observation(db.plan_current_read_context().unwrap(), &numerical)
        .unwrap();
    let owner = combined(&db, &read, &numerical);
    let actual_required = pool.reserved_bytes();
    let named =
        NativeFixedExecutionBufferDemand::from_bound_owners(&owner.data, &owner.system_phase)
            .unwrap();
    assert!(named.capacity_bytes().unwrap() > 0);
    drop(owner);
    drop(read);
    drop(numerical.close().unwrap());
    assert_eq!(pool.reserved_bytes(), 0);
    let numerical = pool
        .begin_managed_epoch(actual_required.checked_sub(1).unwrap())
        .unwrap();
    let read = db
        .begin_fixed_recipe_observation(db.plan_current_read_context().unwrap(), &numerical)
        .unwrap();
    let original_data = data(&db, &read, &numerical);
    let prepared_metadata = metadata(&db, &numerical);
    let failure = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        original_data,
        prepared_metadata,
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Ok(_) => panic!("one byte below actual captured backing must refuse before writer claim"),
        Err(failure) => failure,
    };
    assert!(
        matches!(failure.error(), NativeFixedCleanupError::Path(E::Native(_))),
        "actual resource refusal class"
    );
    assert_eq!(
        before_roots,
        (
            db.mem.get_data_root(),
            db.mem.get_system_root(),
            db.mem.get_last_committed_transaction_id().unwrap()
        )
    );
    assert_eq!(allocated_keys(&db), before_allocated);
    assert!(
        pool.reserved_bytes() > 0,
        "original failure owner retains actual prepared backing"
    );
    drop(failure);
    drop(read);
    drop(numerical.close().unwrap());
    assert_eq!(
        pool.reserved_bytes(),
        0,
        "partial backing physically retires before numerical refund"
    );
    // Genuine ordinary availability after refusal, under its original None
    // path; abort legitimately advances native generation, so recapture below.
    db.begin_write().unwrap().abort().unwrap();
    assert_eq!(allocated_keys(&db), before_allocated);
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    assert!(
        result.is_ok(),
        "same original numerical pool with full real backing runs actual fixed executor"
    );
    drop(result);
    assert_eq!(allocated_keys(&db).as_slice(), &before_allocated[1..]);
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"replacement"
    );
    drop(db);
    assert_eq!(
        pool.reserved_bytes(),
        0,
        "all native aliases and workspace backing retire on real engine teardown"
    );
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_cursor_checksum_real_point_error_retains_buffers_until_failure_drops() {
    let (_file, db) = historical_single_allocation_row();
    let before = allocated_keys(&db);
    let before_roots = (
        db.mem.get_data_root(),
        db.mem.get_system_root(),
        db.mem.get_last_committed_transaction_id().unwrap(),
    );
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let owner = combined(&db, &read, &numerical);
    drop(read);
    let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
    let failure = match owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            context,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        )
        .fail_after_first_real_point(),
    ) {
        Ok(_) => panic!("deterministic failure after the first actual DATA mutation"),
        Err(failure) => failure,
    };
    assert!(
        matches!(failure.error(), NativeFixedCleanupError::InjectedAfterFirstPoint { materialized_bytes } if *materialized_bytes > 0)
    );
    assert_eq!(
        before_roots,
        (
            db.mem.get_data_root(),
            db.mem.get_system_root(),
            db.mem.get_last_committed_transaction_id().unwrap()
        )
    );
    assert_eq!(allocated_keys(&db), before);
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original"
    );
    assert!(
        pool.reserved_bytes() > 0,
        "owning failure retains actual preconstructed workspace and native error"
    );
    drop(failure);
    drop(db);
    assert_eq!(pool.reserved_bytes(), 0);
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_branch_children_real_executor_keeps_actual_child_backing_guarded() {
    let (_file, db) = historical_system_prefix_for_phase_fixture();
    let before = allocated_keys(&db);
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let eligible = {
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        let horizon = system.observed_horizon().raw_id();
        system
            .fixed_cleanup_freed_keys_for_fixture()
            .iter()
            .filter(|(_, id, _)| *id < horizon)
            .count()
    };
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[META.name(), ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        eligible,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        data(&db, &read, &numerical),
        metadata,
        crate::transactions::reclamation_budget_for_actual_prefix_fixture(eligible),
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "branch ownership capture refused: {:?}",
            failure.stage_for_test()
        ),
    };
    assert!(owner.system_phase.original_system_depth_for_fixture() >= 2);
    drop(read);
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap(),
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    assert!(
        result.is_ok(),
        "real branch ownership execution must complete"
    );
    drop(result);
    let observed = numerical.resources().branch_children_observation_for_test();
    assert!(
        observed[0] > 0,
        "actual fixed executor must construct a branch builder"
    );
    assert_eq!(
        observed[1], 0,
        "actual fixed BranchBuilder child backing must be guarded"
    );
    assert_eq!(
        observed[3], 0,
        "every actual PartialBranch transfer must keep its same guard"
    );
    // No assertion claims this small fixture necessarily takes PartialBranch.
    assert_eq!(allocated_keys(&db).as_slice(), &before[1..]);
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(ROWS)
            .unwrap()
            .get(b"first".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"replacement"
    );
    drop(read);
    drop(db);
    assert_eq!(
        pool.reserved_bytes(),
        0,
        "actual buffers retire before final numerical controls"
    );
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_branch_children_real_delete_unwind_transfers_partial_branch_owner() {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    // Genuine ordinary USER writes. A common511-byte prefix keeps the native
    // variable-width separator long enough for a small branch fanout.
    let keys: Vec<_> = (0u8..96)
        .map(|index| {
            let mut key = [b'x'; 512];
            key[511] = index;
            key
        })
        .collect();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for key in &keys {
            drop(table.insert(key.as_slice(), b"value".as_slice()).unwrap());
        }
    }
    write.commit().unwrap();
    // Ordinary component-fixture inspection, not a Source cold-admission claim.
    {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut empty_reclamation_budget_for_native_test())
            .unwrap();
    }
    let before = allocated_keys(&db);
    // Native branch split creates a LEFT5-child branch, not4. Twelve
    // deletes can leave4 children (1652B >4096/3); this genuine prefix
    // continues through the3-child merge (1112B <4096/3) before stopping.
    let points: Vec<_> = keys[..24]
        .iter()
        .map(|key| NativeFixedPoint {
            table: ROWS.name(),
            mutation: NativePointMutation::Delete {
                key: NativePointKey::Bytes(key.as_slice()),
            },
        })
        .collect();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let recipe = NativeOwnedFixedRecipe::prepare(&points, numerical.resources()).unwrap();
    let census = match NativeFixedDataCensus::capture(
        &read,
        recipe,
        db.capture_native_resource_epoch().unwrap(),
        &numerical,
    ) {
        Ok(census) => census,
        Err(_) => panic!("real delete frontier must capture"),
    };
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        census,
        metadata,
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "real delete owner capture refused: {:?}",
            failure.stage_for_test()
        ),
    };
    drop(read);
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap(),
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    assert!(result.is_ok(), "real fixed delete unwind must complete");
    drop(result);
    let observed = numerical.resources().branch_children_observation_for_test();
    assert!(
        observed[0] > 0 && observed[2] > 0,
        "actual native delete must construct AND transfer PartialBranch; observed={observed:?}"
    );
    assert_eq!(
        observed[1], 0,
        "all actual branch child builders own prepared backing"
    );
    assert_eq!(
        observed[3], 0,
        "actual transferred child backing keeps its guard"
    );
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 72);
    for key in &keys[..24] {
        assert!(table.get(key.as_slice()).unwrap().is_none());
    }
    for key in &keys[24..] {
        assert_eq!(
            table.get(key.as_slice()).unwrap().unwrap().value(),
            b"value"
        );
    }
    drop(table);
    drop(read);
    let after = allocated_keys(&db);
    assert!(
        after == before || after.as_slice() == &before[before.len().min(1)..],
        "only existing Ka1 prefix may retire; no new allocated row"
    );
    drop(db);
    assert_eq!(pool.reserved_bytes(), 0);
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_branch_keys_real_delete_unwind_keeps_outer_and_nested_backing_guarded() {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    // Genuine ordinary USER writes. A common511-byte prefix keeps the native
    // variable-width separator long enough for a small branch fanout.
    let keys: Vec<_> = (0u8..96)
        .map(|index| {
            let mut key = [b'x'; 512];
            key[511] = index;
            key
        })
        .collect();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for key in &keys {
            drop(table.insert(key.as_slice(), b"value".as_slice()).unwrap());
        }
    }
    write.commit().unwrap();
    // Ordinary component-fixture inspection, not a Source cold-admission claim.
    {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut empty_reclamation_budget_for_native_test())
            .unwrap();
    }
    let before = allocated_keys(&db);
    // Native branch split creates a LEFT5-child branch, not4. Twelve
    // deletes can leave4 children (1652B >4096/3); this genuine prefix
    // continues through the3-child merge (1112B <4096/3) before stopping.
    let points: Vec<_> = keys[..24]
        .iter()
        .map(|key| NativeFixedPoint {
            table: ROWS.name(),
            mutation: NativePointMutation::Delete {
                key: NativePointKey::Bytes(key.as_slice()),
            },
        })
        .collect();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let recipe = NativeOwnedFixedRecipe::prepare(&points, numerical.resources()).unwrap();
    let census = match NativeFixedDataCensus::capture(
        &read,
        recipe,
        db.capture_native_resource_epoch().unwrap(),
        &numerical,
    ) {
        Ok(census) => census,
        Err(_) => panic!("real delete frontier must capture"),
    };
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        census,
        metadata,
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "real delete owner capture refused: {:?}",
            failure.stage_for_test()
        ),
    };
    drop(read);
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap(),
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    assert!(result.is_ok(), "real fixed delete unwind must complete");
    drop(result);
    let observed = numerical.resources().branch_children_observation_for_test();
    let key_observed = numerical.resources().branch_key_observation_for_test();
    assert!(
        observed[0] > 0 && observed[2] > 0,
        "actual native delete must construct AND transfer PartialBranch; observed={observed:?}"
    );
    assert_eq!(
        observed[1], 0,
        "all actual branch child builders own prepared backing"
    );
    assert_eq!(
        observed[3], 0,
        "actual transferred child backing keeps its guard"
    );
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 72);
    for key in &keys[..24] {
        assert!(table.get(key.as_slice()).unwrap().is_none());
    }
    for key in &keys[24..] {
        assert_eq!(
            table.get(key.as_slice()).unwrap().unwrap().value(),
            b"value"
        );
    }
    drop(table);
    drop(read);
    let after = allocated_keys(&db);
    assert!(
        after == before || after.as_slice() == &before[before.len().min(1)..],
        "only existing Ka1 prefix may retire; no new allocated row"
    );
    drop(db);
    assert_eq!(pool.reserved_bytes(), 0);
    assert!(
        key_observed[0] > 0 && key_observed[2] > 0,
        "actual delete must construct key backing AND own nested PartialBranch keys; observed={key_observed:?}"
    );
    assert_eq!(
        key_observed[1], 0,
        "actual fixed BranchBuilder key backing must be guarded; observed={key_observed:?}"
    );
    assert_eq!(
        key_observed[3], 0,
        "actual PartialBranch nested key backing must keep its guard"
    );
}

// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_split_separator_real_put_only_keeps_owned_separator_guarded() {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    // Genuine ordinary USER writes. A common511-byte prefix keeps the native
    // variable-width separator long enough for a small branch fanout.
    let keys: Vec<_> = (0u8..99)
        .map(|index| {
            let mut key = [b'x'; 512];
            key[511] = index;
            key
        })
        .collect();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for key in &keys[..96] {
            drop(table.insert(key.as_slice(), b"value".as_slice()).unwrap());
        }
    }
    write.commit().unwrap();
    // Ordinary component-fixture inspection, not a Source cold-admission claim.
    {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut empty_reclamation_budget_for_native_test())
            .unwrap();
    }
    let before = allocated_keys(&db);
    // Each packed native leaf holds seven of these real 512-byte keys.
    // Ninety-six rows leave five pairs at the right end. Three ascending puts
    // fill that leaf and require the native rightmost new-leaf separator.
    {
        let read = db.begin_read().unwrap();
        let table = read.open_table(ROWS).unwrap();
        assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 96);
        for key in &keys[96..] {
            assert!(table.get(key.as_slice()).unwrap().is_none());
        }
    }
    let points: Vec<_> = keys[96..]
        .iter()
        .map(|key| NativeFixedPoint {
            table: ROWS.name(),
            mutation: NativePointMutation::Put {
                key: NativePointKey::Bytes(key.as_slice()),
                value: b"value".as_slice(),
            },
        })
        .collect();
    let plan = db.plan_current_read_context().unwrap();
    let limits = plan.fixed_cleanup_limits_for_fixture();
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    let recipe = NativeOwnedFixedRecipe::prepare(&points, numerical.resources()).unwrap();
    let census = match NativeFixedDataCensus::capture(
        &read,
        recipe,
        db.capture_native_resource_epoch().unwrap(),
        &numerical,
    ) {
        Ok(census) => census,
        Err(_) => panic!("real PutOnly frontier must capture"),
    };
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap();
    let owner = match NativePreparedFixedCleanup::capture(
        &db,
        &read,
        census,
        metadata,
        empty_reclamation_budget_for_native_test(),
        &numerical,
    ) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "real PutOnly owner capture refused: {:?}",
            failure.stage_for_test()
        ),
    };
    drop(read);
    let result = owner.execute(
        &numerical,
        NativeFixedWriterInputs::for_fixture(
            NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap(),
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
            FIXTURE_REQUESTS,
        ),
    );
    assert!(result.is_ok(), "real fixed PutOnly insertion must complete");
    drop(result);
    let observed = numerical.resources().branch_children_observation_for_test();
    let key_observed = numerical.resources().branch_key_observation_for_test();
    assert!(
        observed[0] > 0,
        "actual PutOnly must construct a native branch"
    );
    assert_eq!(
        observed[1], 0,
        "all actual branch child builders own prepared backing"
    );
    assert_eq!(
        observed[3], 0,
        "actual transferred child backing keeps its guard"
    );
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 99);
    for key in &keys {
        assert_eq!(
            table.get(key.as_slice()).unwrap().unwrap().value(),
            b"value"
        );
    }
    drop(table);
    drop(read);
    let after = allocated_keys(&db);
    assert!(
        after == before || after.as_slice() == &before[before.len().min(1)..],
        "only existing Ka1 prefix may retire; no new allocated row"
    );
    drop(db);
    assert_eq!(pool.reserved_bytes(), 0);
    assert!(
        key_observed[4] > 0,
        "actual PutOnly must produce an owned native split separator; observed={key_observed:?}"
    );
    assert_eq!(
        key_observed[5], 0,
        "actual fixed split separator backing must be guarded; observed={key_observed:?}"
    );
}

fn actual_branch_key_refusal_fixture() -> (tempfile::NamedTempFile, Database, Vec<[u8; 512]>) {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let keys: Vec<_> = (0u8..96)
        .map(|index| {
            let mut key = [b'x'; 512];
            key[511] = index;
            key
        })
        .collect();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for key in &keys {
            drop(table.insert(key.as_slice(), b"value".as_slice()).unwrap());
        }
    }
    write.commit().unwrap();
    {
        let read = db.begin_read().unwrap();
        let epoch = db.capture_native_resource_epoch().unwrap();
        read.capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut empty_reclamation_budget_for_native_test())
            .unwrap();
    }
    (file, db, keys)
}
fn actual_branch_key_refusal_owner<'work>(
    db: &'work Database,
    read: &ReadTransaction,
    numerical: &NativeNumericalEpoch,
    keys: &[[u8; 512]],
) -> core::result::Result<NativePreparedFixedCleanup<'work>, NativeFixedCleanupFailure<'work>> {
    let points: Vec<_> = keys[..24]
        .iter()
        .map(|key| NativeFixedPoint {
            table: ROWS.name(),
            mutation: NativePointMutation::Delete {
                key: NativePointKey::Bytes(key.as_slice()),
            },
        })
        .collect();
    let recipe = NativeOwnedFixedRecipe::prepare(&points, numerical.resources()).unwrap();
    let data = match NativeFixedDataCensus::capture(
        read,
        recipe,
        db.capture_native_resource_epoch().unwrap(),
        numerical,
    ) {
        Ok(data) => data,
        Err(_) => panic!("actual branch-key DATA frontier must capture"),
    };
    let mut names = NativeSystemFamily::all().map(NativeSystemFamily::name);
    names.sort_unstable();
    let metadata = NativePreparedTxnMetadata::prepare(
        &db.mem,
        numerical.resources(),
        &[ROWS.name()],
        &names,
        FIXTURE_REQUESTS,
        FIXTURE_REQUESTS,
        0,
    )
    .unwrap();
    NativePreparedFixedCleanup::capture(
        db,
        read,
        data,
        metadata,
        empty_reclamation_budget_for_native_test(),
        numerical,
    )
}
// req: STO-020, REC-001, PRJ-004
#[test]
fn native_fixed_branch_key_actual_n_minus_one_refuses_before_writer_claim_with_original_owners() {
    let (_file, db, keys) = actual_branch_key_refusal_fixture();
    let before_roots = (
        db.mem.get_data_root(),
        db.mem.get_system_root(),
        db.mem.get_last_committed_transaction_id().unwrap(),
    );
    let before_allocated = allocated_keys(&db);
    let pool = NativeNumericalPool::from_retained_data(FIXTURE_CEILING, ()).unwrap();
    let numerical = pool.begin_managed_epoch(FIXTURE_CEILING).unwrap();
    let read = db
        .begin_fixed_recipe_observation(db.plan_current_read_context().unwrap(), &numerical)
        .unwrap();
    let owner = match actual_branch_key_refusal_owner(&db, &read, &numerical, &keys) {
        Ok(owner) => owner,
        Err(failure) => panic!(
            "full actual key-owner fixture refused: {:?}",
            failure.stage_for_test()
        ),
    };
    let demand =
        NativeFixedExecutionBufferDemand::from_bound_owners(&owner.data, &owner.system_phase)
            .unwrap();
    let (c, k, m, added) = demand.branch_key_quote_for_fixture().unwrap();
    assert_eq!(k, c - 1);
    assert_eq!(
        m, 512,
        "actual original USER key bytes dominate this fixture maximum"
    );
    assert!(k > 0 && added > 0);
    assert_eq!(
        added,
        crate::tree_store::NativeBranchKeyPool::capacity_bytes(k, m).unwrap()
    );
    assert!(
        demand.capacity_bytes().unwrap() >= added,
        "whole named backing quote contains all five typed key/separator headers"
    );
    let actual_required = pool.reserved_bytes();
    assert!(actual_required > added);
    drop(owner);
    drop(read);
    drop(numerical.close().unwrap());
    assert_eq!(pool.reserved_bytes(), 0);
    let numerical = pool
        .begin_managed_epoch(actual_required.checked_sub(1).unwrap())
        .unwrap();
    let read = db
        .begin_fixed_recipe_observation(db.plan_current_read_context().unwrap(), &numerical)
        .unwrap();
    let failure = match actual_branch_key_refusal_owner(&db, &read, &numerical, &keys) {
        Ok(_) => panic!("n-1 actual prepared key backing must refuse before writer claim"),
        Err(failure) => failure,
    };
    assert!(
        matches!(failure.error(), NativeFixedCleanupError::Path(E::Native(_))),
        "real numerical admission refusal, stage={:?}",
        failure.stage_for_test()
    );
    assert_eq!(
        before_roots,
        (
            db.mem.get_data_root(),
            db.mem.get_system_root(),
            db.mem.get_last_committed_transaction_id().unwrap()
        )
    );
    assert_eq!(allocated_keys(&db), before_allocated);
    assert!(
        pool.reserved_bytes() > 0,
        "original recipe/error/metadata owners retain allowance"
    );
    drop(failure);
    drop(read);
    drop(numerical.close().unwrap());
    assert_eq!(
        pool.reserved_bytes(),
        0,
        "all actually constructed ranges/bytes/headers retire before refund"
    );
    // Actual ordinary availability after refusal; abort may legitimately advance
    // generation, so compare the Source refusal before this ordinary operation.
    db.begin_write().unwrap().abort().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(crate::ReadableTableMetadata::len(&table).unwrap(), 96);
    for key in &keys {
        assert_eq!(
            table.get(key.as_slice()).unwrap().unwrap().value(),
            b"value"
        );
    }
    drop(table);
    drop(read);
    assert_eq!(allocated_keys(&db), before_allocated);
    drop(db);
    assert_eq!(pool.reserved_bytes(), 0);
}
