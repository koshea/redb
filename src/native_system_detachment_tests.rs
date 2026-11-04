//! Genuine native component ownership tests. Numerical unit marker only;
//! no actual SourceReservation, complete write quote or cold factory claim.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::transaction_tracker::{NativeResourceEpoch, NativeResourcePlanningRefusal};
use crate::tree_store::{NativeKeyPathError as E, NativeNumericalEpoch, NativeOwnedTxnIds};
use crate::{
    Database, NativeNumericalPool, ReadTransaction, ReadableDatabase, ReadableTable, StorageError,
    TableDefinition,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("system_detachment_rows");
const SELECTED: usize = 3;
struct NumericalMarker(Arc<AtomicUsize>);
impl Drop for NumericalMarker {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn fixture() -> (tempfile::NamedTempFile, Database) {
    // Real native typed PageList records from the existing bounded fixture.
    // Empty padded lists remain real records; they are not Source history.
    let (file, db) = crate::transactions::native_prepared_reclamation_test_support::seeded(false);
    let mut write = db.begin_write().unwrap();
    write.set_finite_reclamation_for_test(0);
    write.set_quick_repair(true);
    write
        .open_table(ROWS)
        .unwrap()
        .insert(b"key".as_slice(), b"value".as_slice())
        .unwrap();
    write.commit().unwrap();
    // This component uses actual ordinary observations for sizing. It does
    // not claim a cold Source capture or charge these baseline allocations.
    {
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(ROWS)
                .unwrap()
                .get(b"key".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"value"
        );
        let epoch = db.capture_native_resource_epoch().unwrap();
        let mut budget = ReclamationBudget::for_emitted_entries(0);
        let observed = read
            .capture_native_system_read(&epoch)
            .unwrap()
            .prepare_profiled_system_resources(&mut budget)
            .unwrap();
        assert!(observed.system_resources().allocator_state_present());
    }
    (file, db)
}
fn observation(
    db: &Database,
) -> (
    ReadTransaction,
    NativeNumericalPool,
    NativeNumericalEpoch,
    usize,
    Arc<AtomicUsize>,
) {
    let plan = db.plan_current_read_context().unwrap();
    // Finite fixture ceiling from the genuine read plan plus actual detached
    // controls. Not asserted to be a complete SYSTEM/native peak theorem.
    let ceiling = usize::try_from(plan.peak_bytes())
        .unwrap()
        .checked_add(NativeDetachedProfiledSystemResources::capture_control_bytes().unwrap())
        .unwrap();
    let retired = Arc::new(AtomicUsize::new(0));
    let pool =
        NativeNumericalPool::from_retained_data(ceiling, NumericalMarker(retired.clone())).unwrap();
    let numerical = pool.begin_managed_epoch(ceiling).unwrap();
    let read = db.begin_fixed_recipe_observation(plan, &numerical).unwrap();
    (read, pool, numerical, ceiling, retired)
}
fn prepare<'read, 'epoch, 'work>(
    read: &'read ReadTransaction,
    epoch: &'epoch NativeResourceEpoch<'work>,
    numerical: &NativeNumericalEpoch,
) -> NativePreparedProfiledSystemResources<'read, 'epoch, 'work> {
    let mut budget = ReclamationBudget::for_emitted_entries(SELECTED);
    // Both completed-pass buffers are real SAME-pool Source-owned vectors.
    let buffers = [
        NativeOwnedTxnIds::prepare_source(SELECTED, numerical.resources()).unwrap(),
        NativeOwnedTxnIds::prepare_source(SELECTED, numerical.resources()).unwrap(),
    ];
    budget
        .install_completed_storage(buffers, numerical.resources())
        .unwrap();
    read.capture_native_system_read(epoch)
        .unwrap()
        .prepare_profiled_system_with_freed_frontiers(&mut budget)
        .unwrap()
}
fn detach<'read, 'epoch, 'work>(
    prepared: NativePreparedProfiledSystemResources<'read, 'epoch, 'work>,
    db: &'work Database,
    epoch: &NativeResourceEpoch<'work>,
) -> NativeDetachedProfiledSystemResources<'work> {
    match prepared.into_detached(db, epoch) {
        Ok(owner) => owner,
        Err(failure) => panic!("unexpected component refusal: {:?}", failure.error()),
    }
}
fn oom(error: &E) -> bool {
    matches!(error, E::Native(StorageError::Io(error))
        if error.kind() == std::io::ErrorKind::OutOfMemory
            && error.get_ref().is_none() && error.raw_os_error().is_none())
}

#[test]
fn native_system_detachment_keeps_both_families_and_fence_after_original_read_drop() {
    let (_file, db) = fixture();
    let external = db.begin_read().unwrap();
    let (read, pool, numerical, _, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let prepared = prepare(&read, &epoch, &numerical);
    let before = prepared.system_resources().detachment_state_for_test();
    assert!(before.0.is_some());
    assert_eq!(before.2, 2);
    assert!(before.3, "typed frontiers must actually be retained");
    assert!(
        prepared
            .system_resources()
            .selected_prefix()
            .unwrap()
            .data()
            .records()
            > 0
    );
    assert!(
        prepared
            .system_resources()
            .selected_prefix()
            .unwrap()
            .system()
            .records()
            > 0
    );
    let detached = detach(prepared, &db, &epoch);
    drop(read);
    drop(epoch); // Detached seal borrows actual tracker, never this local value.
    detached.require_current().unwrap();
    assert_eq!(detached.system.detachment_state_for_test(), before);
    // A second genuine capture proves the inline planned slot was released.
    let replacement = db
        .begin_fixed_recipe_observation(db.plan_current_read_context().unwrap(), &numerical)
        .unwrap();
    drop(replacement);
    drop(external); // No read floor remains. Preserve the original fence anyway.
    detached.require_current().unwrap();
    assert_eq!(detached.system.detachment_state_for_test(), before);
    assert!(
        pool.reserved_bytes()
            >= NativeDetachedProfiledSystemResources::capture_control_bytes().unwrap()
    );
    drop(numerical);
    drop(pool);
    assert_eq!(
        retired.load(Ordering::SeqCst),
        0,
        "moved guards must retain the SAME numerical T"
    );
    drop(detached);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}

#[test]
fn native_system_detachment_refuses_foreign_database_and_distinct_epoch_instance() {
    let (_file, db) = fixture();
    let (_foreign_file, foreign) = fixture();
    let (read, pool, numerical, _, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let other_epoch = db.capture_native_resource_epoch().unwrap();
    for (actual, original) in [(&foreign, &epoch), (&db, &other_epoch)] {
        let failure = match prepare(&read, &epoch, &numerical).into_detached(actual, original) {
            Ok(_) => panic!("foreign owner accepted"),
            Err(failure) => failure,
        };
        assert!(matches!(failure.error(), E::ForeignOwner));
        assert!(
            failure
                ._control
                .as_ref()
                .unwrap()
                .belongs_to(pool.resources())
        );
        drop(failure);
        epoch.require_current().unwrap();
    }
    drop(read);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(pool);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}

#[test]
fn native_system_detachment_refuses_real_aborted_writer_generation_change() {
    let (_file, db) = fixture();
    let (read, pool, numerical, _, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let detached = detach(prepare(&read, &epoch, &numerical), &db, &epoch);
    drop(read);
    drop(epoch);
    db.begin_write().unwrap().abort().unwrap();
    assert!(matches!(
        detached.require_current(),
        Err(E::Epoch(NativeResourcePlanningRefusal::Stale))
    ));
    assert!(pool.reserved_bytes() > 0);
    drop(detached);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(numerical);
    drop(pool);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}

#[test]
fn native_system_detachment_pressure_before_control_is_value_free_and_preserves_custody() {
    let (_file, db) = fixture();
    let (read, pool, numerical, ceiling, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let prepared = prepare(&read, &epoch, &numerical);
    // Actual fixed Source read resolver, not an independently paired pool.
    let resolver = prepared.system.reclamation_resolver();
    let pressure = resolver
        .admit_fixed_observation(ceiling - pool.reserved_bytes())
        .unwrap();
    let failure = match prepared.into_detached(&db, &epoch) {
        Ok(_) => panic!("zero remaining allowance accepted"),
        Err(failure) => failure,
    };
    assert!(failure._control.is_none());
    assert!(
        oom(failure.error()),
        "pre-admission errors must have no owned payload"
    );
    drop(resolver);
    drop(read);
    drop(numerical);
    drop(pool);
    assert_eq!(retired.load(Ordering::SeqCst), 0);
    drop(failure); // Value-free error cannot release some unrelated allowance.
    assert_eq!(retired.load(Ordering::SeqCst), 0);
    drop(pressure);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}

#[test]
fn native_system_detachment_post_admission_error_retains_control_until_error_drop() {
    let (_file, db) = fixture();
    let (_foreign_file, foreign) = fixture();
    let (read, pool, numerical, _, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let failure = match prepare(&read, &epoch, &numerical).into_detached(&foreign, &epoch) {
        Ok(_) => panic!("foreign memory accepted"),
        Err(failure) => failure,
    };
    assert!(matches!(failure.error(), E::ForeignOwner));
    assert!(
        failure
            ._control
            .as_ref()
            .unwrap()
            .belongs_to(pool.resources())
    );
    drop(read);
    assert_eq!(
        pool.reserved_bytes(),
        NativeDetachedProfiledSystemResources::capture_control_bytes().unwrap()
    );
    drop(numerical);
    drop(pool);
    assert_eq!(
        retired.load(Ordering::SeqCst),
        0,
        "error must keep its own numerical owner"
    );
    assert!(matches!(failure.error(), E::ForeignOwner));
    drop(failure);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}

#[test]
fn native_system_detachment_old_context_refuses_after_same_pool_serial_reopens() {
    let (_file, db) = fixture();
    let (read, pool, numerical, ceiling, retired) = observation(&db);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let detached = detach(prepare(&read, &epoch, &numerical), &db, &epoch);
    drop(read);
    let closed = numerical.close().unwrap();
    assert!(oom(&detached.require_current().unwrap_err()));
    let reopened = pool.begin_managed_epoch(ceiling).unwrap();
    reopened.require_open().unwrap();
    epoch.require_current().unwrap(); // Native root/generation did not change.
    assert!(
        oom(&detached.require_current().unwrap_err()),
        "old serial must not revive on the same Counter"
    );
    assert!(pool.reserved_bytes() > 0);
    drop(detached);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(closed);
    drop(reopened);
    drop(pool);
    assert_eq!(retired.load(Ordering::SeqCst), 1);
}
