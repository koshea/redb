//! Actual native ownership/retirement controls, no Source or write-plan authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::transaction_tracker::NativeResourcePlanningRefusal;
use crate::tree_store::NativeKeyPathError;
use crate::{Database, ReadableDatabase, TableDefinition};
const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("profiled_system_rows");

#[test]
fn native_profiled_system_preparation_releases_pages_and_keeps_complete_native_facts() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    for version in 0..4_u8 {
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for key in 0..16 {
                rows.insert(key, [version; 8192].as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
    }
    drop(db); // Genuine normal close records the native allocator snapshot.
    let db = Database::open(file.path()).unwrap();
    let planned = db
        .begin_planned_read_context(db.plan_current_read_context().unwrap())
        .unwrap();
    let user_charge = planned.context_current_for_test();
    let actual = planned.actual_read_for_system_test();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    #[cfg(debug_assertions)]
    let root = actual.mem.get_system_root().unwrap().root;
    #[cfg(debug_assertions)]
    assert!(
        actual
            .tree
            .read_page_resolver()
            .copied_page_references_for_test(root)
            > 0
    );
    #[cfg(debug_assertions)]
    assert!(!actual.mem.native_page_has_read_alias_for_test(root));
    let mut budget = ReclamationBudget::for_emitted_entries(0);
    let prepared = system
        .prepare_profiled_system_resources(&mut budget)
        .unwrap();
    assert!(prepared.system_resources().allocator_state_present());
    assert!(prepared.system_resources().purge_facts().rows > 0);
    let namespace = prepared.namespace_facts();
    assert!(namespace.actual_definitions() > 0 && namespace.actual_definitions() <= 6);
    assert!(namespace.actual_page_bytes() >= namespace.base_page_bytes());
    assert!(namespace.prospective_union_bytes() <= namespace.base_page_bytes());
    assert_eq!(namespace.base_page_bytes(), 4096);
    assert_eq!(prepared.system_resources().selected_records(), 0);
    assert_eq!(
        planned.context_current_for_test(),
        user_charge,
        "SYS graphs escaped consuming preparation"
    );
    #[cfg(debug_assertions)]
    assert!(!actual.mem.native_page_has_read_alias_for_test(root));
    #[cfg(debug_assertions)]
    assert_eq!(
        actual
            .tree
            .read_page_resolver()
            .copied_page_references_for_test(root),
        0
    );
    prepared.require_before_write().unwrap();
    epoch.require_current().unwrap();
}

#[test]
fn native_profiled_system_preparation_shares_retirement_budget_and_refuses_after_writer_wins() {
    use crate::transactions::native_prepared_reclamation_test_support as support;
    let (_file, db, old) = support::backlog_from_empty_reader();
    let mut write = db.begin_write().unwrap();
    write.set_finite_reclamation_for_test(0);
    write.set_quick_repair(true);
    write.commit().unwrap();
    drop(old);
    // Plain genuine pin: this proves numerical owner/retirement custody only.
    // Cold planned-context refusal is an explicit separate blocker below.
    let actual = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    let prepared = system
        .prepare_profiled_system_resources(&mut budget)
        .unwrap();
    assert!(prepared.system_resources().allocator_state_present());
    assert_eq!(
        prepared.system_resources().selected_records(),
        1,
        "the SAME budget selects one actual eligible whole entry"
    );
    assert!(prepared.namespace_facts().actual_definitions() > 0);
    prepared.require_before_write().unwrap();
    // Deterministic writer-wins schedule AFTER the consuming SYS release.
    let write = db.begin_write().unwrap();
    write
        .open_table(support::ROWS)
        .unwrap()
        .insert(0, [9_u8; 8192].as_slice())
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        prepared.require_before_write(),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    assert_eq!(
        actual
            .open_table(support::ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        6
    );
}

#[test]
fn native_profiled_system_refusal_releases_inspection_and_preserves_generic_small_page_support() {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_page_size(512)
        .create(file.path())
        .unwrap();
    // Genuine ordinary512 native pin, as the already-qualified namespace
    // negative. Empty state has no observed definition/leaf, so Current plan
    // is intentionally incomplete and is not this numerical owner test.
    let actual = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(0);
    let refused = system.prepare_profiled_system_resources(&mut budget);
    assert!(matches!(refused, Err(NativeKeyPathError::UnsupportedState)));
    epoch.require_current().unwrap();
    // The existing generic combined owner still supports this genuine512
    // native state; the narrow namespace profile is an additive prerequisite.
    let system = actual.capture_native_system_read(&epoch).unwrap();
    let generic = system.prepare_system_resources(&mut budget).unwrap();
    generic.require_before_write().unwrap();
    assert_eq!(generic.selected_records(), 0);
    drop(generic);
    db.begin_write().unwrap().abort().unwrap();
}

#[test]
fn native_profiled_system_cold_current_copies_have_own_metadata_without_global_growth() {
    use crate::transactions::native_prepared_reclamation_test_support as support;
    let (_file, db, old) = support::backlog_from_empty_reader();
    let mut write = db.begin_write().unwrap();
    write.set_finite_reclamation_for_test(0);
    write.set_quick_repair(true);
    write.commit().unwrap();
    drop(old);
    // No scan/warmup and no larger page shape. The exact1947 fixture previously
    // failed shared-debug-map growth at len3/cap3 (retained RED capsule).
    let planned = db
        .begin_planned_read_context(db.plan_current_read_context().unwrap())
        .unwrap();
    let actual = planned.actual_read_for_system_test();
    #[cfg(debug_assertions)]
    let primary_before = actual.mem.debug_read_capacity_for_test();
    let user_charge = planned.context_current_for_test();
    let total_charge = actual.tree.read_page_resolver().context_total_for_test();
    assert!(
        total_charge > user_charge,
        "metadata must be charged in the actual same counter"
    );
    let limits = actual
        .tree
        .read_page_resolver()
        .context_limits_for_test()
        .unwrap();
    assert_eq!((limits.0, limits.1, limits.2), (4096, 16384, 113));
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    let result = system.prepare_profiled_system_resources(&mut budget);
    if let Err(error) = &result {
        println!(
            "next exact native cold refusal {:?} diagnostic={:?}",
            error,
            actual.tree.read_page_resolver().context_refusal_for_test()
        );
    }
    let prepared = result.unwrap();
    assert!(prepared.system_resources().allocator_state_present());
    assert_eq!(prepared.system_resources().selected_records(), 1);
    assert_eq!(planned.context_current_for_test(), user_charge);
    assert_eq!(
        actual.tree.read_page_resolver().context_total_for_test(),
        total_charge
    );
    #[cfg(debug_assertions)]
    assert_eq!(
        actual.mem.debug_read_capacity_for_test(),
        primary_before,
        "private copies changed the ordinary debug map"
    );
    prepared.require_before_write().unwrap();
    // USER data is protected by the genuine native pin, not the copied debug
    // map. An ordinary writer may advance after SYSTEM inspection releases.
    let write = db.begin_write().unwrap();
    write
        .open_table(support::ROWS)
        .unwrap()
        .insert(0, [9_u8; 8192].as_slice())
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        prepared.require_before_write(),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Stale
        ))
    ));
    assert_eq!(
        actual
            .open_table(support::ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        6
    );
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(support::ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()[0],
        9
    );
}
