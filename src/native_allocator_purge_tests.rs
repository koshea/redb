//! Native numerical fixtures, not application Source history or authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::tree_store::native_purge_vector_quote;
use crate::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use alloc::vec::Vec;
const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("purge_observation_rows");

fn reopened() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    {
        let db = crate::Builder::new()
            .set_page_size(512)
            .set_region_size(64 * 1024)
            .set_cache_size(0)
            .create(file.path())
            .unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for key in 0..256 {
                rows.insert(key, [7_u8; 4096].as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
    } // Genuine normal close records allocator state; no warmup command.
    let db = Database::builder()
        .set_page_size(512)
        .set_cache_size(0)
        .open(file.path())
        .unwrap();
    (file, db)
}
fn observe_and_compare(system: &NativeSystemRead<'_, '_, '_>) {
    let tree = system
        .open_normal::<AllocatorStateKey, &'static [u8]>(ALLOCATOR_STATE_TABLE_NAME)
        .unwrap()
        .unwrap();
    // Independent full native traversal is TEST ONLY. The production walker
    // never calls stats and does not claim stats is a bounded inspector.
    let expected = tree.stats().unwrap();
    let header = tree.get_root().unwrap();
    let expected_nodes = expected.leaf_pages + expected.branch_pages;
    assert!(
        expected_nodes > 1,
        "fixture must disprove a root-only quote"
    );
    drop(tree);
    let observed = system
        .allocator_purge_seed()
        .unwrap()
        .inspect_complete()
        .unwrap();
    assert!(observed.present());
    assert_eq!(observed.root(), Some(header));
    let facts = observed.facts();
    assert_eq!(facts.nodes, expected_nodes);
    assert_eq!(facts.leaves, expected.leaf_pages);
    assert_eq!(facts.branches, expected.branch_pages);
    assert_eq!(facts.rows, header.length);
    assert_eq!(facts.maximum_depth as u32, expected.tree_height);
    assert!(facts.base_pages >= facts.nodes);
    assert!(facts.maximum_ancestor_page_bytes >= facts.maximum_page_bytes);
    assert!(
        (facts.nodes + 1..=4 * header.length - 2).contains(&facts.fetches_including_cached_root)
    );
    assert!(facts.fetched_bytes >= facts.page_bytes);
    assert!(facts.key_slots >= 3 * facts.rows);
    let definition = observed.definition_path_upper();
    assert!(definition.0 > 0 && definition.1 > 0);
    observed.require_current().unwrap();
}
#[test]
fn native_allocator_purge_complete_census_covers_genuine_normal_close_and_reopen() {
    let (file, mut db) = reopened();
    {
        let epoch = db.capture_native_resource_epoch().unwrap();
        let read = db.begin_read().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        observe_and_compare(&system);
    }
    // The unchanged normal purge still runs and preserves actual user rows.
    let write = db.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0, [9_u8; 4096].as_slice())
        .unwrap();
    write.commit().unwrap();
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        [9; 4096]
    );
    assert!(db.check_integrity().unwrap());
    drop(db);
    // No synthetic source allocation/empty command is needed after a restart.
    let db = Database::builder()
        .set_page_size(512)
        .set_cache_size(0)
        .open(file.path())
        .unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    observe_and_compare(&system);
}
#[test]
fn native_allocator_purge_work_refuses_before_next_fetch_and_requires_exact_header() {
    let (_file, db) = reopened();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    for short in 0..3 {
        let mut seed = system.allocator_purge_seed().unwrap();
        match short {
            0 => seed.work.shorten_for_test(0, u64::MAX, u64::MAX),
            1 => seed.work.shorten_for_test(u64::MAX, 0, u64::MAX),
            _ => seed.work.shorten_for_test(u64::MAX, u64::MAX, 0),
        }
        assert!(matches!(
            seed.inspect_complete(),
            Err(NativeKeyPathError::VisitsExceeded)
        ));
        system.require_current().unwrap();
    }
    for checksum in [false, true] {
        let mut tree = system
            .open_normal::<AllocatorStateKey, &'static [u8]>(ALLOCATOR_STATE_TABLE_NAME)
            .unwrap()
            .unwrap();
        let header = tree.get_root().unwrap();
        let mut work = NativeAllocatorPurgeWork::from_actual_header(
            header.length + 1,
            (read.mem.get_page_size() as u64) * (1_u64 << 31),
        )
        .unwrap();
        work.before_fetch(header.root, read.mem.get_page_size())
            .unwrap();
        tree.alter_allocator_purge_header_for_test(checksum);
        assert!(matches!(
            tree.inspect_complete_allocator_purge(&system.epoch, &mut work),
            Err(NativeKeyPathError::Malformed)
        ));
    }
}
#[test]
fn native_allocator_purge_actual_context_refuses_before_extra_allocation() {
    let (_file, db) = reopened();
    let read = db.begin_read().unwrap();
    // Record actual native maxima by the genuine observer, not fake limits.
    {
        let epoch = db.capture_native_resource_epoch().unwrap();
        let system = read.capture_native_system_read(&epoch).unwrap();
        system
            .allocator_purge_seed()
            .unwrap()
            .inspect_complete()
            .unwrap();
    }
    let plan = read.plan_read_context().unwrap();
    let probe = read.open_planned_read_context(plan).unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = probe
        .actual_read_for_system_test()
        .capture_native_system_read(&epoch)
        .unwrap();
    let namespace = probe.context_current_for_test();
    assert!(namespace > 0);
    drop(system);
    drop(epoch);
    drop(probe);
    let mut plan = read.plan_read_context().unwrap();
    plan.shorten_page_capacity_for_test(namespace);
    let context = read.open_planned_read_context(plan).unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = context
        .actual_read_for_system_test()
        .capture_native_system_read(&epoch)
        .unwrap();
    let before = context.context_current_for_test();
    let result = system
        .allocator_purge_seed()
        .and_then(|seed| seed.inspect_complete());
    assert!(
        matches!(result, Err(NativeKeyPathError::Native(crate::StorageError::Io(error)))
        if error.kind() == std::io::ErrorKind::OutOfMemory)
    );
    assert_eq!(context.context_current_for_test(), before);
    system.require_current().unwrap();
}
#[test]
fn native_allocator_purge_vector_prices_cover_actual_growth_and_into_iter_overlap() {
    for count in [0_usize, 1, 4, 5, 8, 9, 63, 64, 65, 129] {
        let depth = if count == 0 { 0 } else { count.min(64) };
        let quote = native_purge_vector_quote(count, depth, 4, 3).unwrap();
        let mut pages = Vec::<crate::tree_store::PageNumber>::new();
        let page = crate::tree_store::PageNumber::new(0, 0, 0);
        for _ in 0..count {
            pages.push(page);
        }
        assert!(quote.pages_retained >= (pages.capacity() * core::mem::size_of_val(&page)) as u64);
        assert!(quote.pages_growth_peak >= quote.pages_retained);
        assert!(quote.recursive_paths_peak >= quote.recursive_paths_retained);
        let mut freed = Vec::with_capacity(4);
        freed.extend([page; 3]);
        for page in pages {
            freed.push(page);
        }
        assert!(
            quote.pages_and_freed_growth_peak
                >= quote.pages_retained + (freed.capacity() * core::mem::size_of_val(&page)) as u64
        );
    }
    assert!(native_purge_vector_quote(1, 65, 0, 0).is_err());
    assert!(native_purge_vector_quote(0, 1, 0, 0).is_err());
    assert!(native_purge_vector_quote(1, 1, 0, 1).is_err());
    assert!(native_purge_vector_quote(usize::MAX, 64, usize::MAX, usize::MAX).is_err());
    assert!(NativeAllocatorPurgeWork::from_actual_header(u64::MAX, 4096).is_err());
}

// Same read-only native shape-discovery prerequisite as the qualified Current
// context tests. This is not a command/warmup write or a Source factory proof.
fn observe_live_system_shapes(db: &Database) {
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    system
        .allocator_purge_seed()
        .unwrap()
        .inspect_complete()
        .unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(0);
    let observer = system.observe_freed_paths(&mut budget).unwrap();
    observer.require_current().unwrap();
}

#[test]
fn native_planned_current_allocator_purge_uses_the_actual_checked_pin() {
    let (_file, db) = reopened();
    let planned = db
        .begin_planned_read_context(db.plan_current_read_context().unwrap())
        .unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = planned.actual_read_for_system_test();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    // This MUST use the genuine inline planned pin, never an ordinary fallback.
    // Its actual namespace/SYSTEM pages share the preadmitted native context.
    observe_and_compare(&system);
    system.require_current().unwrap();
    drop(system);
    epoch.require_current().unwrap();
}

#[test]
fn native_combined_system_preparation_moves_complete_purge_and_releases_all_system_pages() {
    let (_file, db) = reopened();
    let planned = db
        .begin_planned_read_context(db.plan_current_read_context().unwrap())
        .unwrap();
    let user_charge = planned.context_current_for_test();
    let actual = planned.actual_read_for_system_test();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    #[cfg(debug_assertions)]
    let system_root = actual.mem.get_system_root().unwrap().root;
    #[cfg(debug_assertions)]
    assert!(
        actual
            .tree
            .read_page_resolver()
            .copied_page_references_for_test(system_root)
            > 0
    );
    #[cfg(debug_assertions)]
    assert!(!actual.mem.native_page_has_read_alias_for_test(system_root));
    let mut budget = ReclamationBudget::for_emitted_entries(0);
    let prepared = system.prepare_system_resources(&mut budget).unwrap();
    assert!(prepared.allocator_state_present());
    let root = prepared.allocator_root().unwrap();
    assert_eq!(prepared.purge_facts().rows, root.length);
    assert!(prepared.purge_facts().nodes > 1);
    assert!(prepared.definition_path_upper().0 > 0);
    assert_eq!(prepared.selected_records(), 0);
    assert_eq!(
        planned.context_current_for_test(),
        user_charge,
        "combined owner retained SYSTEM/allocator page allocations"
    );
    #[cfg(debug_assertions)]
    assert!(!actual.mem.native_page_has_read_alias_for_test(system_root));
    #[cfg(debug_assertions)]
    assert_eq!(
        actual
            .tree
            .read_page_resolver()
            .copied_page_references_for_test(system_root),
        0
    );
    prepared.require_before_write().unwrap();
    epoch.require_current().unwrap();
}

#[test]
fn native_combined_system_preparation_has_one_real_budget_and_refuses_after_writer_wins() {
    use crate::transactions::native_prepared_reclamation_test_support as support;
    let (_file, db, old) = support::backlog_from_empty_reader();
    // Native syntax/control fixture: retain actual old-reader retirement work
    // while the EXISTING native quick-repair producer records a real allocator
    // snapshot. This grants no Source history or application mutation authority.
    let mut write = db.begin_write().unwrap();
    write.set_finite_reclamation_for_test(0);
    write.set_quick_repair(true);
    write.commit().unwrap();
    drop(old);
    observe_live_system_shapes(&db);
    let planned = db
        .begin_planned_read_context(db.plan_current_read_context().unwrap())
        .unwrap();
    let actual = planned.actual_read_for_system_test();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    let prepared = system.prepare_system_resources(&mut budget).unwrap();
    assert!(prepared.allocator_state_present());
    assert!(prepared.purge_facts().rows > 0);
    assert_eq!(
        prepared.selected_records(),
        1,
        "the SAME single budget must seal one actual eligible whole entry"
    );
    prepared.require_before_write().unwrap();
    // Deterministic race schedule: the real ordinary native writer wins only
    // AFTER consuming preparation has released its SYSTEM inspection lease.
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
    // The genuine current pin still owns its old USER data, not freed SYSTEM.
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
fn native_combined_system_preparation_refusal_releases_inspection_without_a_prepared_owner() {
    use crate::transactions::native_prepared_reclamation_test_support as support;
    let (_file, db) = support::seeded(true);
    observe_live_system_shapes(&db);
    // The actual401-slot PageList is valid native syntax but outside the finite
    // emitted-entry profile. An incomplete prefix cannot return combined proof.
    let planned = db
        .begin_planned_read_context(db.plan_current_read_context().unwrap())
        .unwrap();
    let user_charge = planned.context_current_for_test();
    let actual = planned.actual_read_for_system_test();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = actual.capture_native_system_read(&epoch).unwrap();
    let mut budget = ReclamationBudget::for_emitted_entries(1);
    assert!(matches!(
        system.prepare_system_resources(&mut budget),
        Err(NativeKeyPathError::UnsupportedState)
    ));
    assert_eq!(planned.context_current_for_test(), user_charge);
    epoch.require_current().unwrap();
    // No policy/obligation was installed on the ordinary native writer.
    db.begin_write().unwrap().abort().unwrap();
}
