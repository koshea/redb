//! Native unit-marker physical payload observations; no Source64/full plan.
// req: STO-020, PRJ-004, REC-001
use crate::tree_store::{
    NativePageResources, NativePointKey as K, NativePointMutation as M, NativePointRecipe,
    NativeReadContext,
};
use crate::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError,
    TableDefinition, TableError,
};

// Fixture-only total insertion/adoption attempt capacity. These schedules
// each execute at most two point operations on a single leaf (split into two
// leaves + one root) and abort before any SYSTEM epilogue. Eight requests cover
// those actual constructor/mutator paths; this is NOT a production demand rule.
const FIXTURE_REQUESTS: usize = 8;
pub(super) const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("point_payload_rows");
pub(super) fn fixture() -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let mut builder = Database::builder();
    builder.set_page_size(512);
    let db = builder.create(file.path()).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        table
            .insert(b"1".as_slice(), [7_u8; 320].as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    (file, db)
}

#[test]
fn native_point_source_payload_retains_deleted_dirty_copy_until_real_guard_drop() {
    let (_file, db) = fixture();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let original = read.open_table(ROWS).unwrap();
    let operations = [
        M::Put {
            key: K::Bytes(b"0"),
            value: &[8_u8; 320],
        },
        M::Delete {
            key: K::Bytes(b"1"),
        },
    ];
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let facts = original
        .prepare_point_frontier(&recipe, &epoch)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(facts.old_leaves(), 1);
    original.validate_point_frontier(&facts).unwrap();
    // Real parsed/read observations, not injected page maxima. This component
    // envelope is intentionally not the complete native write/adopted-tail quote.
    let limits = db.get_memory().read_context_limits().unwrap();
    let metadata = NativeReadContext::metadata_capacity_bytes(limits).unwrap()
        + NativeReadContext::fixed_write_tracking_capacity_bytes(FIXTURE_REQUESTS).unwrap()
        + crate::transactions::WriteTransaction::point_payload_shared_controls_for_test().unwrap();
    drop(facts);
    let (write, resources) = db
        .begin_point_payload_write_for_test(epoch, limits, FIXTURE_REQUESTS)
        .unwrap();
    assert_eq!(resources.current(), metadata);
    assert_eq!(resources.materialized_observation_for_test(), (0, 0, 0, 0));
    let during_old_guard;
    let after_old_guard;
    {
        let mut table = write.open_table(ROWS).unwrap();
        // The actual generic split makes both leaf pages Source-owned, instead
        // of retaining an old committed large-leaf fast-path allocation.
        let inserted_old = table
            .insert(b"0".as_slice(), [8_u8; 320].as_slice())
            .unwrap();
        assert!(inserted_old.is_none());
        drop(inserted_old); // Closed apply never carries a guard into next op.
        assert_eq!(table.stats().unwrap().tree_height(), 2);
        let old = table.remove(b"1".as_slice()).unwrap().unwrap();
        assert_eq!(old.value(), [7_u8; 320]);
        during_old_guard = resources.current();
        let (live_bytes, _, live_buffers, _) = resources.materialized_observation_for_test();
        assert_eq!(
            (live_bytes, live_buffers),
            (1024, 2),
            "remaining Source leaf plus independently retained old copied leaf"
        );
        // Logical free removed the dirty page's write-buffer allocation, while
        // the independently copied return buffer remains genuinely owned.
        drop(old);
        after_old_guard = resources.current();
        let (live_bytes, _, live_buffers, _) = resources.materialized_observation_for_test();
        assert_eq!((live_bytes, live_buffers), (512, 1));
        assert!(during_old_guard > after_old_guard);
        assert!(
            after_old_guard > metadata,
            "remaining new leaf must still own its allocation"
        );
        assert_eq!(
            table.get(b"0".as_slice()).unwrap().unwrap().value(),
            [8_u8; 320]
        );
        assert!(table.get(b"1".as_slice()).unwrap().is_none());
        assert_eq!(
            table.stats().unwrap().tree_height(),
            1,
            "actual root collapse"
        );
    }
    let high_water = resources.admitted_high_water_for_test();
    let (_, physical_peak, _, peak_buffers) = resources.materialized_observation_for_test();
    assert!(
        physical_peak >= 5 * 512,
        "real write-buffer originals and private read copies coexist"
    );
    assert!(peak_buffers >= 5);
    assert!(high_water > after_old_guard);
    write.abort().unwrap();
    assert_eq!(
        resources.current(),
        0,
        "real abort/namespace/context teardown must retire the numerical graph"
    );
    let (live_bytes, final_peak, live_buffers, final_peak_buffers) =
        resources.materialized_observation_for_test();
    assert_eq!((live_bytes, live_buffers), (0, 0));
    assert!(final_peak >= physical_peak && final_peak_buffers >= peak_buffers);
    assert!(
        resources.admitted_high_water_for_test() >= high_water,
        "abort cannot reset cumulative high-water"
    );
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    assert!(original.get(b"0".as_slice()).unwrap().is_none());
}

#[test]
fn native_point_source_payload_refuses_first_namespace_copy_without_retaging_ordinary_guard() {
    let (_file, db) = fixture();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let original = read.open_table(ROWS).unwrap();
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    let mut limits = db.get_memory().read_context_limits().unwrap();
    // Deliberate numerical capacity refusal, not a proposed production policy.
    limits.page_bytes = 0;
    let metadata = NativeReadContext::metadata_capacity_bytes(limits).unwrap()
        + NativeReadContext::fixed_write_tracking_capacity_bytes(FIXTURE_REQUESTS).unwrap()
        + crate::transactions::WriteTransaction::point_payload_shared_controls_for_test().unwrap();
    let (write, resources) = db
        .begin_point_payload_write_for_test(epoch, limits, FIXTURE_REQUESTS)
        .unwrap();
    assert_eq!(resources.current(), metadata);
    let failed = write.open_table(ROWS);
    assert!(
        matches!(failed, Err(TableError::Storage(StorageError::Io(ref error)))
        if error.kind() == std::io::ErrorKind::OutOfMemory)
    );
    drop(failed);
    assert_eq!(resources.current(), metadata);
    assert_eq!(resources.admitted_high_water_for_test(), metadata);
    assert_eq!(resources.materialized_observation_for_test(), (0, 0, 0, 0));
    write.abort().unwrap();
    assert_eq!(resources.current(), 0);
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    // A real subsequent ordinary writer remains available and uses defaults.
    let ordinary = db.begin_write().unwrap();
    {
        ordinary
            .open_table(ROWS)
            .unwrap()
            .insert(b"2".as_slice(), b"ordinary".as_slice())
            .unwrap();
    }
    ordinary.abort().unwrap();
}

#[test]
fn native_point_source_payload_distinguishes_oversized_write_buffer_and_fresh_same_page_copy() {
    let (_file, db) = fixture();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let original = read.open_table(ROWS).unwrap();
    let replacement = vec![9_u8; 4096];
    let operations = [M::Put {
        key: K::Bytes(b"1"),
        value: &replacement,
    }];
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let facts = original
        .prepare_point_frontier(&recipe, &epoch)
        .unwrap()
        .collect()
        .unwrap();
    original.validate_point_frontier(&facts).unwrap();
    let mut limits = db.get_memory().read_context_limits().unwrap();
    // Actual owning final geometry supplies a conservative largest individual
    // future page shape; no Source quota/profile knob is introduced. The
    // existing numerical read envelope is unchanged for this tiny fixture.
    let maximum = usize::try_from(facts.final_live_physical_upper()).unwrap();
    limits.leaf = limits.leaf.max(maximum);
    limits.branch = limits.branch.max(maximum);
    assert!(NativePageResources::page_charge_for_test(maximum).unwrap() < limits.page_bytes);
    drop(facts);
    let (write, resources) = db
        .begin_point_payload_write_for_test(epoch, limits, FIXTURE_REQUESTS)
        .unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        let old = table
            .insert(b"1".as_slice(), replacement.as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(old.value(), [7_u8; 320]);
        let (with_old, _, buffers, _) = resources.materialized_observation_for_test();
        assert_eq!(
            buffers, 2,
            "actual new buffer and old committed private copy"
        );
        assert!(
            with_old > replacement.len() + 512,
            "native physical power-of-two page rounding"
        );
        drop(old); // No returned guard crosses into the next operation.
        let (write_bytes, _, buffers, _) = resources.materialized_observation_for_test();
        assert_eq!(buffers, 1);
        assert_eq!(with_old - write_bytes, 512);
        let current = table.get(b"1".as_slice()).unwrap().unwrap();
        assert_eq!(current.value(), replacement);
        let (with_copy, _, buffers, _) = resources.materialized_observation_for_test();
        assert_eq!(
            (with_copy, buffers),
            (2 * write_bytes, 2),
            "same native page has a write-buffer original and fresh private copy"
        );
        drop(current);
        assert_eq!(resources.materialized_observation_for_test().0, write_bytes);
    }
    let (_, peak, _, peak_buffers) = resources.materialized_observation_for_test();
    write.abort().unwrap();
    let (live, final_peak, buffers, final_peak_buffers) =
        resources.materialized_observation_for_test();
    assert_eq!((live, buffers), (0, 0));
    assert!(final_peak >= peak && final_peak_buffers >= peak_buffers);
    assert_eq!(resources.current(), 0);
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
}

#[test]
fn native_point_source_payload_partial_split_error_retains_real_page_until_abort() {
    let (_file, db) = fixture();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let original = read.open_table(ROWS).unwrap();
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    let mut limits = db.get_memory().read_context_limits().unwrap();
    // Owning exact charge for two native base pages. The old copied leaf and
    // first split page fit; allocating the second split page must refuse.
    limits.page_bytes = NativePageResources::page_charge_for_test(limits.base_page)
        .unwrap()
        .checked_mul(2)
        .unwrap();
    // Page admission happens BEFORE preparing metadata for a new page. Pay
    // exactly ONE actual first-write cache owner, so it cannot consume the
    // two-page allowance; the next native page admission must still refuse.
    let first_cache_controls = db
        .get_memory()
        .point_payload_first_cache_control_capacity_for_test()
        .unwrap();
    let (write, resources) = db
        .begin_point_payload_write_with_controls_for_test(
            epoch,
            limits,
            FIXTURE_REQUESTS,
            first_cache_controls,
        )
        .unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        let failed = table.insert(b"0".as_slice(), [8_u8; 320].as_slice());
        assert!(matches!(failed, Err(StorageError::Io(ref error))
            if error.kind() == std::io::ErrorKind::OutOfMemory));
        let (kind, physical, requested, remaining) = resources.read_refusal_for_test();
        assert_eq!(
            (kind, physical),
            (4, 512),
            "the intended real second-page admission must refuse, not metadata"
        );
        assert_eq!(
            requested,
            NativePageResources::page_charge_for_test(512).unwrap()
        );
        assert!(remaining < requested);
        drop(failed);
        let (live, peak, buffers, peak_buffers) = resources.materialized_observation_for_test();
        assert_eq!(
            (live, buffers),
            (512, 1),
            "the actual first split page remains in the writer graph"
        );
        assert!(peak >= 1024 && peak_buffers >= 2);
    }
    write.abort().unwrap();
    let (live, _, buffers, _) = resources.materialized_observation_for_test();
    assert_eq!((live, buffers), (0, 0));
    assert_eq!(resources.current(), 0);
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    assert!(original.get(b"0".as_slice()).unwrap().is_none());
}
