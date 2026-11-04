// req: STO-020, REC-001, REC-003, PRJ-004
use super::*;
use crate::db::ReadableDatabase;
use crate::{Database, TableDefinition};
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
fn fixture() -> (tempfile::NamedTempFile, Database, ReadTransaction) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let database = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let write = database.begin_write().unwrap();
    {
        let mut rows = write.open_table(ROWS).unwrap();
        for i in 0..256u32 {
            rows.insert(i.to_be_bytes().as_slice(), vec![7u8; 1536].as_slice())
                .unwrap();
        }
    }
    write
        .open_table(META)
        .unwrap()
        .insert("identity", b"native".as_slice())
        .unwrap();
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    // Observe actual native pages/type definitions using the unchanged parser;
    // no fake maxima, private source setter or synthetic stored payload.
    {
        let rows = read.open_table(ROWS).unwrap();
        for row in rows.range_owned::<&[u8]>(..).unwrap() {
            row.unwrap();
        }
        read.open_table(META)
            .unwrap()
            .get_owned("identity")
            .unwrap()
            .unwrap();
    }
    (file, database, read)
}
#[test]
fn native_primary_context_admits_namespace_and_table_pages_and_preserves_root() {
    let (_file, database, read) = fixture();
    let plan = read.plan_read_context().unwrap();
    assert!(plan.peak_bytes() > plan.limits.page_bytes as u64);
    let mut context = read.open_planned_read_context(plan).unwrap();
    assert!(
        context.context_current_for_test() > 0,
        "native namespace page lacks numerical admission"
    );
    let namespace_charge = context.context_current_for_test();
    {
        let mut rows = context.open_bytes_table("rows").unwrap();
        assert!(
            rows.context_current_for_test() > namespace_charge,
            "native table page lacks numerical admission"
        );
        let row = rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap();
        assert_eq!(row.value(), [7; 1536]);
    }
    // The existing pinned root remains authoritative for this read while an
    // ordinary native write advances. No new registration/current-root reopen.
    let write = database.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(0u32.to_be_bytes().as_slice(), b"new".as_slice())
        .unwrap();
    write.commit().unwrap();
    {
        let mut rows = context.open_bytes_table("rows").unwrap();
        let row = rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap();
        assert_eq!(row.value(), [7; 1536]);
    }
    drop(context);
}
#[test]
fn native_primary_context_rejects_foreign_read_without_changing_ordinary_write() {
    let (_file, database, read) = fixture();
    let plan = read.plan_read_context().unwrap();
    let other = database.begin_read().unwrap();
    assert!(matches!(
        other.open_planned_read_context(plan),
        Err(ReadOnlyReopenError::Stale)
    ));
    // Resource refusal did not install a policy on the actual primary.
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert("identity", b"ordinary".as_slice())
        .unwrap();
    write.commit().unwrap();
}
#[test]
fn native_primary_context_forgotten_value_blocks_new_allocations() {
    let (_file, _database, read) = fixture();
    let mut context = read
        .open_planned_read_context(read.plan_read_context().unwrap())
        .unwrap();
    let mut rows = context.open_bytes_table("rows").unwrap();
    let value = rows.get(0u32.to_be_bytes().as_slice()).unwrap().unwrap();
    core::mem::forget(value);
    for _ in 0..3 {
        assert!(matches!(
            rows.get(1u32.to_be_bytes().as_slice()),
            Err(ReadOnlyReopenError::Capacity)
        ));
    }
    drop(rows);
    drop(context);
}
#[test]
fn native_primary_context_forgotten_range_blocks_fanout() {
    let (_file, _database, read) = fixture();
    let mut context = read
        .open_planned_read_context(read.plan_read_context().unwrap())
        .unwrap();
    let mut rows = context.open_bytes_table("rows").unwrap();
    let range = rows.range_from(None).unwrap();
    core::mem::forget(range);
    assert!(matches!(
        rows.range_from(None),
        Err(ReadOnlyReopenError::Capacity)
    ));
    drop(rows);
    assert!(matches!(
        context.open_str_table("meta"),
        Err(ReadOnlyReopenError::Capacity)
    ));
    drop(context);
}
#[test]
#[cfg(debug_assertions)]
fn native_primary_context_private_metadata_preserves_global_map_and_refuses_page_capacity() {
    let (_file, _database, read) = fixture();
    let ordinary = read.open_table(ROWS).unwrap();
    let mut retained = Vec::new();
    let mut next = 0u32;
    for key in 0..255u32 {
        retained.push(
            ordinary
                .get_owned(key.to_be_bytes().as_slice())
                .unwrap()
                .unwrap(),
        );
        let (len, capacity) = read.mem.debug_read_capacity_for_test();
        if len == capacity {
            next = key + 1;
            break;
        }
    }
    assert!(
        next > 0,
        "genuine fixed-size row fixture must fill actual debug map"
    );
    let before = read.mem.debug_read_capacity_for_test();
    let mut context = read
        .open_planned_read_context(read.plan_read_context().unwrap())
        .unwrap();
    let mut rows = context.open_bytes_table("rows").unwrap();
    // Skip enough ordinary leaf keys to force a genuinely different page.
    let target = (next + 4).min(255);
    let physical_controls = rows.context_current_for_test();
    {
        let value = rows.get(target.to_be_bytes().as_slice()).unwrap().unwrap();
        assert_eq!(value.value(), [7; 1536]);
    }
    assert_eq!(rows.context_current_for_test(), physical_controls);
    assert_eq!(read.mem.debug_read_capacity_for_test(), before);
    drop(rows);
    drop(context);
    // Preserve a meaningful allocation refusal: namespace/table roots fit
    // their actual measured physical bound, but a new copied path page cannot
    // borrow the separately held metadata reservation on that SAME counter.
    let mut short = read.plan_read_context().unwrap();
    short.shorten_page_capacity_for_test(physical_controls);
    let mut context = read.open_planned_read_context(short).unwrap();
    let mut rows = context.open_bytes_table("rows").unwrap();
    assert_eq!(rows.context_current_for_test(), physical_controls);
    let result = rows.get(target.to_be_bytes().as_slice());
    assert!(matches!(&result,
        Err(ReadOnlyReopenError::Storage(crate::StorageError::Io(error)))
            if error.kind() == std::io::ErrorKind::OutOfMemory));
    drop(result);
    assert_eq!(
        rows.context_current_for_test(),
        physical_controls,
        "refused physical copy changed the page-only live balance"
    );
    assert_eq!(read.mem.debug_read_capacity_for_test(), before);
    drop(rows);
    drop(context);
    assert_eq!(
        ordinary
            .get_owned(target.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [7; 1536]
    );
}

// The empty padded PageList used below is a numerical native-format fixture.
// It is not an application command, Original source prefix or Source authority.
fn warm_current_system_shapes(database: &Database, read: &ReadTransaction) {
    let epoch = database.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let mut budget =
        crate::transactions::bounded_reclamation::ReclamationBudget::for_emitted_entries(0);
    let observed = system.observe_freed_paths(&mut budget).unwrap();
    observed.require_current().unwrap();
}
fn native_system_oom<T>(result: Result<T, crate::tree_store::NativeKeyPathError>) -> bool {
    matches!(result,
        Err(crate::tree_store::NativeKeyPathError::Native(crate::StorageError::Io(error)))
        if error.kind() == std::io::ErrorKind::OutOfMemory)
}
fn actual_namespace_charges(database: &Database, read: &ReadTransaction) -> (usize, usize) {
    let context = read
        .open_planned_read_context(read.plan_read_context().unwrap())
        .unwrap();
    let user = context.context_current_for_test();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let system = context
        .actual_read_for_system_test()
        .capture_native_system_read(&epoch)
        .unwrap();
    let both = context.context_current_for_test();
    assert!(user > 0);
    assert!(
        both > user,
        "SYSTEM namespace must share the actual read context"
    );
    drop(system);
    assert_eq!(context.context_current_for_test(), user);
    (user, both - user)
}

#[test]
fn native_primary_context_system_namespace_refuses_before_extra_allocation() {
    let (_file, database, read) = fixture();
    warm_current_system_shapes(&database, &read);
    // The short limit is deliberately a regression control, not a supported
    // production quote. It exactly admits the real existing user namespace.
    let mut plan = read.plan_read_context().unwrap();
    let probe = read
        .open_planned_read_context(read.plan_read_context().unwrap())
        .unwrap();
    let user = probe.context_current_for_test();
    assert!(user > 0);
    drop(probe);
    plan.limits.page_bytes = user;
    let context = read.open_planned_read_context(plan).unwrap();
    let before = context.context_current_for_test();
    let epoch = database.capture_native_resource_epoch().unwrap();
    assert!(
        native_system_oom(
            context
                .actual_read_for_system_test()
                .capture_native_system_read(&epoch)
        ),
        "SYSTEM namespace bypassed the actual context capacity"
    );
    assert_eq!(
        context.context_current_for_test(),
        before,
        "refusal retains no extra admitted page allocation"
    );
    epoch.require_current().unwrap();
}

#[test]
fn native_primary_context_system_table_refuses_before_extra_allocation() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let database = crate::Builder::new()
        .set_cache_size(0)
        .create(file.path())
        .unwrap();
    let mut tx = database.begin_write().unwrap();
    tx.set_finite_reclamation_for_test(0);
    tx.open_table(ROWS)
        .unwrap()
        .insert(b"key".as_slice(), b"value".as_slice())
        .unwrap();
    {
        use crate::transactions::{DATA_FREED_TABLE, PageList, TransactionIdWithPagination};
        let mut namespace = tx.system_tables.lock().unwrap();
        let mut table = namespace.open_system_table(DATA_FREED_TABLE).unwrap();
        // Valid native trailing capacity creates an 8-KiB DATA leaf. It is not
        // the supported 400-slot production class, and grants no Source fact.
        let mut value = table
            .insert_reserve(
                &TransactionIdWithPagination {
                    transaction_id: 0,
                    pagination_id: 0,
                },
                PageList::required_bytes(800),
            )
            .unwrap();
        value.as_mut().clear();
    }
    tx.commit().unwrap();
    let read = database.begin_read().unwrap();
    warm_current_system_shapes(&database, &read);
    let (user, system) = actual_namespace_charges(&database, &read);
    let mut plan = read.plan_read_context().unwrap();
    // Admit both genuine namespace caches plus one same-size transient
    // namespace descent; the larger DATA root must refuse before its buffer.
    plan.limits.page_bytes = user.checked_add(system.checked_mul(2).unwrap()).unwrap();
    let context = read.open_planned_read_context(plan).unwrap();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let captured = context
        .actual_read_for_system_test()
        .capture_native_system_read(&epoch)
        .unwrap();
    let before = context.context_current_for_test();
    let mut budget =
        crate::transactions::bounded_reclamation::ReclamationBudget::for_emitted_entries(0);
    assert!(
        native_system_oom(captured.observe_freed_paths(&mut budget)),
        "SYSTEM DATA table bypassed the actual context capacity"
    );
    assert_eq!(
        context.context_current_for_test(),
        before,
        "refusal retains no extra admitted page allocation"
    );
    captured.require_current().unwrap();
    drop(captured);
    epoch.require_current().unwrap();
    assert_eq!(context.context_current_for_test(), user);
}

#[test]
fn native_primary_context_system_consuming_preparation_releases_its_pages() {
    let (_file, database, read) = fixture();
    warm_current_system_shapes(&database, &read);
    let context = read
        .open_planned_read_context(read.plan_read_context().unwrap())
        .unwrap();
    let user = context.context_current_for_test();
    let epoch = database.capture_native_resource_epoch().unwrap();
    let captured = context
        .actual_read_for_system_test()
        .capture_native_system_read(&epoch)
        .unwrap();
    assert!(context.context_current_for_test() > user);
    let mut budget =
        crate::transactions::bounded_reclamation::ReclamationBudget::for_emitted_entries(0);
    let prepared = captured.prepare_reclamation(&mut budget).unwrap();
    prepared.require_before_write().unwrap();
    assert_eq!(
        context.context_current_for_test(),
        user,
        "closed preparation releases all its context-charged SYSTEM pages before write ownership"
    );
    drop(prepared);
    assert_eq!(context.context_current_for_test(), user);
}
