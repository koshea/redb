//! Real native namespace/profile controls; no Source/write-plan authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::{Database, ReadableDatabase, TableDefinition};
const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("namespace_profile_rows");
fn profile(db: &Database) -> (usize, usize) {
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    let observed = system.inspect_namespace_profile().unwrap();
    assert!(observed.actual_definitions() <= 6);
    assert!(observed.prospective_union_bytes() <= observed.base_page_bytes());
    assert!(
        observed.actual_definitions() == 0
            || observed.actual_page_bytes() >= observed.base_page_bytes()
    );
    observed.require_current().unwrap();
    (
        observed.actual_definitions(),
        observed.prospective_union_bytes(),
    )
}
#[test]
fn native_system_namespace_whole_leaf_and_full_prospective_union_survive_real_commits_and_reopen() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    for value in 0..8_u8 {
        let write = db.begin_write().unwrap();
        {
            let mut rows = write.open_table(ROWS).unwrap();
            for key in 0..16 {
                rows.insert(key, [value; 8192].as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
        let (_, future) = profile(&db);
        assert!(
            future > 512 && future <= 4096,
            "the full SIX-family union must be priced"
        );
    }
    drop(db); // Actual default close records allocator metadata.
    let db = Database::open(file.path()).unwrap();
    let (definitions, future) = profile(&db);
    assert!(definitions > 0 && future <= 4096);
}
#[test]
fn native_system_namespace_refuses_small_unknown_and_unexcluded_shapes_without_policy_changes() {
    let file = crate::create_tempfile();
    let db = crate::Builder::new()
        .set_page_size(512)
        .create(file.path())
        .unwrap();
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let system = read.capture_native_system_read(&epoch).unwrap();
    assert!(matches!(
        system.inspect_namespace_profile(),
        Err(NativeKeyPathError::UnsupportedState)
    ));
    drop(system);
    epoch.require_current().unwrap();
    drop(epoch);
    drop(read);
    db.begin_write().unwrap().abort().unwrap();
    drop(db);
    // Real generic native SYSTEM producer; existing native behavior remains.
    // Source's closed numerical family profile must reject this valid outsider.
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut tables = write.system_tables.lock().unwrap();
        let definition = crate::transactions::SystemTableDefinition::<u64, &[u8]>::new(
            "outside_supported_family",
        );
        tables
            .open_system_table(definition)
            .unwrap()
            .insert(1, b"native".as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    // A genuine USER read/Free epoch is still no SYSTEM exclusion proof.
    assert!(matches!(
        read.tree.inspect_system_namespace_profile(&epoch),
        Err(NativeKeyPathError::Epoch(
            NativeResourcePlanningRefusal::Unsupported
        ))
    ));
    let system = read.capture_native_system_read(&epoch).unwrap();
    assert!(matches!(
        system.inspect_namespace_profile(),
        Err(NativeKeyPathError::UnsupportedState)
    ));
    drop(system);
    epoch.require_current().unwrap();
    drop(epoch);
    drop(read);
    db.begin_write().unwrap().abort().unwrap();
}
