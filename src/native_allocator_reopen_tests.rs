use super::{Database, ReadOnlyDatabase, ReadableDatabase};
use crate::tree_store::AllocatorResourceError;
use crate::{Durability, TableDefinition};

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("allocator-reopen-facts");

fn populated_file() -> tempfile::NamedTempFile {
    let file = crate::create_tempfile();
    let database = Database::create(file.path()).unwrap();
    let mut write = database.begin_write().unwrap();
    write.set_durability(Durability::Immediate).unwrap();
    write.set_two_phase_commit(true);
    write.open_table(TABLE).unwrap().insert(1, 2).unwrap();
    write.commit().unwrap();
    drop(database);
    file
}

// req: STO-020, REC-003
#[test]
fn allocator_reopen_observation_is_bound_to_actual_native_owner_and_root() {
    let file = populated_file();
    let first = ReadOnlyDatabase::open(file.path()).unwrap();
    let second = ReadOnlyDatabase::open(file.path()).unwrap();
    let observation = first.mem.allocator_reopen_component().unwrap();
    observation.verify_for(&first.mem).unwrap();
    assert_eq!(
        observation.verify_for(&second.mem).unwrap_err(),
        AllocatorResourceError::Stale
    );
    assert!(observation.facts().final_retained().capacity_bytes() > 0);
    assert_eq!(
        first
            .begin_read()
            .unwrap()
            .open_table(TABLE)
            .unwrap()
            .get_owned(1)
            .unwrap()
            .unwrap()
            .value(),
        2
    );
}

// req: STO-020, REC-003
#[test]
fn allocator_reopen_observation_refuses_actual_native_root_advance() {
    let file = populated_file();
    let database = Database::open(file.path()).unwrap();
    let observation = database.mem.allocator_reopen_component().unwrap();
    let before_transaction = database.mem.get_last_committed_transaction_id().unwrap();
    let mut write = database.begin_write().unwrap();
    write.set_durability(Durability::Immediate).unwrap();
    write.set_two_phase_commit(true);
    write.open_table(TABLE).unwrap().insert(3, 4).unwrap();
    write.commit().unwrap();
    // The immediate commit advances the primary, then the existing default
    // freed-page epilogue publishes a newer non-durable secondary system view.
    // This is a successful native commit, not a repair failure.
    assert!(database.mem.pending_non_durable_commit());
    assert!(!database.mem.needs_repair());
    assert!(database.mem.get_last_committed_transaction_id().unwrap() > before_transaction);
    assert_eq!(
        observation.verify_for(&database.mem).unwrap_err(),
        AllocatorResourceError::Stale
    );
    assert!(matches!(
        database.mem.allocator_reopen_component(),
        Err(AllocatorResourceError::Stale)
    ));
}
