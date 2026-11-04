// req: STO-020, PRJ-004, REC-001
//! Numerical native fixtures; these empty padded PageLists are not application
//! Source history, emission authority or a complete planned writer.
use super::{PageList, TransactionIdWithPagination};
use crate::{Database, Key, ReadableDatabase, StorageError, TableHandle, Value};

#[test]
fn native_freed_point_profile_uses_numeric_order_across_little_endian_boundaries() {
    for (left, right) in [
        (
            TransactionIdWithPagination {
                transaction_id: 255,
                pagination_id: 0,
            },
            TransactionIdWithPagination {
                transaction_id: 256,
                pagination_id: 0,
            },
        ),
        (
            TransactionIdWithPagination {
                transaction_id: 7,
                pagination_id: 255,
            },
            TransactionIdWithPagination {
                transaction_id: 7,
                pagination_id: 256,
            },
        ),
    ] {
        let left = TransactionIdWithPagination::as_bytes(&left);
        let right = TransactionIdWithPagination::as_bytes(&right);
        assert!(
            left > right,
            "little-endian byte lexicography is the wrong profile"
        );
        assert!(TransactionIdWithPagination::compare(&left, &right).is_lt());
    }
}

#[test]
fn native_freed_point_predecessor_and_successor_share_actual_typed_paths() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut namespace = write.system_tables.lock().unwrap();
        let mut table = namespace
            .open_system_table(crate::transactions::SYSTEM_FREED_TABLE)
            .unwrap();
        for (transaction_id, pagination_id) in [(255, 255), (255, 256), (256, 0)] {
            let key = TransactionIdWithPagination {
                transaction_id,
                pagination_id,
            };
            let mut value = table
                .insert_reserve(&key, PageList::required_bytes(200))
                .unwrap();
            value.as_mut().clear();
        }
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let actual = super::super::NativeSystemRead::capture(&read, &epoch).unwrap();
    let tree = actual
        .open_normal::<TransactionIdWithPagination, PageList<'static>>(
            crate::transactions::SYSTEM_FREED_TABLE.name(),
        )
        .unwrap()
        .unwrap();
    for ((transaction_id, pagination_id), expected) in [
        ((256, 0), (255, 256)),
        ((255, 256), (255, 255)),
        ((257, 0), (256, 0)),
    ] {
        let before = TransactionIdWithPagination {
            transaction_id,
            pagination_id,
        };
        let mut visits = crate::tree_store::CompiledPathVisits::for_freed_probe();
        let (entry, _) = tree
            .read_native_freed_before(&before, &actual.epoch, &mut visits)
            .unwrap();
        let entry = entry.unwrap();
        assert_eq!(
            (entry.key().transaction_id, entry.key().pagination_id),
            expected
        );
        entry.require_current().unwrap();
    }
    let after = TransactionIdWithPagination {
        transaction_id: 255,
        pagination_id: 255,
    };
    let mut visits = crate::tree_store::CompiledPathVisits::for_freed_probe();
    let (entry, _) = tree
        .read_native_freed_after(Some(&after), &actual.epoch, &mut visits)
        .unwrap();
    let entry = entry.unwrap();
    assert_eq!(
        (entry.key().transaction_id, entry.key().pagination_id),
        (255, 256)
    );
    drop(entry);
    drop(tree);
    drop(actual);
    epoch.require_current().unwrap();
}

#[test]
fn native_fixed_observation_refuses_ordinary_read_context_without_source_pool() {
    let file = crate::create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let read = db.begin_read().unwrap();
    let resolver = read.tree.read_page_resolver();
    assert!(
        matches!(resolver.admit_fixed_observation(0), Err(StorageError::Io(error))
        if error.kind() == std::io::ErrorKind::OutOfMemory)
    );
    assert!(
        matches!(resolver.fixed_observation_definition_limit(), Err(StorageError::Io(error))
        if error.kind() == std::io::ErrorKind::OutOfMemory)
    );
}
