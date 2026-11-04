//! Genuine native writer schedules for prepared tracking and adopted tails.
//! Unit numerical marker only; no complete Source reservation/factory theorem.
// req: STO-020, PRJ-004, REC-001
use super::native_point_payload_tests::{ROWS, fixture};
use crate::{Database, ReadableDatabase, ReadableTable, StorageError};

#[test]
fn native_source_tracking_refuses_before_first_allocator_mutation_and_abort_still_works() {
    let (_file, db) = fixture();
    let read = db.begin_read().unwrap();
    let original = read.open_table(ROWS).unwrap();
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    let epoch = db.capture_native_resource_epoch().unwrap();
    let limits = db.get_memory().read_context_limits().unwrap();
    let before = db.get_memory().count_allocated_pages().unwrap();
    let (write, resources) = db
        .begin_point_payload_write_for_test(epoch, limits, 0)
        .unwrap();
    let refused;
    {
        let mut table = write.open_table(ROWS).unwrap();
        let result = table.insert(b"0".as_slice(), [8_u8; 320].as_slice());
        refused = matches!(result, Err(StorageError::Io(ref error)) if error.kind() == std::io::ErrorKind::OutOfMemory);
        drop(result);
    }
    let after = db.get_memory().count_allocated_pages().unwrap();
    write.abort().unwrap();
    assert_eq!(resources.current(), 0);
    assert_eq!(
        original.get(b"1".as_slice()).unwrap().unwrap().value(),
        [7_u8; 320]
    );
    assert!(original.get(b"0".as_slice()).unwrap().is_none());
    let ordinary = db.begin_write().unwrap();
    ordinary
        .open_table(ROWS)
        .unwrap()
        .insert(b"2".as_slice(), b"ordinary".as_slice())
        .unwrap();
    ordinary.abort().unwrap();
    // Cleanup precedes the contrast assertions in both RED and GREEN images.
    assert!(
        refused,
        "prepared tracking request capacity must refuse before allocation"
    );
    assert_eq!(
        after, before,
        "tracking refusal changed the actual allocator"
    );
}

#[test]
fn native_source_post_only_batch_survives_commit_and_next_real_adoption() {
    let (_file, db) = fixture();
    {
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(ROWS)
                .unwrap()
                .get(b"1".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            [7_u8; 320]
        );
    }
    let epoch = db.capture_native_resource_epoch().unwrap();
    let mut limits = db.get_memory().read_context_limits().unwrap();
    // Explicit numerical fixture envelope for SYSTEM PageList's existing
    // 400-page pagination plus one key/native header; no production policy.
    let page_list =
        crate::transactions::PageList::main_pagination_page_bytes_for_test(limits.base_page);
    limits.leaf = limits.leaf.max(page_list);
    limits.branch = limits.branch.max(page_list);
    // This fixture-only capacity covers the single point operation, both
    // six-family single-leaf namespaces and the ordinary post-commit pass. It
    // is a deliberately loose provision, NOT the missing logical-demand theorem.
    let (write, resources) = db
        .begin_point_payload_write_for_test(epoch, limits, 128)
        .unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        drop(
            table
                .insert(b"1".as_slice(), b"replacement".as_slice())
                .unwrap(),
        );
    }
    write.commit().unwrap();
    let tail = db.get_memory().reclamation_quote_snapshot_for_test();
    assert!(
        tail.post_only && tail.post_pages > 0,
        "real post-commit tail precondition"
    );
    assert!(
        resources.current() > 0,
        "persistent source batch lost its custody"
    );
    {
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(ROWS)
                .unwrap()
                .get(b"1".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"replacement"
        );
    }
    // A real ordinary durable writer must adopt/retire the original batch,
    // without acquiring Source admission or changing normal native defaults.
    db.begin_write().unwrap().commit().unwrap();
    assert_eq!(
        resources.current(),
        0,
        "old Source buffers survived final real adoption"
    );
}
