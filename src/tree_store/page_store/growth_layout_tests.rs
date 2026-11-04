//! Native geometry shared by the real grow path and numerical resource quote.
// req: STO-020, REC-001, PRJ-004
use super::super::{allocator_reopen_facts::AllocatorResourceError as E, region::Allocators};
use super::*;

fn layout(pages: u64) -> DatabaseLayout {
    DatabaseLayout::calculate(pages * 4096, 64, 1, 4096)
}

#[test]
fn native_growth_layout_preserves_single_full_and_trailing_native_cases() {
    // Actual native calculate representations: a single exact-full region has
    // zero full-region count and an exact-full trailing region.
    for (old_pages, order, expected_pages) in [
        (1, 0, 3),
        (8, 0, 16),
        (8, 4, 40),
        (64, 0, 128),
        (128, 0, 192),
        (129, 0, 192),
        (191, 0, 256),
    ] {
        let before = layout(old_pages);
        let after = before.growth_for(order).unwrap();
        assert_eq!(after.usable_bytes(), expected_pages * 4096);
        assert!(after.len() > before.len());
        assert_eq!(after.full_region_layout(), before.full_region_layout());
        let mut allocator = Allocators::new(before);
        let quote = allocator.resize_quote(before, after).unwrap();
        allocator.resize_to(after);
        let actual = allocator.reopen_capacity_facts().unwrap();
        assert!(
            quote.retained_after_bytes
                >= 2 * actual.capacity_bytes() as u64 + 272 * actual.buffer_count() as u64
        );
        assert_eq!(
            allocator
                .region_allocators
                .iter()
                .map(|a| u64::from(a.len()))
                .sum::<u64>(),
            expected_pages
        );
    }
}

#[test]
fn native_growth_layout_refuses_overflow_and_exhausted_format_before_native_resize() {
    use super::super::base::{MAX_PAGE_INDEX, MAX_REGIONS};
    let full = RegionLayout::new(64, 1, 4096);
    let exhausted = DatabaseLayout::new(MAX_REGIONS, full, None);
    assert!(matches!(exhausted.growth_for(0), Err(E::Unsupported)));
    let oversized = DatabaseLayout::new(
        MAX_REGIONS,
        RegionLayout::new(MAX_PAGE_INDEX + 1, 1, 1 << 31),
        None,
    );
    assert!(matches!(oversized.growth_for(0), Err(E::Overflow)));
    let ordinary = layout(1);
    assert!(matches!(ordinary.growth_for(u8::MAX), Err(E::Unsupported)));
    assert_eq!(ordinary.usable_bytes(), 4096);
}

#[test]
fn native_growth_layout_rejects_foreign_trailing_geometry_without_mutation() {
    let full = RegionLayout::new(64, 1, 4096);
    let invalid = DatabaseLayout::new(1, full, Some(RegionLayout::new(65, 1, 4096)));
    assert!(matches!(invalid.growth_for(0), Err(E::Incomplete)));
    let foreign = DatabaseLayout::new(1, full, Some(RegionLayout::new(1, 2, 4096)));
    assert!(matches!(foreign.growth_for(0), Err(E::Incomplete)));
    assert_eq!(invalid.num_full_regions(), 1);
    assert_eq!(foreign.num_full_regions(), 1);
}

#[test]
fn native_growth_oversized_tiny_region_refuses_and_preserves_committed_database() {
    use crate::{Database, ReadableDatabase, ReadableTable, StorageError, TableDefinition};
    const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("growth-bounds");
    let file = crate::create_tempfile();
    let mut db = Database::builder()
        .set_page_size(1024)
        .set_region_size(8 * 1024)
        .create(file.path())
        .unwrap();
    let first = db.begin_write().unwrap();
    first
        .open_table(ROWS)
        .unwrap()
        .insert(1, b"committed".as_slice())
        .unwrap();
    first.commit().unwrap();
    let before = file.as_file().metadata().unwrap().len();
    let next = db.begin_write().unwrap();
    {
        let mut table = next.open_table(ROWS).unwrap();
        let value = vec![7_u8; 32 * 1024];
        eprintln!("native_actual_tiny_region_oversized_allocation_begin");
        assert!(
            matches!(
                table.insert(2, value.as_slice()),
                Err(StorageError::Unsupported)
            ),
            "oversized native allocation must refuse before growing unsupported regions"
        );
    }
    next.abort().unwrap();
    assert_eq!(file.as_file().metadata().unwrap().len(), before);
    assert!(db.check_integrity().unwrap());
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), b"committed");
    assert!(table.get(2).unwrap().is_none());
    drop(table);
    drop(read);
    drop(db);
    let reopened = Database::builder()
        .set_page_size(1024)
        .set_region_size(8 * 1024)
        .open(file.path())
        .unwrap();
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), b"committed");
    assert!(table.get(2).unwrap().is_none());
}
