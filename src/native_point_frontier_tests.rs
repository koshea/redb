//! Real native fixtures; numerical frontier bounds never grant Source authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::tree_store::{
    NativeKeyPathError, NativePointKey as K, NativePointMutation as M, NativePointRecipe,
    NativePointRecipeClass, point_frontier_capture_bytes,
};
use crate::{Database, ReadableDatabase, TableDefinition};

const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("point_frontier_rows");
fn fixture(rows: u64) -> (tempfile::NamedTempFile, Database) {
    let file = crate::create_tempfile();
    let mut builder = Database::builder();
    builder.set_page_size(512);
    let db = builder.create(file.path()).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(ROWS).unwrap();
        for i in 0..rows {
            t.insert(i.to_be_bytes().as_slice(), [7_u8; 96].as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
    (file, db)
}

#[test]
fn native_point_frontier_contains_actual_sparse_delete_neighbors() {
    let (_file, db) = fixture(512);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert!(
        table.stats().unwrap().tree_height() >= 3,
        "real multi-parent fixture required"
    );
    let keys: Vec<_> = (0_u64..64).map(u64::to_be_bytes).collect();
    let operations: Vec<_> = keys
        .iter()
        .map(|k| M::Delete { key: K::Bytes(k) })
        .collect();
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    assert_eq!(recipe.class(), NativePointRecipeClass::DeleteOnly);
    assert!(point_frontier_capture_bytes().unwrap() > 0);
    let preparation = table.prepare_point_frontier(&recipe, &epoch).unwrap();
    assert!(preparation.record_capacity_bytes() > point_frontier_capture_bytes().unwrap());
    let (facts, pages) = preparation.collect_pages_for_test().unwrap();
    facts.require_current().unwrap();
    assert!(facts.old_branches() >= 3);
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(ROWS).unwrap();
        for key in &keys {
            assert_eq!(
                t.remove(key.as_slice()).unwrap().unwrap().value(),
                [7_u8; 96]
            );
        }
    }
    let actually_freed = tx.data_freed_pages_for_point_test();
    assert!(!actually_freed.is_empty());
    tx.abort().unwrap();
    // Release the genuine write before assertions about the old immutable read.
    // The old point-path-only control deliberately omits real merge neighbors.
    let missing: Vec<_> = actually_freed
        .iter()
        .filter(|p| !pages.contains(p))
        .collect();
    assert!(
        missing.is_empty(),
        "actual committed DATA deletion pages escaped frontier"
    );
    assert!(actually_freed.len() <= facts.old_pages());
    assert_eq!(
        table
            .get(0_u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [7_u8; 96]
    );
    assert!(facts.require_current().is_err()); // aborted writes invalidate the proposal
}

#[test]
fn native_point_frontier_bounds_real_put_splits_oversized_values_and_replacements() {
    let (_file, db) = fixture(80);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let keys: Vec<_> = (0_u64..96).map(u64::to_be_bytes).collect();
    // Full old-key coverage makes the affected forest the whole actual tree.
    // Mix same-size replacements, oversized replacements and new appends.
    let values: Vec<Vec<u8>> = (0..keys.len())
        .map(|i| vec![i as u8; if i % 3 == 0 { 4096 } else { 96 }])
        .collect();
    let operations: Vec<_> = keys
        .iter()
        .zip(&values)
        .map(|(k, v)| M::Put {
            key: K::Bytes(k),
            value: v,
        })
        .collect();
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let preparation = table.prepare_point_frontier(&recipe, &epoch).unwrap();
    let (facts, pages) = preparation.collect_pages_for_test().unwrap();
    assert_eq!(facts.boundary_children(), 0);
    assert_eq!(
        facts.old_leaves() as u64,
        table.stats().unwrap().leaf_pages()
    );
    let tx = db.begin_write().unwrap();
    let after;
    {
        let mut t = tx.open_table(ROWS).unwrap();
        for (key, value) in keys.iter().zip(&values) {
            t.insert(key.as_slice(), value.as_slice()).unwrap();
        }
        after = t.stats().unwrap();
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(t.get(key.as_slice()).unwrap().unwrap().value(), value);
        }
    }
    let actually_freed = tx.data_freed_pages_for_point_test();
    tx.abort().unwrap();
    assert!(actually_freed.iter().all(|p| pages.contains(p)));
    assert!(after.leaf_pages() <= facts.future_leaf_upper() as u64);
    assert!(after.branch_pages() <= facts.future_branch_upper() as u64);
    let physical = after.stored_bytes() + after.metadata_bytes() + after.fragmented_bytes();
    assert!(physical <= facts.final_live_physical_upper());
    assert_eq!(table.len().unwrap(), 80);
}

#[test]
fn native_point_frontier_handles_actual_empty_str_root_and_checked_recipe_classes() {
    const META: TableDefinition<&str, &[u8]> = TableDefinition::new("point_frontier_meta");
    let (_file, db) = fixture(0);
    let tx = db.begin_write().unwrap();
    drop(tx.open_table(META).unwrap());
    tx.commit().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(META).unwrap();
    let operations = [
        M::Put {
            key: K::Str("a"),
            value: b"left",
        },
        M::Put {
            key: K::Str("z"),
            value: b"right",
        },
    ];
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let preparation = table.prepare_point_frontier(&recipe, &epoch).unwrap();
    assert_eq!(preparation.observed_fetches(), 0);
    let facts = preparation.collect().unwrap();
    assert_eq!(facts.old_pages(), 0);
    assert_eq!(facts.data_freed_entry_upper(), 0);
    assert_eq!(facts.future_leaf_upper(), 2);
    assert_eq!(facts.future_branch_upper(), 1);
    table.validate_point_frontier(&facts).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(META).unwrap();
        t.insert("a", b"left".as_slice()).unwrap();
        t.insert("z", b"right".as_slice()).unwrap();
        assert_eq!(t.len().unwrap(), 2);
    }
    tx.abort().unwrap();
    assert_eq!(table.len().unwrap(), 0);
    let mixed = [
        M::Delete { key: K::Str("a") },
        M::Put {
            key: K::Str("m"),
            value: b"v",
        },
        M::Put {
            key: K::Str("z"),
            value: b"v",
        },
    ];
    assert!(matches!(
        NativePointRecipe::compile(&mixed),
        Err(NativeKeyPathError::UnsupportedState)
    ));
    let duplicate = [
        M::Delete { key: K::Str("a") },
        M::Put {
            key: K::Str("a"),
            value: b"v",
        },
    ];
    assert!(matches!(
        NativePointRecipe::compile(&duplicate),
        Err(NativeKeyPathError::Malformed)
    ));
}

#[test]
fn native_point_frontier_refuses_foreign_and_advanced_epochs_before_record_allocation() {
    let (_f1, one) = fixture(32);
    let (_f2, two) = fixture(32);
    let read = one.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let key = 0_u64.to_be_bytes();
    let operations = [M::Delete {
        key: K::Bytes(&key),
    }];
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let wrong = two.capture_native_resource_epoch().unwrap();
    assert!(matches!(
        table.prepare_point_frontier(&recipe, &wrong),
        Err(NativeKeyPathError::Epoch(_))
    ));
    let epoch = one.capture_native_resource_epoch().unwrap();
    let preparation = table.prepare_point_frontier(&recipe, &epoch).unwrap();
    let tx = one.begin_write().unwrap();
    tx.abort().unwrap();
    assert!(matches!(
        preparation.collect(),
        Err(NativeKeyPathError::Epoch(_))
    ));
    assert_eq!(
        table.get(key.as_slice()).unwrap().unwrap().value(),
        [7_u8; 96]
    );
}

#[test]
fn native_point_frontier_exact_one_delete_one_put_preserves_unselected_values() {
    let (_file, db) = fixture(64);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let old = 63_u64.to_be_bytes();
    let new = 1000_u64.to_be_bytes();
    let operations = [
        M::Delete {
            key: K::Bytes(&old),
        },
        M::Put {
            key: K::Bytes(&new),
            value: b"replacement",
        },
    ];
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    assert_eq!(recipe.class(), NativePointRecipeClass::OneDeleteOnePut);
    let (facts, pages) = table
        .prepare_point_frontier(&recipe, &epoch)
        .unwrap()
        .collect_pages_for_test()
        .unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(ROWS).unwrap();
        assert!(t.remove(old.as_slice()).unwrap().is_some());
        assert!(
            t.insert(new.as_slice(), b"replacement".as_slice())
                .unwrap()
                .is_none()
        );
        assert_eq!(t.len().unwrap(), 64);
        assert_eq!(
            t.get(new.as_slice()).unwrap().unwrap().value(),
            b"replacement"
        );
    }
    let freed = tx.data_freed_pages_for_point_test();
    tx.abort().unwrap();
    assert!(freed.iter().all(|p| pages.contains(p)));
    assert!(freed.len() <= facts.old_pages());
    assert!(table.get(new.as_slice()).unwrap().is_none());
    assert_eq!(
        table.get(old.as_slice()).unwrap().unwrap().value(),
        [7_u8; 96]
    );
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_point_transient_heap_bounds_actual_sparse_delete_unwind_and_owned_conversion() {
    let (_file, db) = fixture(512);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let keys: Vec<_> = (0_u64..256).map(u64::to_be_bytes).collect();
    let operations: Vec<_> = keys
        .iter()
        .map(|k| M::Delete { key: K::Bytes(k) })
        .collect();
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let facts = table
        .prepare_point_frontier(&recipe, &epoch)
        .unwrap()
        .collect()
        .unwrap();
    let upper = facts.scratch_heap_upper();
    let pair_upper = facts.leaf_pair_capacity_upper();
    let child_upper = facts.branch_child_capacity_upper();
    let key_upper = facts.branch_key_capacity_upper();
    let tx = db.begin_write().unwrap();
    let trace = tx.point_trace_for_test();
    {
        let mut t = tx.open_table(ROWS).unwrap();
        for k in &keys {
            assert!(t.remove(k.as_slice()).unwrap().is_some());
        }
    }
    let observed = trace.snapshot();
    tx.abort().unwrap();
    // Release the real writer before checking observations. The immutable old
    // read still exposes all original data; no fixture deletes are committed.
    assert_eq!(observed.current_bytes, 0);
    assert_eq!(observed.leaf_live, 0);
    assert_eq!(observed.branch_live, 0);
    assert!(
        observed.conversions > 0,
        "genuine PartialBranch Cow-to-owned conversion required"
    );
    assert!(observed.partial_scopes > 0);
    assert!(
        observed.branch_peak >= 2,
        "real parent+child rebuild overlap required"
    );
    assert!(observed.branch_peak <= 2);
    assert!(observed.leaf_peak <= 1);
    assert!(observed.peak_bytes as u64 <= upper);
    assert!(observed.maximum_leaf_pairs <= pair_upper);
    assert!(observed.maximum_branch_children <= child_upper);
    assert!(observed.maximum_branch_keys <= key_upper);
    assert_eq!(table.len().unwrap(), 512);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_point_transient_heap_bounds_real_oversized_replacement_and_leaf_branch_split() {
    let (_file, db) = fixture(80);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let keys: Vec<_> = (0_u64..160).map(u64::to_be_bytes).collect();
    let values: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, _)| vec![i as u8; if i % 3 == 0 { 8192 } else { 96 }])
        .collect();
    let ops: Vec<_> = keys
        .iter()
        .zip(&values)
        .map(|(k, v)| M::Put {
            key: K::Bytes(k),
            value: v,
        })
        .collect();
    let recipe = NativePointRecipe::compile(&ops).unwrap();
    let facts = table
        .prepare_point_frontier(&recipe, &epoch)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(facts.maximum_value_bytes(), 8192);
    assert!(facts.maximum_old_leaf_bytes() >= 512);
    assert!(facts.maximum_old_branch_bytes() >= 512);
    assert!(facts.scratch_heap_upper() > facts.leaf_pair_capacity_upper() as u64);
    let tx = db.begin_write().unwrap();
    let trace = tx.point_trace_for_test();
    {
        let mut t = tx.open_table(ROWS).unwrap();
        for (k, v) in keys.iter().zip(&values) {
            drop(t.insert(k.as_slice(), v.as_slice()).unwrap());
        }
        assert!(t.stats().unwrap().leaf_pages() > facts.old_leaves() as u64);
    }
    let observed = trace.snapshot();
    tx.abort().unwrap();
    assert_eq!(observed.current_bytes, 0);
    assert!(observed.leaf_peak > 0 && observed.branch_peak > 0);
    assert!(observed.leaf_peak <= 1 && observed.branch_peak <= 2);
    assert!(observed.peak_bytes as u64 <= facts.scratch_heap_upper());
    assert!(observed.maximum_leaf_pairs <= facts.leaf_pair_capacity_upper());
    assert!(observed.maximum_branch_children <= facts.branch_child_capacity_upper());
    assert_eq!(table.len().unwrap(), 80);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_point_owned_epoch_facts_survive_read_teardown_and_refuse_wrong_table_and_new_epoch() {
    const OTHER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("other_point_rows");
    let (_file, db) = fixture(8);
    let tx = db.begin_write().unwrap();
    drop(tx.open_table(OTHER).unwrap());
    tx.commit().unwrap();
    let keys = [0_u64.to_be_bytes()];
    let ops = [M::Put {
        key: K::Bytes(&keys[0]),
        value: b"replacement",
    }];
    let recipe = NativePointRecipe::compile(&ops).unwrap();
    // The epoch is genuinely owned, not a reference to a temporary local.
    let facts = {
        let read = db.begin_read().unwrap();
        let table = read.open_table(ROWS).unwrap();
        let preparation = table
            .prepare_owned_point_frontier(&recipe, db.capture_native_resource_epoch().unwrap())
            .unwrap();
        assert!(preparation.record_capacity_bytes() > 0);
        preparation.collect().unwrap()
    };
    facts.require_current().unwrap();
    assert!(facts.old_pages() > 0 && facts.scratch_heap_upper() > 0);
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    table
        .validate_owned_point_frontier(&facts, &recipe)
        .unwrap();
    // Same root/name/read but a different exact native recipe cannot reuse the
    // earlier numerical quote, even if its shape/counts happen to be equal.
    let other_key = [M::Put {
        key: K::Bytes(b"different"),
        value: b"replacement",
    }];
    let other_key = NativePointRecipe::compile(&other_key).unwrap();
    assert!(
        table
            .validate_owned_point_frontier(&facts, &other_key)
            .is_err(),
        "same-root different native recipe was accepted"
    );
    let oversized = vec![9_u8; 8192];
    let other_value = [M::Put {
        key: K::Bytes(&keys[0]),
        value: &oversized,
    }];
    let other_value = NativePointRecipe::compile(&other_value).unwrap();
    assert!(
        table
            .validate_owned_point_frontier(&facts, &other_value)
            .is_err()
    );
    let other_class = [M::Delete {
        key: K::Bytes(&keys[0]),
    }];
    let other_class = NativePointRecipe::compile(&other_class).unwrap();
    assert!(
        table
            .validate_owned_point_frontier(&facts, &other_class)
            .is_err()
    );
    // Byte-identical operation envelopes are equivalent numerical input; this
    // doesn't manufacture read/write authority or bypass native epoch checks.
    let identical = NativePointRecipe::compile(&ops).unwrap();
    table
        .validate_owned_point_frontier(&facts, &identical)
        .unwrap();
    assert!(
        read.open_table(OTHER)
            .unwrap()
            .validate_owned_point_frontier(&facts, &recipe)
            .is_err()
    );
    let tx = db.begin_write().unwrap();
    tx.abort().unwrap();
    assert!(facts.require_current().is_err());
    assert!(
        table
            .validate_owned_point_frontier(&facts, &recipe)
            .is_err()
    );
    assert_eq!(table.len().unwrap(), 8);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_point_frontier_deduplicates_actual_same_parent_same_edge_overlap() {
    let (_file, db) = fixture(32);
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let keys = [0_u64.to_be_bytes(), 1_u64.to_be_bytes()];
    let operations: Vec<_> = keys
        .iter()
        .map(|key| M::Put {
            key: K::Bytes(key),
            value: b"replacement",
        })
        .collect();
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    let preparation = table.prepare_point_frontier(&recipe, &epoch).unwrap();
    let visits = preparation.observed_fetches();
    let (facts, pages) = preparation.collect_pages_for_test().unwrap();
    assert_eq!(
        facts.old_leaves(),
        1,
        "real keys must share one actual leaf"
    );
    assert!(visits > pages.len(), "same actual edge must be revisited");
    assert_eq!(facts.old_pages(), pages.len());
    facts.require_current().unwrap();
    assert_eq!(table.len().unwrap(), 32);
}

// req: STO-020, PRJ-004, REC-001
#[test]
fn native_point_frontier_rejects_actual_distinct_edge_page_alias_before_write() {
    let (_file, db) = fixture(32);
    let write = db.begin_write().unwrap();
    let second_edge_key;
    {
        let mut table = write.open_table(ROWS).unwrap();
        assert_eq!(
            table.stats().unwrap().tree_height(),
            2,
            "real root-to-leaf fixture"
        );
        second_edge_key = table
            .alias_first_root_child_for_point_frontier_test()
            .unwrap()
            .unwrap();
    }
    // This commits a deliberately malformed fixture through the real native
    // checksum/namespace owner. It is not asserted to be a valid native graph.
    write.commit().unwrap();
    let epoch = db.capture_native_resource_epoch().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let first = 0_u64.to_be_bytes();
    let operations = [
        M::Put {
            key: K::Bytes(&first),
            value: b"left",
        },
        M::Put {
            key: K::Bytes(&second_edge_key),
            value: b"right",
        },
    ];
    let recipe = NativePointRecipe::compile(&operations).unwrap();
    // The no-record pass uses the same normal parser/checksum validation; the
    // admitted census must additionally bind each page to its incoming edge.
    let preparation = table.prepare_point_frontier(&recipe, &epoch).unwrap();
    assert!(
        matches!(preparation.collect(), Err(NativeKeyPathError::Malformed)),
        "distinct native parent edge alias was accepted"
    );
    epoch.require_current().unwrap(); // No new write was opened by the census.
}
