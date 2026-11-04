//! Exact native codec equality controls; numerical fixtures grant no authority.
// req: STO-020, PRJ-004, REC-001
use super::*;
use crate::transaction_tracker::SavepointId;
use crate::transactions::{AllocatorStateKey, PageList, TransactionIdWithPagination};
use crate::tree_store::SerializedSavepoint;
use crate::types::NativeResourceTypeName;
fn check<K: Key + NativeResourceTypeName, V: Value + NativeResourceTypeName>() {
    for root in [
        None,
        Some(BtreeHeader::new(
            PageNumber::new(0, 0, 0),
            u128::MAX,
            u64::MAX,
        )),
    ] {
        for length in [0, u64::MAX] {
            let definition = InternalTableDefinition::new::<K, V>(TableType::Normal, root, length);
            let bytes = InternalTableDefinition::as_bytes(&definition);
            assert_eq!(
                InternalTableDefinition::native_resource_encoded_len::<K, V>().unwrap(),
                bytes.len()
            );
            assert!(InternalTableDefinition::native_resource_matches::<K, V>(&bytes).unwrap());
            let round = InternalTableDefinition::from_bytes(&bytes);
            assert_eq!(round, definition);
            assert_eq!(InternalTableDefinition::as_bytes(&round), bytes);
        }
    }
}
fn check_union(root: Option<BtreeHeader>, length: u64) {
    use crate::transactions::NativeSystemFamily;
    use crate::tree_store::{LeafAccessor, RawLeafBuilder};
    let mut pairs = NativeSystemFamily::all().map(|family| {
        let definition = match family {
            NativeSystemFamily::NextSavepoint => {
                InternalTableDefinition::new::<(), SavepointId>(TableType::Normal, root, length)
            }
            NativeSystemFamily::Savepoints => InternalTableDefinition::new::<
                SavepointId,
                SerializedSavepoint<'static>,
            >(TableType::Normal, root, length),
            NativeSystemFamily::DataAllocated
            | NativeSystemFamily::DataFreed
            | NativeSystemFamily::SystemFreed => InternalTableDefinition::new::<
                TransactionIdWithPagination,
                PageList<'static>,
            >(TableType::Normal, root, length),
            NativeSystemFamily::Allocator => InternalTableDefinition::new::<
                AllocatorStateKey,
                &'static [u8],
            >(TableType::Normal, root, length),
        };
        let encoded = InternalTableDefinition::as_bytes(&definition);
        assert_eq!(family.encoded_definition_bytes().unwrap(), encoded.len());
        assert!(family.matches_definition(&encoded).unwrap());
        (family.name(), encoded)
    });
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let key_bytes = pairs.iter().map(|(key, _)| key.len()).sum::<usize>();
    let actual_pair_bytes = pairs
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>();
    let quoted_pair_bytes = NativeSystemFamily::all()
        .into_iter()
        .map(|family| family.name().len() + family.encoded_definition_bytes().unwrap())
        .sum::<usize>();
    assert_eq!(actual_pair_bytes, quoted_pair_bytes);
    let quoted = RawLeafBuilder::required_bytes(pairs.len(), quoted_pair_bytes, None, None);
    let mut encoded_leaf = vec![0; quoted];
    {
        let mut leaf = RawLeafBuilder::new(&mut encoded_leaf, pairs.len(), None, None, key_bytes);
        for (key, value) in &pairs {
            leaf.append(key.as_bytes(), value);
        }
    }
    let leaf = LeafAccessor::new(&encoded_leaf, None, None);
    assert_eq!(leaf.num_pairs(), 6);
    assert_eq!(leaf.total_length(), quoted);
}
#[test]
fn native_system_definition_quote_equals_real_encoder_at_all_scalar_extremes() {
    for root in [
        None,
        Some(BtreeHeader::new(
            PageNumber::new(0, 0, 0),
            u128::MAX,
            u64::MAX,
        )),
    ] {
        for length in [0, u64::MAX] {
            check_union(root, length);
        }
    }
    check::<(), SavepointId>();
    check::<SavepointId, SerializedSavepoint<'static>>();
    check::<TransactionIdWithPagination, PageList<'static>>();
    check::<AllocatorStateKey, &'static [u8]>();
    let definition = InternalTableDefinition::new::<AllocatorStateKey, &'static [u8]>(
        TableType::Normal,
        Some(BtreeHeader::new(
            PageNumber::new(0, 0, 0),
            u128::MAX,
            u64::MAX,
        )),
        u64::MAX,
    );
    let mut bytes = InternalTableDefinition::as_bytes(&definition);
    let root_flag = 1 + size_of::<u64>();
    let key_flag = root_flag + 1 + BtreeHeader::serialized_size();
    let value_flag = key_flag + 1 + size_of::<u32>();
    // Existing native decoder semantics are nonzero=>Some; ignored None-width
    // padding remains ignored. The shared parser must not silently change them.
    bytes[root_flag] = 255;
    bytes[key_flag] = 2;
    bytes[value_flag + 1..value_flag + 1 + size_of::<u32>()]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(InternalTableDefinition::from_bytes(&bytes), definition);
    assert!(
        InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(
            &bytes
        )
        .unwrap()
    );
}
#[test]
fn native_system_definition_shared_decoder_preserves_malformed_and_schema_mismatch() {
    let definition = InternalTableDefinition::new::<AllocatorStateKey, &'static [u8]>(
        TableType::Normal,
        None,
        0,
    );
    let encoded = InternalTableDefinition::as_bytes(&definition);
    for end in 0..64 {
        assert!(matches!(
            InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(
                &encoded[..end]
            ),
            Err(NativeDefinitionResourceError::Malformed)
        ));
    }
    for tag in [0, 1, 2, 5, 255] {
        let mut bad = encoded.clone();
        bad[0] = tag;
        assert!(matches!(
            InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(
                &bad
            ),
            Err(NativeDefinitionResourceError::Malformed)
        ));
        assert!(std::panic::catch_unwind(|| InternalTableDefinition::from_bytes(&bad)).is_err());
    }
    let fixed = 1
        + size_of::<u64>()
        + 1
        + BtreeHeader::serialized_size()
        + 2 * (1 + size_of::<u32>())
        + 3 * size_of::<u32>();
    for class in [0, 5, 255] {
        let mut bad = encoded.clone();
        bad[fixed] = class;
        assert!(matches!(
            InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(
                &bad
            ),
            Err(NativeDefinitionResourceError::Malformed)
        ));
        assert!(std::panic::catch_unwind(|| InternalTableDefinition::from_bytes(&bad)).is_err());
    }
    let mut bad = encoded.clone();
    bad[fixed - size_of::<u32>()..fixed].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(&bad),
        Err(NativeDefinitionResourceError::Malformed)
    ));
    let mut bad = encoded.clone();
    *bad.last_mut().unwrap() = 255;
    assert!(matches!(
        InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(&bad),
        Err(NativeDefinitionResourceError::Malformed)
    ));
    // A valid foreign classification is a schema mismatch, not missing data.
    let mut foreign = encoded.clone();
    foreign[fixed] = 2;
    assert!(
        !InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(
            &foreign
        )
        .unwrap()
    );
    assert_eq!(
        InternalTableDefinition::as_bytes(&InternalTableDefinition::from_bytes(&foreign)),
        foreign
    );
    // The ordinary native compatibility predicate still accepts a real legacy
    // composite classification. Numerical SYSTEM schema matching deliberately
    // supports only the seven flat built-in types of the current six families;
    // it is not a replacement for check_match's wider compatibility contract.
    let composite =
        InternalTableDefinition::new::<Option<u32>, &'static [u8]>(TableType::Normal, None, 0);
    let current = InternalTableDefinition::as_bytes(&composite);
    assert_eq!(current[fixed], 4); // Current Internal3 composite classification.
    let current_decoded = InternalTableDefinition::from_bytes(&current);
    current_decoded
        .check_match::<Option<u32>, &'static [u8]>(TableType::Normal, "current")
        .unwrap();
    let mut legacy = current;
    legacy[fixed] = 1; // Genuine old Internal spelling.
    let legacy_decoded = InternalTableDefinition::from_bytes(&legacy);
    legacy_decoded
        .check_match::<Option<u32>, &'static [u8]>(TableType::Normal, "legacy")
        .unwrap();
    assert_eq!(InternalTableDefinition::as_bytes(&legacy_decoded), legacy);
    let multimap = InternalTableDefinition::new::<AllocatorStateKey, &'static [u8]>(
        TableType::Multimap,
        None,
        0,
    );
    assert!(
        !InternalTableDefinition::native_resource_matches::<AllocatorStateKey, &'static [u8]>(
            &InternalTableDefinition::as_bytes(&multimap)
        )
        .unwrap()
    );
}
