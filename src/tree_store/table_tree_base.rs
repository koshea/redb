use crate::sync::Mutex;
use crate::tree_store::NativeOwnedPageVec;
use crate::tree_store::NativeOwnedShared;
use crate::tree_store::btree::{PagePath, UntypedBtree, UntypedBtreeMut};
use crate::tree_store::multimap_btree::{UntypedMultiBtree, relocate_subtrees};
use crate::tree_store::{
    BtreeHeader, PageAllocator, PageHint, PageNumber, PageNumberHashMap, PageResolver,
};
use crate::{Key, Result, TableError, TypeName, Value};
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::mem::size_of;

// Forward compatibility feature in case alignment can be supported in the future
// See https://github.com/cberner/redb/issues/360
const ALIGNMENT: usize = 1;

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) enum TableType {
    Normal,
    Multimap,
}

impl TableType {
    fn is_legacy(value: u8) -> bool {
        value == 1 || value == 2
    }
    fn try_from_current_byte(value: u8) -> Option<Self> {
        match value {
            3 => Some(Self::Normal),
            4 => Some(Self::Multimap),
            _ => None,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<u8> for TableType {
    fn into(self) -> u8 {
        match self {
            // 1 & 2 were used in the v1 file format
            // TableType::Normal => 1,
            // TableType::Multimap => 2,
            TableType::Normal => 3,
            TableType::Multimap => 4,
        }
    }
}

impl From<u8> for TableType {
    fn from(value: u8) -> Self {
        Self::try_from_current_byte(value).expect("Unknown native table type")
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum InternalTableDefinition {
    Normal {
        table_root: Option<BtreeHeader>,
        table_length: u64,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        key_alignment: usize,
        value_alignment: usize,
        key_type: TypeName,
        value_type: TypeName,
    },
    Multimap {
        table_root: Option<BtreeHeader>,
        table_length: u64,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        key_alignment: usize,
        value_alignment: usize,
        key_type: TypeName,
        value_type: TypeName,
    },
}

impl InternalTableDefinition {
    pub(super) fn new<K: Key, V: Value>(
        table_type: TableType,
        table_root: Option<BtreeHeader>,
        table_length: u64,
    ) -> Self {
        match table_type {
            TableType::Normal => InternalTableDefinition::Normal {
                table_root,
                table_length,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                key_alignment: ALIGNMENT,
                value_alignment: ALIGNMENT,
                key_type: K::type_name(),
                value_type: V::type_name(),
            },
            TableType::Multimap => InternalTableDefinition::Multimap {
                table_root,
                table_length,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                key_alignment: ALIGNMENT,
                value_alignment: ALIGNMENT,
                key_type: K::type_name(),
                value_type: V::type_name(),
            },
        }
    }

    pub(super) fn set_header(&mut self, root: Option<BtreeHeader>, length: u64) {
        match self {
            InternalTableDefinition::Normal {
                table_root,
                table_length,
                ..
            }
            | InternalTableDefinition::Multimap {
                table_root,
                table_length,
                ..
            } => {
                *table_root = root;
                *table_length = length;
            }
        }
    }

    pub(super) fn check_match_untyped(
        &self,
        table_type: TableType,
        name: &str,
    ) -> Result<(), TableError> {
        if self.get_type() != table_type {
            return if self.get_type() == TableType::Multimap {
                Err(TableError::TableIsMultimap(name.to_string()))
            } else {
                Err(TableError::TableIsNotMultimap(name.to_string()))
            };
        }
        if self.private_get_key_alignment() != ALIGNMENT {
            return Err(TableError::TypeDefinitionChanged {
                name: self.private_key_type(),
                alignment: self.private_get_key_alignment(),
                width: self.private_get_fixed_key_size(),
            });
        }
        if self.private_get_value_alignment() != ALIGNMENT {
            return Err(TableError::TypeDefinitionChanged {
                name: self.private_value_type(),
                alignment: self.private_get_value_alignment(),
                width: self.private_get_fixed_value_size(),
            });
        }

        Ok(())
    }

    pub(super) fn check_match<K: Key, V: Value>(
        &self,
        table_type: TableType,
        name: &str,
    ) -> Result<(), TableError> {
        self.check_match_untyped(table_type, name)?;

        let stored_key = self.private_key_type();
        let stored_value = self.private_value_type();
        // Accept the current type name, or the spelling an older redb version may have stored for
        // the same type (see `TypeName::matches_legacy`).
        let expected_key = K::type_name();
        let expected_value = V::type_name();
        let key_matches = stored_key == expected_key || expected_key.matches_legacy(&stored_key);
        let value_matches =
            stored_value == expected_value || expected_value.matches_legacy(&stored_value);
        if !key_matches || !value_matches {
            return Err(TableError::TableTypeMismatch {
                table: name.to_string(),
                key: stored_key,
                value: stored_value,
            });
        }
        if self.private_get_fixed_key_size() != K::fixed_width() {
            return Err(TableError::TypeDefinitionChanged {
                name: K::type_name(),
                alignment: self.private_get_key_alignment(),
                width: self.private_get_fixed_key_size(),
            });
        }
        if self.private_get_fixed_value_size() != V::fixed_width() {
            return Err(TableError::TypeDefinitionChanged {
                name: V::type_name(),
                alignment: self.private_get_value_alignment(),
                width: self.private_get_fixed_value_size(),
            });
        }

        Ok(())
    }

    pub(crate) fn visit_all_pages<'a, F>(
        &self,
        mem: PageResolver,
        hint: PageHint,
        visitor: F,
    ) -> Result
    where
        F: FnMut(&PagePath) -> Result + 'a,
    {
        match self {
            InternalTableDefinition::Normal {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                let tree =
                    UntypedBtree::new(*table_root, mem, hint, *fixed_key_size, *fixed_value_size);
                tree.visit_all_pages(visitor)?;
            }
            InternalTableDefinition::Multimap {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                let tree = UntypedMultiBtree::new(
                    *table_root,
                    mem,
                    hint,
                    *fixed_key_size,
                    *fixed_value_size,
                );
                tree.visit_all_pages(visitor)?;
            }
        }

        Ok(())
    }

    pub(crate) fn relocate_tree(
        &mut self,
        page_allocator: PageAllocator,
        freed_pages: NativeOwnedShared<Mutex<NativeOwnedPageVec>>,
        relocation_map: &PageNumberHashMap<PageNumber>,
    ) -> Result<Option<BtreeHeader>> {
        let original_root = self.private_get_root();
        let relocated_root = match self {
            InternalTableDefinition::Normal { table_root, .. } => *table_root,
            InternalTableDefinition::Multimap {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                if let Some(header) = table_root {
                    let (page_number, checksum) = relocate_subtrees(
                        (header.root, header.checksum),
                        *fixed_key_size,
                        *fixed_value_size,
                        page_allocator.clone(),
                        freed_pages.clone(),
                        relocation_map,
                    )?;
                    Some(BtreeHeader::new(page_number, checksum, header.length))
                } else {
                    None
                }
            }
        };
        let mut tree = UntypedBtreeMut::new(
            relocated_root,
            page_allocator,
            freed_pages,
            self.private_get_fixed_key_size(),
            self.private_get_fixed_value_size(),
        );
        tree.relocate(relocation_map)?;
        if tree.get_root() != original_root {
            self.set_header(tree.get_root(), self.get_length());
            Ok(tree.get_root())
        } else {
            Ok(None)
        }
    }

    fn private_get_root(&self) -> Option<BtreeHeader> {
        match self {
            InternalTableDefinition::Normal { table_root, .. }
            | InternalTableDefinition::Multimap { table_root, .. } => *table_root,
        }
    }

    pub(crate) fn get_length(&self) -> u64 {
        match self {
            InternalTableDefinition::Normal { table_length, .. }
            | InternalTableDefinition::Multimap { table_length, .. } => *table_length,
        }
    }

    fn private_get_fixed_key_size(&self) -> Option<usize> {
        match self {
            InternalTableDefinition::Normal { fixed_key_size, .. }
            | InternalTableDefinition::Multimap { fixed_key_size, .. } => *fixed_key_size,
        }
    }

    fn private_get_fixed_value_size(&self) -> Option<usize> {
        match self {
            InternalTableDefinition::Normal {
                fixed_value_size, ..
            }
            | InternalTableDefinition::Multimap {
                fixed_value_size, ..
            } => *fixed_value_size,
        }
    }

    fn private_get_key_alignment(&self) -> usize {
        match self {
            InternalTableDefinition::Normal { key_alignment, .. }
            | InternalTableDefinition::Multimap { key_alignment, .. } => *key_alignment,
        }
    }

    fn private_get_value_alignment(&self) -> usize {
        match self {
            InternalTableDefinition::Normal {
                value_alignment, ..
            }
            | InternalTableDefinition::Multimap {
                value_alignment, ..
            } => *value_alignment,
        }
    }

    pub(crate) fn get_type(&self) -> TableType {
        match self {
            InternalTableDefinition::Normal { .. } => TableType::Normal,
            InternalTableDefinition::Multimap { .. } => TableType::Multimap,
        }
    }

    fn private_key_type(&self) -> TypeName {
        match self {
            InternalTableDefinition::Normal { key_type, .. }
            | InternalTableDefinition::Multimap { key_type, .. } => key_type.clone(),
        }
    }

    fn private_value_type(&self) -> TypeName {
        match self {
            InternalTableDefinition::Normal { value_type, .. }
            | InternalTableDefinition::Multimap { value_type, .. } => value_type.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum NativeDefinitionResourceError {
    Malformed,
    Overflow,
}

struct BorrowedNativeDefinition<'bytes> {
    table_type: TableType,
    table_root: Option<BtreeHeader>,
    table_length: u64,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    key_alignment: usize,
    value_alignment: usize,
    key_type: &'bytes [u8],
    value_type: &'bytes [u8],
}
struct NativeDefinitionReader<'bytes> {
    remaining: &'bytes [u8],
}
impl<'bytes> NativeDefinitionReader<'bytes> {
    fn take(
        &mut self,
        count: usize,
    ) -> core::result::Result<&'bytes [u8], NativeDefinitionResourceError> {
        let part = self
            .remaining
            .get(..count)
            .ok_or(NativeDefinitionResourceError::Malformed)?;
        self.remaining = &self.remaining[count..];
        Ok(part)
    }
    fn bytes<const N: usize>(
        &mut self,
    ) -> core::result::Result<[u8; N], NativeDefinitionResourceError> {
        self.take(N)?
            .try_into()
            .map_err(|_| NativeDefinitionResourceError::Malformed)
    }
    fn number(&mut self) -> core::result::Result<usize, NativeDefinitionResourceError> {
        // Preserve the existing native decoder's u32-to-usize semantics.
        Ok(u32::from_le_bytes(self.bytes()?) as usize)
    }
    fn optional_width(
        &mut self,
    ) -> core::result::Result<Option<usize>, NativeDefinitionResourceError> {
        let non_null = self.bytes::<1>()?[0] != 0;
        let width = self.number()?;
        Ok(non_null.then_some(width))
    }
}
impl InternalTableDefinition {
    // ONE shared native field parser for the normal owned decoder and resource
    // observation. No TypeName/String/Vec allocation precedes complete framing.
    fn borrowed_native_parts(
        data: &[u8],
    ) -> core::result::Result<BorrowedNativeDefinition<'_>, NativeDefinitionResourceError> {
        let mut reader = NativeDefinitionReader { remaining: data };
        let tag = reader.bytes::<1>()?[0];
        if TableType::is_legacy(tag) {
            return Err(NativeDefinitionResourceError::Malformed);
        }
        let table_type = TableType::try_from_current_byte(tag)
            .ok_or(NativeDefinitionResourceError::Malformed)?;
        let table_length = u64::from_le_bytes(reader.bytes()?);
        let non_null = reader.bytes::<1>()?[0] != 0;
        let root = reader.bytes::<{ BtreeHeader::serialized_size() }>()?;
        let table_root = if non_null {
            Some(BtreeHeader::from_le_bytes(root))
        } else {
            None
        };
        let fixed_key_size = reader.optional_width()?;
        let fixed_value_size = reader.optional_width()?;
        let key_alignment = reader.number()?;
        let value_alignment = reader.number()?;
        let key_type_len = reader.number()?;
        let key_type = reader.take(key_type_len)?;
        let value_type = reader.remaining;
        if !TypeName::valid_native_encoding(key_type)
            || !TypeName::valid_native_encoding(value_type)
        {
            return Err(NativeDefinitionResourceError::Malformed);
        }
        Ok(BorrowedNativeDefinition {
            table_type,
            table_root,
            table_length,
            fixed_key_size,
            fixed_value_size,
            key_alignment,
            value_alignment,
            key_type,
            value_type,
        })
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepaid_decode_work_bytes(
        data: &[u8],
        name: &str,
        expected_key: usize,
        expected_value: usize,
    ) -> crate::Result<usize> {
        use crate::tree_store::page_store::{
            point_vector_bytes as vector, source_owner_refused as refused,
        };
        let parsed = Self::borrowed_native_parts(data).map_err(|_| {
            crate::StorageError::Corrupted("Malformed native table definition".to_string())
        })?;
        let key = TypeName::decoded_native_name_len(parsed.key_type).ok_or_else(refused)?;
        let value = TypeName::decoded_native_name_len(parsed.value_type).ok_or_else(refused)?;
        let mut bytes = 0u64;
        // Actual definition's two owned names plus both check_match stored clones.
        // Both expected names stay live during comparison; one additional expected
        // name may be constructed in TypeDefinitionChanged before those locals drop.
        for len in [
            key,
            value,
            key,
            value,
            expected_key,
            expected_value,
            expected_key.max(expected_value),
            name.len(),
        ] {
            bytes = bytes
                .checked_add(vector::<u8>(len).map_err(|_| refused())?)
                .ok_or_else(refused)?;
        }
        let u = usize::MAX.ilog10() as usize + 1;
        let width = "Some("
            .len()
            .checked_add(u)
            .and_then(|n| n.checked_add(")".len()))
            .ok_or_else(refused)?;
        let changed =
            "Current definition of  does not match stored definition (width=, alignment=)"
                .len()
                .checked_add(key.max(value).max(expected_key).max(expected_value))
                .and_then(|n| n.checked_add(width))
                .and_then(|n| n.checked_add(u))
                .ok_or_else(refused)?;
        let mismatch = " is of type Table<, >"
            .len()
            .checked_add(name.len())
            .and_then(|n| n.checked_add(key))
            .and_then(|n| n.checked_add(value))
            .ok_or_else(refused)?;
        let kind = name
            .len()
            .checked_add(" is not a multimap table".len())
            .ok_or_else(refused)?;
        let output = "Unexpected TableError: "
            .len()
            .checked_add(changed.max(mismatch).max(kind))
            .ok_or_else(refused)?;
        // Conversion formats the original typed error while its names are still live.
        bytes = bytes
            .checked_add(crate::tree_store::page_store::native_cold_format_work_bytes(output)?)
            .ok_or_else(refused)?;
        usize::try_from(bytes).map_err(|_| refused())
    }
    pub(crate) fn native_resource_encoded_len<
        K: Key + crate::types::NativeResourceTypeName,
        V: Value + crate::types::NativeResourceTypeName,
    >() -> core::result::Result<usize, NativeDefinitionResourceError> {
        // SAME fixed-width fields as as_bytes below; root/length/option contents
        // never alter the stored definition length (existing native invariant).
        let fixed = 1
            + size_of::<u64>()
            + 1
            + BtreeHeader::serialized_size()
            + 2 * (1 + size_of::<u32>())
            + 3 * size_of::<u32>();
        let key = TypeName::native_resource_encoded_len::<K>()
            .map_err(|_| NativeDefinitionResourceError::Overflow)?;
        let value = TypeName::native_resource_encoded_len::<V>()
            .map_err(|_| NativeDefinitionResourceError::Overflow)?;
        u32::try_from(key).map_err(|_| NativeDefinitionResourceError::Overflow)?;
        fixed
            .checked_add(key)
            .and_then(|n| n.checked_add(value))
            .ok_or(NativeDefinitionResourceError::Overflow)
    }
    pub(crate) fn native_resource_matches<
        K: Key + crate::types::NativeResourceTypeName,
        V: Value + crate::types::NativeResourceTypeName,
    >(
        data: &[u8],
    ) -> core::result::Result<bool, NativeDefinitionResourceError> {
        let parsed = Self::borrowed_native_parts(data)?;
        let name = |result| match result {
            Ok(value) => Ok(value),
            Err(crate::types::NativeResourceNameError::Malformed) => {
                Err(NativeDefinitionResourceError::Malformed)
            }
            Err(crate::types::NativeResourceNameError::Overflow) => {
                Err(NativeDefinitionResourceError::Overflow)
            }
        };
        Ok(parsed.table_type == TableType::Normal
            && parsed.fixed_key_size == K::fixed_width()
            && parsed.fixed_value_size == V::fixed_width()
            && parsed.key_alignment == ALIGNMENT
            && parsed.value_alignment == ALIGNMENT
            && name(TypeName::native_resource_matches::<K>(parsed.key_type))?
            && name(TypeName::native_resource_matches::<V>(parsed.value_type))?)
    }
}

impl Value for InternalTableDefinition {
    type SelfType<'a> = InternalTableDefinition;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        let parsed = Self::borrowed_native_parts(data).expect("Malformed native table definition");
        let key_type = TypeName::from_bytes(parsed.key_type);
        let value_type = TypeName::from_bytes(parsed.value_type);
        match parsed.table_type {
            TableType::Normal => InternalTableDefinition::Normal {
                table_root: parsed.table_root,
                table_length: parsed.table_length,
                fixed_key_size: parsed.fixed_key_size,
                fixed_value_size: parsed.fixed_value_size,
                key_alignment: parsed.key_alignment,
                value_alignment: parsed.value_alignment,
                key_type,
                value_type,
            },
            TableType::Multimap => InternalTableDefinition::Multimap {
                table_root: parsed.table_root,
                table_length: parsed.table_length,
                fixed_key_size: parsed.fixed_key_size,
                fixed_value_size: parsed.fixed_value_size,
                key_alignment: parsed.key_alignment,
                value_alignment: parsed.value_alignment,
                key_type,
                value_type,
            },
        }
    }

    // Be careful if you change this serialization format! The InternalTableDefinition for
    // a given table needs to have a consistent serialized length, regardless of the table
    // contents, so that create_table_and_flush_table_root() can update the allocator state
    // table without causing more allocations
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = vec![value.get_type().into()];
        result.extend_from_slice(&value.get_length().to_le_bytes());
        if let Some(header) = value.private_get_root() {
            result.push(1);
            result.extend_from_slice(&header.to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; BtreeHeader::serialized_size()]);
        }
        if let Some(fixed) = value.private_get_fixed_key_size() {
            result.push(1);
            result.extend_from_slice(&u32::try_from(fixed).unwrap().to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()]);
        }
        if let Some(fixed) = value.private_get_fixed_value_size() {
            result.push(1);
            result.extend_from_slice(&u32::try_from(fixed).unwrap().to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()]);
        }
        result.extend_from_slice(
            &u32::try_from(value.private_get_key_alignment())
                .unwrap()
                .to_le_bytes(),
        );
        result.extend_from_slice(
            &u32::try_from(value.private_get_value_alignment())
                .unwrap()
                .to_le_bytes(),
        );
        let key_type_bytes = value.private_key_type().to_bytes();
        result.extend_from_slice(&u32::try_from(key_type_bytes.len()).unwrap().to_le_bytes());
        result.extend_from_slice(&key_type_bytes);
        result.extend_from_slice(&value.private_value_type().to_bytes());

        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::InternalTableDefinition")
    }
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "native_definition_resource_tests.rs"]
mod native_resource_tests;
