//! Complete one-leaf SYSTEM numerical profile under actual inspection exclusion.
use super::resource_key_paths::{NativeKeyPathError, check_layout};
use super::{Btree, DEFERRED, LEAF, LeafAccessor, Page, leaf_checksum};
use crate::transaction_tracker::NativeResourceEpoch;
use crate::transactions::NativeSystemFamily;
use crate::tree_store::{InternalTableDefinition, NativeDefinitionResourceError, RawLeafBuilder};
type ProfileResult<T> = Result<T, NativeKeyPathError>;
fn map_definition(error: NativeDefinitionResourceError) -> NativeKeyPathError {
    match error {
        NativeDefinitionResourceError::Malformed => NativeKeyPathError::Malformed,
        NativeDefinitionResourceError::Overflow => NativeKeyPathError::Overflow,
    }
}

// Closed inline numerical facts: only the actual inspected namespace below
// can mint them. No page, epoch, tracker, transaction, or ownership alias remains.
pub(crate) struct NativeSystemNamespaceFacts {
    actual_definitions: usize,
    actual_page_bytes: usize,
    prospective_union_bytes: usize,
    base_page_bytes: usize,
}
impl NativeSystemNamespaceFacts {
    pub(crate) fn actual_definitions(&self) -> usize {
        self.actual_definitions
    }
    pub(crate) fn actual_page_bytes(&self) -> usize {
        self.actual_page_bytes
    }
    pub(crate) fn prospective_union_bytes(&self) -> usize {
        self.prospective_union_bytes
    }
    pub(crate) fn base_page_bytes(&self) -> usize {
        self.base_page_bytes
    }
}

pub(crate) struct NativeSystemNamespaceProfile<'tree, 'epoch, 'tracker> {
    actual: &'tree Btree<&'static str, InternalTableDefinition>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    actual_definitions: usize,
    actual_page_bytes: usize,
    prospective_union_bytes: usize,
    base_page_bytes: usize,
}
impl NativeSystemNamespaceProfile<'_, '_, '_> {
    pub(crate) fn into_inline_facts(self) -> ProfileResult<NativeSystemNamespaceFacts> {
        self.require_current()?;
        Ok(NativeSystemNamespaceFacts {
            actual_definitions: self.actual_definitions,
            actual_page_bytes: self.actual_page_bytes,
            prospective_union_bytes: self.prospective_union_bytes,
            base_page_bytes: self.base_page_bytes,
        })
    }

    pub(crate) fn require_current(&self) -> ProfileResult<()> {
        self.actual.require_system_namespace_inspection(self.epoch)
    }
    pub(crate) fn actual_definitions(&self) -> usize {
        self.actual_definitions
    }
    pub(crate) fn actual_page_bytes(&self) -> usize {
        self.actual_page_bytes
    }
    pub(crate) fn prospective_union_bytes(&self) -> usize {
        self.prospective_union_bytes
    }
    pub(crate) fn base_page_bytes(&self) -> usize {
        self.base_page_bytes
    }
}
impl Drop for NativeSystemNamespaceProfile<'_, '_, '_> {
    fn drop(&mut self) {}
}
impl Btree<&'static str, InternalTableDefinition> {
    fn require_system_namespace_inspection(
        &self,
        epoch: &NativeResourceEpoch<'_>,
    ) -> ProfileResult<()> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        epoch
            .require_system_inspection_for(parts.actual_tracker())
            .map_err(NativeKeyPathError::Epoch)?;
        self.require_current_native_resource_read()
    }
    pub(crate) fn inspect_system_namespace_profile<'tree, 'epoch, 'tracker>(
        &'tree self,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
    ) -> ProfileResult<NativeSystemNamespaceProfile<'tree, 'epoch, 'tracker>> {
        self.require_system_namespace_inspection(epoch)?;
        let families = NativeSystemFamily::all();
        let mut pair_bytes = 0usize;
        for family in families {
            let definition = family.encoded_definition_bytes().map_err(map_definition)?;
            pair_bytes = pair_bytes
                .checked_add(family.name().len())
                .and_then(|n| n.checked_add(definition))
                .ok_or(NativeKeyPathError::Overflow)?;
        }
        // Prove the native builder's additions cannot overflow BEFORE calling
        // the same real leaf-layout owner. All six prospective schemas count,
        // including currently absent families; no type/definition is encoded.
        pair_bytes
            .checked_add(4 + 2 * families.len() * core::mem::size_of::<u32>())
            .ok_or(NativeKeyPathError::Overflow)?;
        let prospective_union_bytes =
            RawLeafBuilder::required_bytes(families.len(), pair_bytes, None, None);
        let base_page_bytes = self.mem.resource_base_page_size();
        if prospective_union_bytes > base_page_bytes {
            return Err(NativeKeyPathError::UnsupportedState);
        }
        let mut actual_definitions = 0usize;
        let mut actual_page_bytes = 0usize;
        if let Some(header) = self.root {
            if header.checksum == DEFERRED {
                return Err(NativeKeyPathError::UnsupportedState);
            }
            let page = self
                .cached_root
                .as_ref()
                .ok_or(NativeKeyPathError::Malformed)?;
            if page.get_page_number() != header.root {
                return Err(NativeKeyPathError::ForeignOwner);
            }
            if page.memory().first() != Some(&LEAF) {
                return Err(NativeKeyPathError::UnsupportedState);
            }
            let count = page
                .memory()
                .get(2..4)
                .ok_or(NativeKeyPathError::Malformed)?;
            actual_definitions = usize::from(u16::from_le_bytes([count[0], count[1]]));
            if actual_definitions > families.len() {
                return Err(NativeKeyPathError::UnsupportedState);
            }
            if header.length != actual_definitions as u64 {
                return Err(NativeKeyPathError::Malformed);
            }
            check_layout::<&'static str, InternalTableDefinition>(page)?;
            if leaf_checksum(page, None, None)? != header.checksum {
                return Err(NativeKeyPathError::Malformed);
            }
            actual_page_bytes = page.memory().len();
            let leaf = LeafAccessor::new(page.memory(), None, None);
            let mut prior = None::<&[u8]>;
            let mut seen = 0u8;
            for index in 0..actual_definitions {
                let (key, value) = leaf
                    .entry_ranges(index)
                    .ok_or(NativeKeyPathError::Malformed)?;
                let key = &page.memory()[key];
                if prior.is_some_and(|previous| previous >= key) {
                    return Err(NativeKeyPathError::Malformed);
                }
                prior = Some(key);
                let family = NativeSystemFamily::for_name(key)
                    .ok_or(NativeKeyPathError::UnsupportedState)?;
                let flag = 1u8 << family.index();
                if seen & flag != 0 {
                    return Err(NativeKeyPathError::Malformed);
                }
                seen |= flag;
                if !family
                    .matches_definition(&page.memory()[value])
                    .map_err(map_definition)?
                {
                    return Err(NativeKeyPathError::UnsupportedState);
                }
            }
        }
        self.require_system_namespace_inspection(epoch)?;
        Ok(NativeSystemNamespaceProfile {
            actual: self,
            epoch,
            actual_definitions,
            actual_page_bytes,
            prospective_union_bytes,
            base_page_bytes,
        })
    }
}
