//! Inline numerical observations from the actual selected freed-entry prefix.
//! No raw-count constructor, heap, SYSTEM handle or mutation authority.
use super::{NativeKeyPathError, PathResult};

#[derive(Default)]
pub(crate) struct NativeSelectedFreedFamilyFacts {
    records: usize,
    page_numbers: usize,
    canonical_kv_bytes: usize,
    weighted_pages: u64,
    maximum_record_page_numbers: usize,
    maximum_record_kv_bytes: usize,
}
impl NativeSelectedFreedFamilyFacts {
    pub(crate) fn maximum_record_page_numbers(&self) -> usize {
        self.maximum_record_page_numbers
    }
    pub(crate) fn maximum_record_kv_bytes(&self) -> usize {
        self.maximum_record_kv_bytes
    }
    pub(crate) fn records(&self) -> usize {
        self.records
    }
    pub(crate) fn page_numbers(&self) -> usize {
        self.page_numbers
    }
    pub(crate) fn canonical_kv_bytes(&self) -> usize {
        self.canonical_kv_bytes
    }
    /// Sum of 2^order in actual selected PageNumbers; base-page bytes separate.
    pub(crate) fn weighted_pages(&self) -> u64 {
        self.weighted_pages
    }
}

pub(crate) struct NativeSelectedFreedPrefix {
    data: NativeSelectedFreedFamilyFacts,
    system: NativeSelectedFreedFamilyFacts,
}
impl NativeSelectedFreedPrefix {
    pub(super) fn empty() -> Self {
        Self {
            data: NativeSelectedFreedFamilyFacts::default(),
            system: NativeSelectedFreedFamilyFacts::default(),
        }
    }
    // Called only after SAME actual owner/epoch/exact head re-probe succeeds.
    // Empty values remain one record and pay their whole encoded capacity.
    pub(super) fn observe_selected(
        &mut self,
        system: bool,
        count: usize,
        bytes: usize,
        weighted_pages: u64,
    ) -> PathResult<()> {
        let family = if system {
            &mut self.system
        } else {
            &mut self.data
        };
        let records = family
            .records
            .checked_add(1)
            .ok_or(NativeKeyPathError::Overflow)?;
        let page_numbers = family
            .page_numbers
            .checked_add(count)
            .ok_or(NativeKeyPathError::Overflow)?;
        let canonical_kv_bytes = family
            .canonical_kv_bytes
            .checked_add(bytes)
            .ok_or(NativeKeyPathError::Overflow)?;
        let weighted_pages = family
            .weighted_pages
            .checked_add(weighted_pages)
            .ok_or(NativeKeyPathError::Overflow)?;
        family.records = records;
        family.page_numbers = page_numbers;
        family.canonical_kv_bytes = canonical_kv_bytes;
        family.weighted_pages = weighted_pages;
        family.maximum_record_page_numbers = family.maximum_record_page_numbers.max(count);
        family.maximum_record_kv_bytes = family.maximum_record_kv_bytes.max(bytes);
        Ok(())
    }
    pub(crate) fn data(&self) -> &NativeSelectedFreedFamilyFacts {
        &self.data
    }
    pub(crate) fn system(&self) -> &NativeSelectedFreedFamilyFacts {
        &self.system
    }
}
