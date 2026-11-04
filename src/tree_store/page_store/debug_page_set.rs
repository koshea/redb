//! One semantic debug PN membership, with separately owned Source backing.
//! Ordinary HashSet behavior stays intact. Logical remove/clear never refunds
//! guarded Source capacity; only replacement or physical owner destruction can.
use super::base::PageNumber;
use super::fast_hash::PageNumberHashSet;
#[cfg(not(redb_no_std))]
use super::page_resources::{NativeControlAllocation, NativePageResources, refused};
#[cfg(not(redb_no_std))]
use alloc::vec::Vec;
use core::fmt;

#[cfg(not(redb_no_std))]
struct SourcePages {
    // Backing deallocates before its numerical owner.
    pages: Vec<PageNumber>,
    allocation: NativeControlAllocation,
}
#[derive(Default)]
pub(super) struct NativeDebugPageSet {
    ordinary: PageNumberHashSet,
    #[cfg(not(redb_no_std))]
    source: Option<SourcePages>,
    #[cfg(all(test, not(redb_no_std)))]
    fail_ordinary_reserve: bool,
}
impl NativeDebugPageSet {
    pub(super) fn len(&self) -> usize {
        let count = self.ordinary.len();
        #[cfg(not(redb_no_std))]
        let count = count + self.source.as_ref().map_or(0, |source| source.pages.len());
        count
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn contains(&self, page: &PageNumber) -> bool {
        if self.ordinary.contains(page) {
            return true;
        }
        #[cfg(not(redb_no_std))]
        if self
            .source
            .as_ref()
            .is_some_and(|source| source.pages.binary_search(page).is_ok())
        {
            return true;
        }
        false
    }
    pub(super) fn insert(&mut self, page: PageNumber) -> bool {
        if self.contains(&page) {
            return false;
        }
        #[cfg(not(redb_no_std))]
        if let Some(source) = &mut self.source {
            // Complete demand and selected-family checks occur BEFORE PN or
            // mutable-page mutation. This is only that private invariant.
            assert!(source.pages.len() < source.pages.capacity());
            let index = source.pages.binary_search(&page).unwrap_err();
            source.pages.insert(index, page);
            return true;
        }
        self.ordinary.insert(page)
    }
    pub(super) fn remove(&mut self, page: &PageNumber) -> bool {
        if self.ordinary.remove(page) {
            return true;
        }
        #[cfg(not(redb_no_std))]
        if let Some(source) = &mut self.source {
            if let Ok(index) = source.pages.binary_search(page) {
                source.pages.remove(index);
                return true;
            }
        }
        false
    }
    pub(super) fn clear(&mut self) {
        self.ordinary.clear();
        #[cfg(not(redb_no_std))]
        if let Some(source) = &mut self.source {
            source.pages.clear();
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &PageNumber> {
        #[cfg(not(redb_no_std))]
        {
            self.ordinary
                .iter()
                .chain(self.source.iter().flat_map(|source| source.pages.iter()))
        }
        #[cfg(redb_no_std)]
        {
            self.ordinary.iter()
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn has_source_storage(&self) -> bool {
        self.source.is_some()
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn belongs_to(&self, resources: &NativePageResources) -> bool {
        self.source
            .as_ref()
            .is_some_and(|source| source.allocation.belongs_to(resources))
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_source(
        &mut self,
        additional: usize,
        resources: &NativePageResources,
    ) -> crate::Result<()> {
        if self
            .source
            .as_ref()
            .is_some_and(|source| !source.allocation.belongs_to(resources))
        {
            return Err(refused());
        }
        let old_len = self.source.as_ref().map_or(0, |source| source.pages.len());
        let required = old_len.checked_add(additional).ok_or_else(refused)?;
        if self
            .source
            .as_ref()
            .is_some_and(|source| source.pages.capacity() >= required)
        {
            return Ok(());
        }
        let bound = super::container_bounds::vector_storage::<PageNumber>(required)
            .map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut pages = Vec::new();
        pages.try_reserve_exact(required).map_err(|_| refused())?;
        if let Some(source) = &self.source {
            pages.extend_from_slice(&source.pages);
        }
        let old = self.source.replace(SourcePages { pages, allocation });
        drop(old); // old backing first; every moved PN has exactly one new owner
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn before_mutation(
        &self,
        resources: Option<&NativePageResources>,
        insert: bool,
        required: bool,
    ) -> crate::Result<()> {
        match (&self.source, resources) {
            (None, _) if !required => Ok(()), // historical partial numerical fixtures only
            (Some(source), Some(resources))
                if source.allocation.belongs_to(resources)
                    && (!insert || source.pages.len() < source.pages.capacity()) =>
            {
                Ok(())
            }
            _ => Err(refused()),
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn handoff_ordinary(&mut self) -> crate::Result<()> {
        let Some(source) = self.source.as_ref() else {
            return Ok(());
        };
        if !source.pages.is_empty() {
            #[cfg(test)]
            if core::mem::take(&mut self.fail_ordinary_reserve) {
                return Err(refused());
            }
            // Genuine ordinary owner; no Source allowance/profile dependency.
            // Failure preserves Source membership/backing and is retryable.
            self.ordinary
                .try_reserve(source.pages.len())
                .map_err(|_| refused())?;
            for page in &source.pages {
                assert!(self.ordinary.insert(*page));
            }
        }
        let old = self.source.take();
        drop(old);
        Ok(())
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn source_buffer_for_test(&self) -> (usize, usize) {
        self.source.as_ref().map_or((0, 0), |source| {
            (source.pages.capacity(), source.pages.as_ptr() as usize)
        })
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn fail_next_ordinary_reserve_for_test(&mut self) {
        self.fail_ordinary_reserve = true;
    }
}
impl fmt::Debug for NativeDebugPageSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}
