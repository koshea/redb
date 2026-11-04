//! Bounded debug bookkeeping for guaranteed fresh private copied pages ONLY.
//! Ordinary native pages retain the primary's unchanged global alias map.
use super::allocator_reopen_facts::AllocatorResourceError;
use super::base::PageNumber;
use super::page_resources::{NativeControlAllocation, NativePageResources, refused};
use super::read_only_resource::ReadOnlyResourceProfile;
use crate::Result;
use crate::sync::Mutex;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::mem::{align_of, size_of};

#[derive(Clone, Copy)]
struct Slot {
    page: PageNumber,
    references: u64,
}
struct CopiedPageMetadata {
    // FIRST: the actual fixed Vec/Mutex graph dies before the reservation.
    slots: Mutex<Vec<Option<Slot>>>,
    // LAST: refunds the numerical charge only after physical metadata dies.
    _control: NativeControlAllocation,
}

// Every strong metadata alias uses this closed owner; no Weak/naked Arc escapes.
// into_inner retires the final Arc CONTROL before inner slots/control destruction.
#[derive(Clone)]
pub(super) struct SharedCopiedPageMetadata(Option<Arc<CopiedPageMetadata>>);
impl Drop for SharedCopiedPageMetadata {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            drop(Arc::into_inner(owner));
        }
    }
}
impl SharedCopiedPageMetadata {
    fn get(&self) -> &CopiedPageMetadata {
        self.0.as_deref().expect("live copied-page metadata")
    }
    fn maximum_slots(
        physical_pool: usize,
        base_page: usize,
    ) -> core::result::Result<usize, AllocatorResourceError> {
        if base_page == 0 {
            return Err(AllocatorResourceError::Unsupported);
        }
        let minimum = NativePageResources::page_allocation_bound(base_page)
            .map_err(|_| AllocatorResourceError::Overflow)?;
        if minimum == 0 {
            return Err(AllocatorResourceError::Unsupported);
        }
        // Every distinct metadata key has at least one LIVE copied buffer, each
        // admitted >=the exact native base-page allocation. PageImpl clones
        // retain that same entry, never a new payload or a new distinct key.
        Ok(physical_pool / minimum)
    }
    pub(super) fn capacity_bytes(
        physical_pool: usize,
        base_page: usize,
    ) -> core::result::Result<usize, AllocatorResourceError> {
        let slots = Self::maximum_slots(physical_pool, base_page)?;
        let payload = slots
            .checked_mul(size_of::<Option<Slot>>())
            .ok_or(AllocatorResourceError::Overflow)?;
        let heap = ReadOnlyResourceProfile::allocation(payload, align_of::<Option<Slot>>())?
            .checked_add(ReadOnlyResourceProfile::arc_allocation::<CopiedPageMetadata>()?)
            .ok_or(AllocatorResourceError::Overflow)?;
        // Include construction input/result/control/Vec/Mutex overlap without
        // assuming stack-slot reuse. No growth/rehash/shrink allocation exists.
        let construction = size_of::<(
            CopiedPageMetadata,
            Option<CopiedPageMetadata>,
            Arc<CopiedPageMetadata>,
            Option<Arc<CopiedPageMetadata>>,
            SharedCopiedPageMetadata,
            Result<SharedCopiedPageMetadata>,
            Vec<Option<Slot>>,
            Mutex<Vec<Option<Slot>>>,
            NativeControlAllocation,
            NativePageResources,
            Option<Slot>,
            [usize; 5],
            Option<usize>,
            core::result::Result<usize, AllocatorResourceError>,
        )>();
        let total = heap
            .checked_add(u64::try_from(construction).map_err(|_| AllocatorResourceError::Overflow)?)
            .ok_or(AllocatorResourceError::Overflow)?;
        usize::try_from(total).map_err(|_| AllocatorResourceError::Overflow)
    }
    pub(super) fn new(
        resources: &NativePageResources,
        physical_pool: usize,
        base_page: usize,
    ) -> Result<Self> {
        let count = Self::maximum_slots(physical_pool, base_page).map_err(|_| refused())?;
        let bytes = Self::capacity_bytes(physical_pool, base_page).map_err(|_| refused())?;
        // The SAME preadmitted counter must pay every metadata byte BEFORE
        // the fixed slot Vec or Arc allocation; every error drops this guard.
        let control = resources.admit_control(bytes)?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(count).map_err(|_| refused())?;
        slots.resize_with(count, || None);
        Ok(Self(Some(Arc::new(CopiedPageMetadata {
            slots: Mutex::new(slots),
            _control: control,
        }))))
    }
    #[cfg(test)]
    pub(super) fn reserved_control_bytes_for_test(&self) -> usize {
        self.get()._control.reserved_bytes()
    }
    pub(super) fn require_available(&self) -> Result<()> {
        drop(self.get().slots.lock().map_err(|_| refused())?);
        Ok(())
    }
    pub(super) fn register(&self, page: PageNumber) -> Result<()> {
        let mut slots = self.get().slots.lock().map_err(|_| refused())?;
        if let Some(slot) = slots.iter_mut().flatten().find(|slot| slot.page == page) {
            slot.references = slot.references.checked_add(1).ok_or_else(refused)?;
            return Ok(());
        }
        let vacant = slots
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or_else(refused)?;
        *vacant = Some(Slot {
            page,
            references: 1,
        });
        Ok(())
    }
    pub(super) fn clone_reference(&self, page: PageNumber) {
        // Existing alias accounting may recover primitive counts for cleanup;
        // poisoning remains set, so future register admission still refuses.
        let mut slots = self
            .get()
            .slots
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let slot = slots
            .iter_mut()
            .flatten()
            .find(|slot| slot.page == page)
            .expect("live copied PageImpl metadata");
        slot.references = slot
            .references
            .checked_add(1)
            .expect("bounded native copied aliases");
    }
    pub(super) fn release_reference(&self, page: PageNumber) {
        // Existing alias accounting may recover primitive counts for cleanup;
        // poisoning remains set, so future register admission still refuses.
        let mut slots = self
            .get()
            .slots
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let slot = slots
            .iter_mut()
            .find(|slot| slot.is_some_and(|entry| entry.page == page))
            .expect("live copied PageImpl metadata");
        let entry = slot.as_mut().unwrap();
        assert!(entry.references > 0);
        entry.references -= 1;
        if entry.references == 0 {
            *slot = None;
        }
    }
    #[cfg(test)]
    pub(super) fn references_for_test(&self, page: PageNumber) -> u64 {
        self.get()
            .slots
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .find(|slot| slot.page == page)
            .map_or(0, |slot| slot.references)
    }
    #[cfg(test)]
    pub(super) fn capacity_for_test(&self) -> (usize, usize) {
        let slots = self.get().slots.lock().unwrap();
        (slots.len(), slots.capacity())
    }
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "copied_page_metadata_tests.rs"]
mod tests;
