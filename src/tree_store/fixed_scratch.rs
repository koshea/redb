//! Internal bare-storage slots. A checkout grants no page/tree authority.
use crate::Result;
#[cfg(not(redb_no_std))]
use crate::sync::Mutex;
#[cfg(not(redb_no_std))]
use crate::tree_store::page_store::{
    NativeControlAllocation, NativeFixedObservationAdmission, NativeNumericalEpoch,
    NativePageResources, point_vector_bytes,
};
#[cfg(not(redb_no_std))]
use crate::tree_store::{NativeOwnedShared, PageResolver};
use alloc::vec::Vec;
use core::ops::{Deref, RangeBounds};

#[cfg(not(redb_no_std))]
struct Slot<T> {
    storage: Option<Vec<T>>,
    admission: NativeFixedObservationAdmission,
    allocation: NativeControlAllocation,
}
// The bare Vec in Slot never contains a checkout/owner. No backedge exists.
#[cfg(not(redb_no_std))]
pub(super) struct PreparedVec<T> {
    owner: NativeOwnedShared<Mutex<Slot<T>>>,
    requested: usize,
}
#[cfg(not(redb_no_std))]
impl<T> Clone for PreparedVec<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            requested: self.requested,
        }
    }
}
#[cfg(not(redb_no_std))]
impl<T> PreparedVec<T> {
    pub(super) fn capacity_bytes(requested: usize) -> Result<usize> {
        usize::try_from(point_vector_bytes::<T>(requested).map_err(|_| refused())?)
            .map_err(|_| refused())?
            .checked_add(NativeOwnedShared::<Mutex<Slot<T>>>::source_capacity_bytes()?)
            .ok_or_else(refused)
    }
    pub(super) fn prepare(
        resolver: &PageResolver,
        numerical: &NativeNumericalEpoch,
        requested: usize,
    ) -> Result<Self> {
        numerical.require_open()?;
        let bytes = usize::try_from(point_vector_bytes::<T>(requested).map_err(|_| refused())?)
            .map_err(|_| refused())?;
        let allocation = resolver.admit_fixed_observation(bytes)?;
        if !allocation.belongs_to(numerical.resources()) {
            return Err(refused());
        }
        let admission = resolver.seal_fixed_observation_admission()?;
        admission.require_for(numerical.resources())?;
        // Both backing and the concrete closed Arc have been admitted before
        // the first heap construction. Errors retire backing before controls.
        let owner = NativeOwnedShared::source(
            Mutex::new(Slot {
                storage: None,
                admission,
                allocation,
            }),
            numerical.resources(),
        )?;
        let result = Self { owner, requested };
        {
            let mut slot = result.owner.lock()?;
            let mut storage = Vec::new();
            storage
                .try_reserve_exact(requested)
                .map_err(|_| refused())?;
            slot.storage = Some(storage);
        }
        result.require_available(numerical.resources())?;
        Ok(result)
    }
    pub(super) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
        let slot = self.owner.lock()?;
        slot.admission.require_for(resources)?;
        if !resources.is_source_write()
            || !slot.allocation.belongs_to(resources)
            || slot
                .storage
                .as_ref()
                .is_none_or(|v| !v.is_empty() || v.capacity() < self.requested)
        {
            return Err(refused());
        }
        Ok(())
    }
    pub(super) fn try_checkout(
        &self,
        resources: &NativePageResources,
    ) -> Result<Option<ScratchVec<T>>> {
        let storage = {
            let mut slot = self.owner.lock()?;
            // Refusal is distinct from a live checkout. Never scan past a
            // foreign, closed, poisoned or malformed slot as if merely busy.
            slot.admission.require_for(resources)?;
            if !resources.is_source_write() || !slot.allocation.belongs_to(resources) {
                return Err(refused());
            }
            match slot.storage.as_ref() {
                None => return Ok(None),
                Some(storage) if storage.is_empty() && storage.capacity() >= self.requested => {}
                Some(_) => return Err(refused()),
            }
            slot.storage
                .take()
                .expect("validated available scratch storage")
        };
        Ok(Some(ScratchVec {
            storage: Some(storage),
            owner: Some(self.clone()),
        }))
    }
    pub(super) fn checkout(&self, resources: &NativePageResources) -> Result<ScratchVec<T>> {
        self.try_checkout(resources)?.ok_or_else(refused)
    }
}
// Actual elements/backing precede the closed owner on every error/drop path.
// There is deliberately no Clone, raw Vec extraction or growing DerefMut.
pub(super) struct ScratchVec<T> {
    storage: Option<Vec<T>>,
    #[cfg(not(redb_no_std))]
    owner: Option<PreparedVec<T>>,
}
impl<T> Default for ScratchVec<T> {
    fn default() -> Self {
        Self {
            storage: Some(Vec::new()),
            #[cfg(not(redb_no_std))]
            owner: None,
        }
    }
}
impl<T> ScratchVec<T> {
    pub(super) fn ordinary(storage: Vec<T>) -> Self {
        Self {
            storage: Some(storage),
            #[cfg(not(redb_no_std))]
            owner: None,
        }
    }
    pub(super) fn try_push(&mut self, value: T) -> Result<()> {
        let storage = self.storage.as_mut().expect("live scratch storage");
        #[cfg(not(redb_no_std))]
        if self.owner.is_some() && storage.len() == storage.capacity() {
            return Err(refused());
        }
        storage.push(value);
        Ok(())
    }
    pub(super) fn capacity(&self) -> usize {
        self.storage
            .as_ref()
            .expect("live scratch storage")
            .capacity()
    }
    pub(super) fn as_slice(&self) -> &[T] {
        self.storage
            .as_ref()
            .expect("live scratch storage")
            .as_slice()
    }
    pub(super) fn is_prepared(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.owner.is_some()
        }
        #[cfg(redb_no_std)]
        {
            false
        }
    }
    pub(super) fn into_ordinary(mut self) -> Result<Vec<T>> {
        if self.is_prepared() {
            return Err(refused());
        }
        Ok(self.storage.take().expect("live ordinary scratch storage"))
    }
    pub(super) fn truncate(&mut self, length: usize) {
        self.storage
            .as_mut()
            .expect("live scratch storage")
            .truncate(length);
    }
    pub(super) fn as_mut_slice(&mut self) -> &mut [T] {
        self.storage
            .as_mut()
            .expect("live scratch storage")
            .as_mut_slice()
    }
    pub(super) fn clear(&mut self) {
        self.storage.as_mut().expect("live scratch storage").clear();
    }
    pub(super) fn pop(&mut self) -> Option<T> {
        self.storage.as_mut().expect("live scratch storage").pop()
    }
    pub(super) fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> alloc::vec::Drain<'_, T> {
        self.storage
            .as_mut()
            .expect("live scratch storage")
            .drain(range)
    }
    pub(super) fn reverse(&mut self) {
        self.storage
            .as_mut()
            .expect("live scratch storage")
            .reverse();
    }
}
impl ScratchVec<u8> {
    pub(super) fn try_extend_from_slice(&mut self, values: &[u8]) -> Result<()> {
        let storage = self.storage.as_mut().expect("live scratch storage");
        #[cfg(not(redb_no_std))]
        let count = storage
            .len()
            .checked_add(values.len())
            .ok_or_else(refused)?;
        #[cfg(not(redb_no_std))]
        if self.owner.is_some() && count > storage.capacity() {
            return Err(refused());
        }
        storage.extend_from_slice(values);
        Ok(())
    }
}
impl<T> Deref for ScratchVec<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.storage
            .as_ref()
            .expect("live scratch storage")
            .as_slice()
    }
}
impl<T> Drop for ScratchVec<T> {
    fn drop(&mut self) {
        let Some(mut storage) = self.storage.take() else {
            return;
        };
        // T can include actual PageImpl. It physically drops before returning
        // backing and before the LAST closed numerical control can retire.
        storage.clear();
        #[cfg(not(redb_no_std))]
        if let Some(pool) = &self.owner {
            if let Ok(mut slot) = pool.owner.lock() {
                if slot.storage.is_none() {
                    slot.storage = Some(storage);
                    return;
                }
            }
        }
        drop(storage);
    }
}
fn refused() -> crate::StorageError {
    #[cfg(not(redb_no_std))]
    {
        crate::tree_store::source_owner_refused()
    }
    #[cfg(redb_no_std)]
    {
        crate::StorageError::PreviousIo
    }
}

impl<T: core::fmt::Debug> core::fmt::Debug for ScratchVec<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self.as_slice(), f)
    }
}
impl<'a, T> IntoIterator for &'a ScratchVec<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}
