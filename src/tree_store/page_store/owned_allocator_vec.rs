//! Actual allocator backing custody, independent of Source authority.
//!
//! The normal allocator parser and bitmap algorithms stay the sole semantic
//! owners. Guarded storage can only be installed before native growth/resize;
//! this private component does not activate a complete Source writer.
#[cfg(not(redb_no_std))]
use super::page_resources::{NativeControlAllocation, NativePageResources};
use alloc::vec::Vec;
use core::ops::{Deref, DerefMut, RangeBounds};

#[cfg(not(redb_no_std))]
#[derive(Default)]
pub(super) struct NativeOrdinaryAllocatorHandoff {
    #[cfg(test)]
    pub(super) fail_after: Option<usize>,
}
#[cfg(not(redb_no_std))]
impl NativeOrdinaryAllocatorHandoff {
    fn before_backing_reserve(&mut self) -> crate::Result<()> {
        #[cfg(test)]
        if let Some(remaining) = &mut self.fail_after {
            if *remaining == 0 {
                return Err(super::page_resources::refused());
            }
            *remaining -= 1;
        }
        Ok(())
    }
}

pub(super) struct NativeOwnedAllocatorVec<T> {
    values: Vec<T>,
    // Actual backing always retires before its numerical control.
    #[cfg(not(redb_no_std))]
    allocation: Option<NativeControlAllocation>,
}
impl<T> Default for NativeOwnedAllocatorVec<T> {
    fn default() -> Self {
        Self::from(Vec::new())
    }
}
impl<T> From<Vec<T>> for NativeOwnedAllocatorVec<T> {
    fn from(values: Vec<T>) -> Self {
        Self {
            values,
            #[cfg(not(redb_no_std))]
            allocation: None,
        }
    }
}
impl<T> Deref for NativeOwnedAllocatorVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}
impl<T> DerefMut for NativeOwnedAllocatorVec<T> {
    // Mutable element/slice access cannot grow the physical backing.
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}
impl<T> NativeOwnedAllocatorVec<T> {
    pub(super) fn capacity(&self) -> usize {
        self.values.capacity()
    }
    pub(super) fn push(&mut self, value: T) {
        #[cfg(not(redb_no_std))]
        if self.allocation.is_some() {
            assert!(self.values.len() < self.values.capacity());
        }
        self.values.push(value);
    }
    pub(super) fn insert(&mut self, index: usize, value: T) {
        #[cfg(not(redb_no_std))]
        if self.allocation.is_some() {
            assert!(self.values.len() < self.values.capacity());
        }
        self.values.insert(index, value);
    }
    pub(super) fn resize(&mut self, count: usize, value: T)
    where
        T: Clone,
    {
        #[cfg(not(redb_no_std))]
        if self.allocation.is_some() {
            assert!(count <= self.values.capacity());
        }
        self.values.resize(count, value);
    }
    pub(super) fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> alloc::vec::Drain<'_, T> {
        self.values.drain(range)
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn source(count: usize, resources: &NativePageResources) -> crate::Result<Self> {
        use super::{container_bounds, page_resources::refused};
        let bound = container_bounds::vector_storage::<T>(count).map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|_| refused())?;
        Ok(Self {
            values,
            allocation: Some(allocation),
        })
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_capacity(
        &mut self,
        count: usize,
        resources: &NativePageResources,
    ) -> crate::Result<()> {
        use super::page_resources::refused;
        if count < self.values.len() {
            return Err(refused());
        }
        if let Some(allocation) = &self.allocation {
            if !allocation.belongs_to(resources) {
                return Err(refused());
            }
        }
        // Existing physical baseline or same-pool storage is reused only when
        // its actual capacity already suffices. No fresh allocation is tagged
        // as baseline; complete factory custody must prove the baseline owner.
        if count <= self.values.capacity() {
            return Ok(());
        }
        let mut replacement = Self::source(count, resources)?;
        replacement.values.extend(self.values.drain(..));
        let old = core::mem::replace(self, replacement);
        drop(old); // old Vec, then old allowance; child owners moved intact
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn handoff_ordinary(
        &mut self,
        handoff: &mut NativeOrdinaryAllocatorHandoff,
    ) -> crate::Result<()> {
        if self.allocation.is_none() {
            return Ok(());
        }
        handoff.before_backing_reserve()?;
        // The real next ordinary writer owns the native writer slot before
        // aliases/namespaces escape. Ordinary allocation never needs Source
        // quota or a supported Source resource build profile.
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.values.capacity())
            .map_err(|_| super::page_resources::refused())?;
        values.extend(self.values.drain(..));
        let old = core::mem::replace(self, Self::from(values));
        drop(old);
        Ok(())
    }
}
impl<'a, T> IntoIterator for &'a NativeOwnedAllocatorVec<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}
impl<'a, T> IntoIterator for &'a mut NativeOwnedAllocatorVec<T> {
    type Item = &'a mut T;
    type IntoIter = core::slice::IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter_mut()
    }
}
