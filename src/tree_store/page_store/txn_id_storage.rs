//! Physical custody of finite cleanup result IDs, including moved iterators.
//! The complete fixed recipe prepares both passes before any native mutation.
#[cfg(not(redb_no_std))]
use super::page_resources::{NativeControlAllocation, NativePageResources};
use crate::transaction_tracker::TransactionId;
use alloc::vec::Vec;
use core::ops::{Deref, DerefMut};

#[derive(Default)]
pub(crate) struct NativeOwnedTxnIds {
    values: Vec<TransactionId>,
    #[cfg(not(redb_no_std))]
    allocation: Option<NativeControlAllocation>,
}
impl From<Vec<TransactionId>> for NativeOwnedTxnIds {
    fn from(values: Vec<TransactionId>) -> Self {
        Self {
            values,
            #[cfg(not(redb_no_std))]
            allocation: None,
        }
    }
}
impl NativeOwnedTxnIds {
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_source(
        count: usize,
        resources: &NativePageResources,
    ) -> crate::Result<Self> {
        use super::{container_bounds, page_resources::refused};
        let bound =
            container_bounds::vector_storage::<TransactionId>(count).map_err(|_| refused())?;
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
    pub(crate) fn require_for(
        &self,
        count: usize,
        resources: &NativePageResources,
    ) -> crate::Result<()> {
        if !self.values.is_empty()
            || self.values.capacity() < count
            || !self
                .allocation
                .as_ref()
                .is_some_and(|guard| guard.belongs_to(resources))
        {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    pub(crate) fn push(&mut self, id: TransactionId) {
        #[cfg(not(redb_no_std))]
        if self.allocation.is_some() {
            // At most one distinct touched ID per removed entry. This internal
            // invariant uses the earlier total-removal policy capacity proof.
            assert!(self.values.len() < self.values.capacity());
        }
        self.values.push(id);
    }
    pub(crate) fn truncate(&mut self, length: usize) {
        self.values.truncate(length);
    }
}
impl Deref for NativeOwnedTxnIds {
    type Target = [TransactionId];
    fn deref(&self) -> &[TransactionId] {
        &self.values
    }
}
impl DerefMut for NativeOwnedTxnIds {
    fn deref_mut(&mut self) -> &mut [TransactionId] {
        &mut self.values
    }
}
pub(crate) struct NativeOwnedTxnIdsIter {
    values: alloc::vec::IntoIter<TransactionId>,
    #[cfg(not(redb_no_std))]
    _allocation: Option<NativeControlAllocation>,
}
impl Iterator for NativeOwnedTxnIdsIter {
    type Item = TransactionId;
    fn next(&mut self) -> Option<TransactionId> {
        self.values.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }
}
impl ExactSizeIterator for NativeOwnedTxnIdsIter {}
impl IntoIterator for NativeOwnedTxnIds {
    type Item = TransactionId;
    type IntoIter = NativeOwnedTxnIdsIter;
    fn into_iter(self) -> Self::IntoIter {
        NativeOwnedTxnIdsIter {
            values: self.values.into_iter(),
            #[cfg(not(redb_no_std))]
            _allocation: self.allocation,
        }
    }
}
