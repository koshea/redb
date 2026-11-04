//! Actual retained Source freed-record marker storage, not a reclamation budget.
use super::page_resources::{NativeControlAllocation, NativePageResources, refused};
use crate::transaction_tracker::TransactionId;
use alloc::vec::Vec;

pub(crate) struct NativeOwnedTransactionMarkers {
    ids: Vec<TransactionId>,
    _allocation: NativeControlAllocation,
}
impl NativeOwnedTransactionMarkers {
    // The prospective count comes from the exact retained marker count plus
    // this one real Immediate postepilogue, never pending-map shape/occupancy.
    pub(crate) fn prepare_replacement(
        existing: Option<&Self>,
        resources: &NativePageResources,
    ) -> crate::Result<Self> {
        let count = existing
            .map_or(0, Self::len)
            .checked_add(1)
            .ok_or_else(refused)?;
        let bound = super::container_bounds::vector_storage::<TransactionId>(count)
            .map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(count).map_err(|_| refused())?;
        if let Some(existing) = existing {
            ids.extend_from_slice(&existing.ids);
        }
        Ok(Self {
            ids,
            _allocation: allocation,
        })
    }
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }
    pub(crate) fn first(&self) -> Option<TransactionId> {
        self.ids.first().copied()
    }
    pub(crate) fn contains(&self, id: TransactionId) -> bool {
        self.ids.binary_search(&id).is_ok()
    }
    pub(crate) fn has_spare(&self) -> bool {
        self.ids.len() < self.ids.capacity()
    }
    pub(crate) fn insert_prepared(&mut self, id: TransactionId) {
        match self.ids.binary_search(&id) {
            Ok(_) => panic!("one real native transaction is registered once"),
            Err(index) => {
                assert!(
                    self.has_spare(),
                    "Source marker capacity is prepared before mutation"
                );
                self.ids.insert(index, id);
            }
        }
    }
    pub(crate) fn remove(&mut self, id: TransactionId) {
        if let Ok(index) = self.ids.binary_search(&id) {
            self.ids.remove(index);
        }
    }
}
