//! Preadmitted checksum frames for the closed fixed executor.
use crate::Result;
use crate::sync::Mutex;
use crate::tree_store::btree_base::Checksum;
use crate::tree_store::fixed_scratch::{PreparedVec, ScratchVec};
use crate::tree_store::page_store::{
    NativeControlAllocation, NativeFixedObservationAdmission, NativeNumericalEpoch,
    NativePageResources, point_vector_bytes,
};
use crate::tree_store::{NativeOwnedShared, PageNumber, PageResolver};
use alloc::vec::Vec;

type Child = Option<(usize, PageNumber, Checksum)>;
struct Frames {
    slots: Vec<PreparedVec<Child>>,
    admission: NativeFixedObservationAdmission,
    allocation: NativeControlAllocation,
}
#[derive(Clone)]
pub(crate) struct NativeChecksumPool {
    owner: NativeOwnedShared<Mutex<Frames>>,
}
impl NativeChecksumPool {
    pub(crate) fn capacity_bytes(depth: usize, children: usize) -> Result<usize> {
        let outer = usize::try_from(
            point_vector_bytes::<PreparedVec<Child>>(depth).map_err(|_| refused())?,
        )
        .map_err(|_| refused())?;
        PreparedVec::<Child>::capacity_bytes(children)?
            .checked_mul(depth)
            .and_then(|n| n.checked_add(outer))
            .and_then(|n| {
                n.checked_add(NativeOwnedShared::<Mutex<Frames>>::source_capacity_bytes().ok()?)
            })
            .ok_or_else(refused)
    }
    pub(crate) fn prepare(
        actual: &PageResolver,
        numerical: &NativeNumericalEpoch,
        depth: usize,
        children: usize,
    ) -> Result<Self> {
        numerical.require_open()?;
        let bytes = usize::try_from(
            point_vector_bytes::<PreparedVec<Child>>(depth).map_err(|_| refused())?,
        )
        .map_err(|_| refused())?;
        let allocation = actual.admit_fixed_observation(bytes)?;
        if !allocation.belongs_to(numerical.resources()) {
            return Err(refused());
        }
        let admission = actual.seal_fixed_observation_admission()?;
        admission.require_for(numerical.resources())?;
        let owner = NativeOwnedShared::source(
            Mutex::new(Frames {
                slots: Vec::new(),
                admission,
                allocation,
            }),
            numerical.resources(),
        )?;
        let result = Self { owner };
        {
            let mut owner = result.owner.lock()?;
            owner
                .slots
                .try_reserve_exact(depth)
                .map_err(|_| refused())?;
            for _ in 0..depth {
                owner
                    .slots
                    .push(PreparedVec::prepare(actual, numerical, children)?);
            }
        }
        result.require_available(numerical.resources())?;
        Ok(result)
    }
    pub(crate) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
        let owner = self.owner.lock()?;
        owner.admission.require_for(resources)?;
        if !owner.allocation.belongs_to(resources) {
            return Err(refused());
        }
        for slot in &owner.slots {
            slot.require_available(resources)?;
        }
        Ok(())
    }
    pub(in crate::tree_store) fn checkout(
        &self,
        resources: &NativePageResources,
        depth: usize,
    ) -> Result<ScratchVec<Child>> {
        let slot = {
            let owner = self.owner.lock()?;
            owner.admission.require_for(resources)?;
            if !owner.allocation.belongs_to(resources) {
                return Err(refused());
            }
            owner.slots.get(depth).ok_or_else(refused)?.clone()
        };
        slot.checkout(resources)
    }
}
fn refused() -> crate::StorageError {
    crate::tree_store::source_owner_refused()
}
