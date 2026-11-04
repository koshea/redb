//! Physical ownership of moved page-number buffers.
//!
//! These private owners do not authorize writes or establish a complete native
//! demand bound. Source capacities come from the closed recipe planner; they
//! must never be inferred from the number of pages live at one instant.
use super::fast_hash::PageNumberHashSet;
#[cfg(not(redb_no_std))]
use super::page_resources::{NativeControlAllocation, NativePageResources};
#[cfg(not(redb_no_std))]
use crate::Result;
use crate::tree_store::PageNumber;
use alloc::vec::Vec;
use core::ops::{Deref, DerefMut, RangeBounds};

// Field order is deliberate: native storage physically retires before its
// numerical guard. Neither this owner nor its iterator exposes the raw guard.
#[derive(Default)]
pub(crate) struct NativeOwnedPageBatch {
    pages: PageNumberHashSet,
    #[cfg(not(redb_no_std))]
    prepared_vector: Option<NativeOwnedPageVec>,
    #[cfg(not(redb_no_std))]
    _allocation: Option<NativeControlAllocation>,
}

impl NativeOwnedPageBatch {
    pub(crate) fn ordinary(pages: PageNumberHashSet) -> Self {
        Self {
            pages,
            ..Self::default()
        }
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_source(
        resources: &NativePageResources,
        total_requests: usize,
    ) -> Result<Self> {
        use super::{container_bounds, page_resources::refused};
        // This is a FRESH hash allocation. The witness is its requested insert
        // count, not capacity() sampled after a history of tombstones.
        let set_bound =
            container_bounds::hash_storage_from_historical_fresh_capacity::<PageNumber>(
                total_requests,
            )
            .map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(set_bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut pages = PageNumberHashSet::default();
        pages.try_reserve(total_requests).map_err(|_| refused())?;
        // Prepare the set -> Vec destination before the first page mutation.
        // Both guards coexist while the actual old hash buckets are consumed.
        let prepared_vector = NativeOwnedPageVec::prepare_source(resources, total_requests)?;
        Ok(Self {
            pages,
            prepared_vector: Some(prepared_vector),
            _allocation: Some(allocation),
        })
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn is_source(&self) -> bool {
        self._allocation.is_some()
    }

    pub(crate) fn into_prepared_vec(self) -> NativeOwnedPageVec {
        #[cfg(not(redb_no_std))]
        {
            let Self {
                pages,
                prepared_vector,
                _allocation: allocation,
            } = self;
            if let Some(mut vector) = prepared_vector {
                assert!(pages.len() <= vector.capacity());
                // Keep old allocation custody outside the loop. HashSet's
                // consuming iterator may retire its bucket array at loop end.
                vector.pages.extend(pages);
                drop(allocation);
                return vector;
            }
            debug_assert!(allocation.is_none());
            return NativeOwnedPageVec::ordinary(pages.into_iter().collect());
        }
        #[cfg(redb_no_std)]
        NativeOwnedPageVec::ordinary(self.pages.into_iter().collect())
    }
}

impl Deref for NativeOwnedPageBatch {
    type Target = PageNumberHashSet;
    fn deref(&self) -> &Self::Target {
        &self.pages
    }
}
impl NativeOwnedPageBatch {
    pub(crate) fn insert(&mut self, page: PageNumber) -> bool {
        #[cfg(not(redb_no_std))]
        if self.is_source() {
            // Never enter HashSet's allocating insertion path for a charged
            // buffer. The owning tracker separately prechecks TOTAL requests
            // before the native page/allocator mutation.
            assert!(self.pages.contains(&page) || self.pages.len() < self.pages.capacity());
        }
        self.pages.insert(page)
    }
    pub(crate) fn remove(&mut self, page: &PageNumber) -> bool {
        self.pages.remove(page)
    }
}

pub(crate) struct NativeOwnedPageBatchIter {
    pages: <PageNumberHashSet as IntoIterator>::IntoIter,
    #[cfg(not(redb_no_std))]
    _prepared_vector: Option<NativeOwnedPageVec>,
    #[cfg(not(redb_no_std))]
    _allocation: Option<NativeControlAllocation>,
}
impl Iterator for NativeOwnedPageBatchIter {
    type Item = PageNumber;
    fn next(&mut self) -> Option<PageNumber> {
        self.pages.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.pages.size_hint()
    }
}
impl ExactSizeIterator for NativeOwnedPageBatchIter {}
impl IntoIterator for NativeOwnedPageBatch {
    type Item = PageNumber;
    type IntoIter = NativeOwnedPageBatchIter;
    fn into_iter(self) -> Self::IntoIter {
        NativeOwnedPageBatchIter {
            pages: self.pages.into_iter(),
            #[cfg(not(redb_no_std))]
            _prepared_vector: self.prepared_vector,
            #[cfg(not(redb_no_std))]
            _allocation: self._allocation,
        }
    }
}
impl<'a> IntoIterator for &'a NativeOwnedPageBatch {
    type Item = &'a PageNumber;
    type IntoIter = <&'a PageNumberHashSet as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        (&self.pages).into_iter()
    }
}

#[cfg(not(redb_no_std))]
#[derive(Clone, Copy, Eq, PartialEq)]
enum NativePageVectorFamily {
    Allocated,
    DataFreed,
    SystemFreed,
}

#[derive(Default)]
pub(crate) struct NativeOwnedPageVec {
    pages: Vec<PageNumber>,
    // Original finite request, never a sampled historical Vec capacity.
    #[cfg(not(redb_no_std))]
    requested_capacity: usize,
    // Only the shared destination owns the pool; stored local slots never do.
    #[cfg(not(redb_no_std))]
    local_pool: Option<super::NativeOwnedShared<crate::sync::Mutex<NativeLocalFreedPool>>>,
    #[cfg(not(redb_no_std))]
    family: Option<NativePageVectorFamily>,
    #[cfg(not(redb_no_std))]
    _allocation: Option<NativeControlAllocation>,
}
impl NativeOwnedPageVec {
    pub(crate) fn ordinary(pages: Vec<PageNumber>) -> Self {
        Self {
            pages,
            #[cfg(not(redb_no_std))]
            requested_capacity: 0,
            #[cfg(not(redb_no_std))]
            local_pool: None,
            #[cfg(not(redb_no_std))]
            family: None,
            #[cfg(not(redb_no_std))]
            _allocation: None,
        }
    }
    #[cfg(not(redb_no_std))]
    fn prepare_source(resources: &NativePageResources, count: usize) -> Result<Self> {
        Self::prepare_family(resources, count, NativePageVectorFamily::Allocated)
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_data_freed(
        resources: &NativePageResources,
        count: usize,
    ) -> Result<Self> {
        Self::prepare_family(resources, count, NativePageVectorFamily::DataFreed)
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_system_freed(
        resources: &NativePageResources,
        count: usize,
    ) -> Result<Self> {
        Self::prepare_family(resources, count, NativePageVectorFamily::SystemFreed)
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_prepared_data_freed(
        &self,
        resources: &NativePageResources,
        count: usize,
    ) -> Result<()> {
        self.require_prepared_family(resources, count, NativePageVectorFamily::DataFreed)
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_prepared_system_freed(
        &self,
        resources: &NativePageResources,
        count: usize,
    ) -> Result<()> {
        self.require_prepared_family(resources, count, NativePageVectorFamily::SystemFreed)
    }
    #[cfg(not(redb_no_std))]
    fn require_prepared_family(
        &self,
        resources: &NativePageResources,
        count: usize,
        family: NativePageVectorFamily,
    ) -> Result<()> {
        if !self.pages.is_empty()
            || self.pages.capacity() < count
            || self.family != Some(family)
            || !self
                ._allocation
                .as_ref()
                .is_some_and(|guard| guard.belongs_to(resources))
        {
            return Err(super::page_resources::refused());
        }
        Ok(())
    }
    // The checked fixed executor has one namespace tree, one table tree, and
    // one cursor/extractor local owner per family. It exposes no parallel/raw
    // table handles. Slots are recycled, not allocated per operation/turn.
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_fixed_local_storage(
        &mut self,
        resources: &NativePageResources,
    ) -> Result<()> {
        use super::page_resources::refused;
        if self.local_pool.is_some()
            || self.family == Some(NativePageVectorFamily::Allocated)
            || !self.pages.is_empty()
            || !self
                ._allocation
                .as_ref()
                .is_some_and(|a| a.belongs_to(resources))
        {
            return Err(refused());
        }
        let family = self.family.ok_or_else(refused)?;
        let slots = [
            Some(Self::prepare_family(
                resources,
                self.requested_capacity,
                family,
            )?),
            Some(Self::prepare_family(
                resources,
                self.requested_capacity,
                family,
            )?),
            Some(Self::prepare_family(
                resources,
                self.requested_capacity,
                family,
            )?),
        ];
        let pool = NativeLocalFreedPool {
            slots,
            quarantined: [None, None, None],
            poisoned: false,
            family,
            requested_capacity: self.requested_capacity,
        };
        self.local_pool = Some(super::NativeOwnedShared::source(
            crate::sync::Mutex::new(pool),
            resources,
        )?);
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn require_fixed_local_storage(
        &self,
        resources: &NativePageResources,
    ) -> Result<()> {
        use super::page_resources::refused;
        let pool = self.local_pool.as_ref().ok_or_else(refused)?.lock()?;
        if pool.poisoned
            || pool.quarantined.iter().any(Option::is_some)
            || pool.family != self.family.ok_or_else(refused)?
            || pool.requested_capacity != self.requested_capacity
        {
            return Err(refused());
        }
        for slot in &pool.slots {
            let slot = slot.as_ref().ok_or_else(refused)?;
            if slot.local_pool.is_some()
                || slot.family != Some(pool.family)
                || slot.requested_capacity != pool.requested_capacity
                || slot.pages.capacity() < pool.requested_capacity
                || !slot.pages.is_empty()
                || !slot
                    ._allocation
                    .as_ref()
                    .is_some_and(|a| a.belongs_to(resources))
            {
                return Err(refused());
            }
        }
        Ok(())
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn require_fixed_local_integrity(&self) -> Result<()> {
        use super::page_resources::refused;
        let pool = self.local_pool.as_ref().ok_or_else(refused)?.lock()?;
        if pool.poisoned
            || pool.quarantined.iter().any(Option::is_some)
            || Some(pool.family) != self.family
            || pool.requested_capacity != self.requested_capacity
        {
            return Err(refused());
        }
        Ok(())
    }

    // Called before an operation can mutate any page. The original shared
    // queue and this local scratch retain distinct, fully admitted backings.
    // Whole-recipe preconstruction outside the writer remains a factory join.
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_local_like(
        &mut self,
        original: &Self,
        resources: Option<&NativePageResources>,
    ) -> Result<()> {
        use super::page_resources::refused;
        match (&original._allocation, &self._allocation) {
            // Preserve original ordinary buffers, including partial numerical
            // fixtures that have never installed prepared freed owners.
            (None, None) => return Ok(()),
            (Some(origin), existing) => {
                let resources = resources.ok_or_else(refused)?;
                let family = original.family.ok_or_else(refused)?;
                if family == NativePageVectorFamily::Allocated
                    || !origin.belongs_to(resources)
                    || original.pages.capacity() < original.requested_capacity
                    || !self.pages.is_empty()
                {
                    return Err(refused());
                }
                if let Some(local) = existing {
                    if self.family != Some(family)
                        || !local.belongs_to(resources)
                        || self.requested_capacity != original.requested_capacity
                        || self.pages.capacity() < self.requested_capacity
                    {
                        return Err(refused());
                    }
                    return Ok(());
                }
                // Only a fresh empty local can become Source scratch. Do not
                // relabel an already allocated ordinary backing as admitted.
                if self.pages.capacity() != 0 || self.family.is_some() {
                    return Err(refused());
                }
                let prepared =
                    Self::prepare_family(resources, original.requested_capacity, family)?;
                *self = prepared;
                Ok(())
            }
            (None, Some(_)) => Err(refused()),
        }
    }

    #[cfg(not(redb_no_std))]
    fn prepare_family(
        resources: &NativePageResources,
        count: usize,
        family: NativePageVectorFamily,
    ) -> Result<Self> {
        use super::{container_bounds, page_resources::refused};
        let bound = container_bounds::vector_storage::<PageNumber>(count).map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut pages = Vec::new();
        pages.try_reserve_exact(count).map_err(|_| refused())?;
        Ok(Self {
            pages,
            requested_capacity: count,
            local_pool: None,
            family: Some(family),
            _allocation: Some(allocation),
        })
    }
}
impl Deref for NativeOwnedPageVec {
    type Target = Vec<PageNumber>;
    fn deref(&self) -> &Self::Target {
        &self.pages
    }
}
impl NativeOwnedPageVec {
    pub(crate) fn push(&mut self, page: PageNumber) {
        #[cfg(not(redb_no_std))]
        if self._allocation.is_some() {
            // The fixed executor must prove/prepay the full push bound before
            // any tree mutation. No late allocating insertion is permitted.
            assert!(self.pages.len() < self.pages.capacity());
        }
        self.pages.push(page);
    }

    // The complete fixed executor must call this with its proved operation
    // upper BEFORE tree mutation. Actual current len alone is not that upper.
    // This numeric check grants no native writer or Source authority.
    pub(crate) fn preflight_append_bound(
        &self,
        source: &Self,
        maximum_pages: usize,
    ) -> crate::Result<()> {
        if maximum_pages < source.len() {
            return Err(super::page_resources::refused());
        }
        #[cfg(not(redb_no_std))]
        match (&self._allocation, &source._allocation) {
            (None, None) => {} // original ordinary Vec behavior/growth
            (Some(destination), Some(origin)) => {
                if self.family != source.family
                    || !destination.same_pool(origin)
                    || maximum_pages > self.pages.capacity() - self.pages.len()
                    || maximum_pages > source.pages.capacity()
                {
                    return Err(super::page_resources::refused());
                }
            }
            // Never copy a charged source into an ordinary growing buffer or
            // adopt uncharged scratch as a supposedly complete Source owner.
            _ => return Err(super::page_resources::refused()),
        }
        Ok(())
    }

    pub(crate) fn append_owner(&mut self, pages: &mut Self) {
        // This is an internal invariant recheck, not the earlier whole-recipe
        // capacity proof or a recoverable admission boundary. Source freed
        // construction remains inactive until that actual executor is joined.
        assert!(self.preflight_append_bound(pages, pages.len()).is_ok());
        // The source keeps its actual emptied buffer and guard. The destination
        // keeps its distinct preallocated owner through the physical copy.
        self.pages.append(&mut pages.pages);
    }

    pub(crate) fn clear(&mut self) {
        // Logical empty is not capacity retirement or permission to refund.
        self.pages.clear();
    }

    // Existing ordinary non-durable extraction preserves its original Vec
    // predicate and never grows capacity. The vector/guard stay borrowed.
    pub(crate) fn extract_if<R, F>(
        &mut self,
        range: R,
        filter: F,
    ) -> alloc::vec::ExtractIf<'_, PageNumber, F>
    where
        R: RangeBounds<usize>,
        F: FnMut(&mut PageNumber) -> bool,
    {
        self.pages.extract_if(range, filter)
    }

    // Source's closed Immediate executor never enters the generic non-durable
    // raw-map representation. Ordinary buffers retain exactly their old Vec.
    // There is no guardless conversion of an actually charged buffer.
    pub(super) fn into_ordinary(self) -> crate::Result<Vec<PageNumber>> {
        #[cfg(not(redb_no_std))]
        if self._allocation.is_some() {
            return Err(super::page_resources::refused());
        }
        Ok(self.pages)
    }

    // No mutable Vec/reserve/raw-buffer escape: this only removes members
    // while the borrowing wrapper keeps custody of its original allocation.
    pub(crate) fn drain<R: RangeBounds<usize>>(
        &mut self,
        range: R,
    ) -> alloc::vec::Drain<'_, PageNumber> {
        self.pages.drain(range)
    }
}
pub(crate) struct NativeOwnedPageVecIter {
    pages: alloc::vec::IntoIter<PageNumber>,
    #[cfg(not(redb_no_std))]
    _local_pool: Option<super::NativeOwnedShared<crate::sync::Mutex<NativeLocalFreedPool>>>,
    #[cfg(not(redb_no_std))]
    _allocation: Option<NativeControlAllocation>,
}
impl Iterator for NativeOwnedPageVecIter {
    type Item = PageNumber;
    fn next(&mut self) -> Option<PageNumber> {
        self.pages.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.pages.size_hint()
    }
}
impl ExactSizeIterator for NativeOwnedPageVecIter {}
impl IntoIterator for NativeOwnedPageVec {
    type Item = PageNumber;
    type IntoIter = NativeOwnedPageVecIter;
    fn into_iter(self) -> Self::IntoIter {
        NativeOwnedPageVecIter {
            pages: self.pages.into_iter(),
            #[cfg(not(redb_no_std))]
            _local_pool: self.local_pool,
            #[cfg(not(redb_no_std))]
            _allocation: self._allocation,
        }
    }
}
impl<'a> IntoIterator for &'a NativeOwnedPageVec {
    type Item = &'a PageNumber;
    type IntoIter = core::slice::Iter<'a, PageNumber>;
    fn into_iter(self) -> Self::IntoIter {
        self.pages.iter()
    }
}

// A moved map owns the per-transaction batches. IntoIter's current and
// unconsumed values retain their own actual storage guards independently.
pub(crate) type NativeOwnedTxnPageBatches =
    alloc::collections::BTreeMap<crate::transaction_tracker::TransactionId, NativeOwnedPageBatch>;

// Only UnpersistedState mints the claimed variant, under its existing mutex at
// the durable transaction's existing irreversible adoption boundary.
pub(super) struct NativeAdoptedPageBatch {
    pub(super) pages: NativeOwnedPageBatch,
    pub(super) already_claimed: bool,
}

#[cfg(all(test, not(redb_no_std)))]
mod vector_transfer_tests {
    use super::super::{container_bounds, page_resources::NativePageResources};
    use super::{NativeOwnedPageVec, NativePageVectorFamily};
    use crate::tree_store::PageNumber;

    fn pool() -> NativePageResources {
        // Four actual two-entry vector buffers, including simultaneous owners.
        // Numeric unit fixture only: no SourceReservation/full factory proof.
        let bound = container_bounds::vector_storage::<PageNumber>(2)
            .unwrap()
            .allocator_bytes;
        NativePageResources::from_retained_pool_data(usize::try_from(bound).unwrap() * 4, ())
            .unwrap()
    }
    fn data(pool: &NativePageResources, count: usize) -> NativeOwnedPageVec {
        NativeOwnedPageVec::prepare_family(pool, count, NativePageVectorFamily::DataFreed).unwrap()
    }
    // req: STO-020, PRJ-004
    #[test]
    fn native_source_vector_preflight_rejects_guardless_cross_pool_and_cross_family_destination() {
        let pool = pool();
        let other = self::pool();
        let mut source = data(&pool, 1);
        source.push(PageNumber::new(0, 1, 0));
        let ordinary = NativeOwnedPageVec::default();
        let foreign = data(&other, 2);
        let system =
            NativeOwnedPageVec::prepare_family(&pool, 2, NativePageVectorFamily::SystemFreed)
                .unwrap();
        let before = pool.current();
        assert!(ordinary.preflight_append_bound(&source, 1).is_err());
        assert!(foreign.preflight_append_bound(&source, 1).is_err());
        assert!(system.preflight_append_bound(&source, 1).is_err());
        assert_eq!(source.len(), 1);
        assert_eq!(ordinary.len(), 0);
        assert_eq!(
            ordinary.capacity(),
            0,
            "refusal must not allocate guardless destination"
        );
        assert_eq!(pool.current(), before);
    }
    // req: STO-020, PRJ-004
    #[test]
    fn native_source_vector_transfer_keeps_both_physical_buffers_owned_after_merge() {
        let pool = pool();
        let mut source = data(&pool, 1);
        let mut destination = data(&pool, 2);
        source.push(PageNumber::new(0, 2, 0));
        let before = pool.current();
        destination.preflight_append_bound(&source, 1).unwrap();
        destination.append_owner(&mut source);
        assert!(source.is_empty());
        assert_eq!(source.capacity(), 1);
        assert_eq!(destination.as_slice(), &[PageNumber::new(0, 2, 0)]);
        assert_eq!(
            pool.current(),
            before,
            "append does not physically retire emptied source capacity"
        );
        drop(source);
        let destination_charge = pool.current();
        assert!(destination_charge > 0 && destination_charge < before);
        destination.clear();
        assert_eq!(
            pool.current(),
            destination_charge,
            "logical empty cannot refund backing storage"
        );
        drop(destination);
        assert_eq!(pool.current(), 0);
    }
    // req: STO-020, PRJ-004
    #[test]
    fn native_source_vector_preflight_requires_prospective_capacity_before_mutation() {
        let pool = pool();
        let source = data(&pool, 2);
        let destination = data(&pool, 1);
        let before = pool.current();
        assert!(source.is_empty());
        // Checking only current zero len would miss the proved future count.
        assert!(destination.preflight_append_bound(&source, 2).is_err());
        assert_eq!(destination.len(), 0);
        assert_eq!(pool.current(), before);
    }
}

#[cfg(not(redb_no_std))]
struct NativeLocalFreedPool {
    slots: [Option<NativeOwnedPageVec>; 3],
    // No extra backing allocation: retain an undrained original local owner
    // here until the actual failed writer/owner teardown, never as fresh scratch.
    quarantined: [Option<NativeOwnedPageVec>; 3],
    poisoned: bool,
    family: NativePageVectorFamily,
    requested_capacity: usize,
}

// The fields stay private: neither a mutable Vec nor a pool clone escapes.
// Ordinary owners contain only their original default Vec and no pool.
#[derive(Default)]
pub(crate) struct NativeLocalPageVec {
    pages: NativeOwnedPageVec,
    #[cfg(not(redb_no_std))]
    pool: Option<super::NativeOwnedShared<crate::sync::Mutex<NativeLocalFreedPool>>>,
}
impl Deref for NativeLocalPageVec {
    type Target = NativeOwnedPageVec;
    fn deref(&self) -> &Self::Target {
        &self.pages
    }
}
impl DerefMut for NativeLocalPageVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pages
    }
}
impl NativeLocalPageVec {
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_for(
        &mut self,
        original: &NativeOwnedPageVec,
        resources: Option<&NativePageResources>,
    ) -> Result<()> {
        use super::page_resources::refused;
        let Some(origin_pool) = &original.local_pool else {
            // Historical generic numerical components preserve their partial
            // local admission route. Checked production metadata MUST have pool.
            if self.pool.is_some() {
                return Err(refused());
            }
            return self.pages.prepare_local_like(original, resources);
        };
        let resources = resources.ok_or_else(refused)?;
        let origin = original._allocation.as_ref().ok_or_else(refused)?;
        if !origin.belongs_to(resources) || !self.pages.is_empty() {
            return Err(refused());
        }
        // Revalidation cannot allocate, even after a forgotten local has
        // consumed another slot. A closed serial refuses before any mutation.
        drop(resources.admit_control(0)?);
        if let Some(pool) = &self.pool {
            // Compare the retained actual mutex owners, not a scalar pool ID.
            if !core::ptr::eq(&**pool, &**origin_pool) {
                return Err(refused());
            }
            let state = pool.lock()?;
            if state.poisoned || state.quarantined.iter().any(Option::is_some) {
                return Err(refused());
            }
            return self.pages.prepare_local_like(original, Some(resources));
        }
        if self.pages.capacity() != 0 || self.pages._allocation.is_some() {
            return Err(refused());
        }
        let mut state = origin_pool.lock()?;
        if state.poisoned
            || state.quarantined.iter().any(Option::is_some)
            || Some(state.family) != original.family
            || state.requested_capacity != original.requested_capacity
        {
            return Err(refused());
        }
        let index = state
            .slots
            .iter()
            .position(Option::is_some)
            .ok_or_else(refused)?;
        let slot = state.slots[index].as_ref().ok_or_else(refused)?;
        if slot.local_pool.is_some()
            || slot.family != original.family
            || slot.requested_capacity != original.requested_capacity
            || slot.pages.capacity() < slot.requested_capacity
            || !slot.pages.is_empty()
            || !slot
                ._allocation
                .as_ref()
                .is_some_and(|a| a.belongs_to(resources))
        {
            return Err(refused());
        }
        // All checks precede the infallible transfer. No quota, reserve or new
        // Arc allocation occurs under the real native writer here.
        self.pages = state.slots[index]
            .take()
            .expect("checked actual local slot");
        self.pool = Some(origin_pool.clone());
        Ok(())
    }
}
impl Drop for NativeLocalPageVec {
    fn drop(&mut self) {
        #[cfg(not(redb_no_std))]
        if let Some(pool) = self.pool.take() {
            // No allocation or early PN free on any path. Normal operations
            // merge/drain before return; a nonempty abnormal local is retained
            // as quarantine, poisons this pool and can never be checked out.
            // The closed executor rechecks both pool states before commit.
            let mut state = match pool.lock() {
                Ok(state) => state,
                Err(error) => {
                    let mut state = error.into_inner();
                    state.poisoned = true;
                    state
                }
            };
            let valid = self.pages.is_empty()
                && self.pages.local_pool.is_none()
                && self.pages.family == Some(state.family)
                && self.pages.requested_capacity == state.requested_capacity;
            if valid {
                if let Some(slot) = state.slots.iter_mut().find(|slot| slot.is_none()) {
                    *slot = Some(core::mem::take(&mut self.pages));
                    return;
                }
            }
            state.poisoned = true;
            // Exactly three preconstructed local buffers exist. If this one is
            // outside the ready slots, at most two can already be quarantined.
            if let Some(slot) = state.quarantined.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(core::mem::take(&mut self.pages));
            }
        }
    }
}

#[cfg(all(test, not(redb_no_std)))]
mod fixed_local_tests {
    use super::*;
    use crate::sync::Mutex;
    use alloc::sync::Arc;
    use std::{sync::Barrier, thread};
    // Numerical unit marker, actual native storage/control allocations only.
    // These controls do not establish Source T/Source64 or writer authority.
    const REQUEST: usize = 4;
    fn prepared() -> (NativePageResources, NativeOwnedPageVec) {
        let resources = NativePageResources::from_retained_pool_data(1024 * 1024, ()).unwrap();
        let mut original = NativeOwnedPageVec::prepare_data_freed(&resources, REQUEST).unwrap();
        original.prepare_fixed_local_storage(&resources).unwrap();
        (resources, original)
    }
    // req: STO-020, REC-001
    #[test]
    fn native_fixed_local_preclaim_refusal_restores_all_existing_live_charges() {
        let vector = usize::try_from(
            super::super::container_bounds::vector_storage::<PageNumber>(REQUEST)
                .unwrap()
                .allocator_bytes,
        )
        .unwrap();
        let shell =
            super::super::NativeOwnedShared::<Mutex<NativeLocalFreedPool>>::source_capacity_bytes()
                .unwrap();
        // Exactly one destination + three actual local buffers, then the
        // concrete pool Arc shell. Refuse its last byte before Arc allocation.
        let limit = vector.checked_mul(4).unwrap().checked_add(shell).unwrap() - 1;
        let resources = NativePageResources::from_retained_pool_data(limit, ()).unwrap();
        let mut original = NativeOwnedPageVec::prepare_data_freed(&resources, REQUEST).unwrap();
        let before = resources.current();
        assert_eq!(before, vector);
        assert!(original.prepare_fixed_local_storage(&resources).is_err());
        assert!(original.local_pool.is_none());
        assert_eq!(
            resources.current(),
            before,
            "all temporary guarded slots must retire"
        );
        assert!(original.pages.capacity() >= REQUEST);
        assert!(original.pages.is_empty());
        assert!(original.prepare_fixed_local_storage(&resources).is_err());
        assert_eq!(
            resources.current(),
            before,
            "original storage survives repeated refusal"
        );
        drop(original);
        assert_eq!(resources.current(), 0);
    }
    // req: STO-020, REC-001
    #[test]
    fn native_fixed_local_reuse_and_forgotten_slot_never_allocate_replacement() {
        let (resources, original) = prepared();
        let full = resources.current();
        for _ in 0..12 {
            let mut local = NativeLocalPageVec::default();
            local.prepare_for(&original, Some(&resources)).unwrap();
            assert_eq!(resources.current(), full, "checkout is transfer only");
            drop(local);
            assert_eq!(
                resources.current(),
                full,
                "recycle keeps exact admitted backing"
            );
        }
        let mut held: [NativeLocalPageVec; 3] =
            core::array::from_fn(|_| NativeLocalPageVec::default());
        for local in &mut held {
            local.prepare_for(&original, Some(&resources)).unwrap();
        }
        for _ in 0..4 {
            let mut missing = NativeLocalPageVec::default();
            assert!(missing.prepare_for(&original, Some(&resources)).is_err());
            assert_eq!(
                resources.current(),
                full,
                "no fresh replacement on exhaustion"
            );
        }
        let [forgotten, a, b] = held;
        core::mem::forget(forgotten);
        drop(a);
        drop(b);
        let mut a = NativeLocalPageVec::default();
        let mut b = NativeLocalPageVec::default();
        a.prepare_for(&original, Some(&resources)).unwrap();
        b.prepare_for(&original, Some(&resources)).unwrap();
        let mut missing = NativeLocalPageVec::default();
        assert!(missing.prepare_for(&original, Some(&resources)).is_err());
        assert_eq!(
            resources.current(),
            full,
            "forgotten slot cannot be replaced"
        );
        drop(a);
        drop(b);
        drop(original);
        assert!(
            resources.current() > 0,
            "forgotten actual local retains pool and all backing charges"
        );
    }
    // req: STO-020, REC-001
    #[test]
    fn native_fixed_local_concurrent_final_aliases_retire_actual_pool_and_buffers() {
        let (resources, original) = prepared();
        let mut a = NativeLocalPageVec::default();
        let mut b = NativeLocalPageVec::default();
        a.prepare_for(&original, Some(&resources)).unwrap();
        b.prepare_for(&original, Some(&resources)).unwrap();
        let arrived = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for local in [a, b] {
            let arrived = arrived.clone();
            let release = release.clone();
            workers.push(thread::spawn(move || {
                arrived.wait();
                release.wait();
                drop(local);
            }));
        }
        drop(original);
        arrived.wait();
        assert!(
            resources.current() > 0,
            "real slot and pool controls stay charged before final release"
        );
        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(
            resources.current(),
            0,
            "final physical pool retirement releases all charges"
        );
    }
}
