//! Closed key backing. Ordinary Cow/Vec requests keep their existing behavior.
use super::PageAllocator;
#[cfg(not(redb_no_std))]
use super::fixed_scratch::ScratchVec;
use crate::Result;
use alloc::borrow::Cow;
use alloc::vec::Vec;

pub(super) enum BranchKeys<'a> {
    Ordinary(Vec<Cow<'a, [u8]>>),
    #[cfg(not(redb_no_std))]
    Source(source::KeyArena),
}
impl<'a> BranchKeys<'a> {
    pub(super) fn ordinary(requested: usize) -> Self {
        Self::Ordinary(Vec::with_capacity(requested))
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Ordinary(v) => v.len(),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.len(),
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn capacity(&self) -> usize {
        match self {
            Self::Ordinary(v) => v.capacity(),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.capacity(),
        }
    }
    pub(super) fn get(&self, index: usize) -> Option<&[u8]> {
        match self {
            Self::Ordinary(v) => v.get(index).map(|v| v.as_ref()),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.get(index),
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|i| self.get(i).expect("existing branch key"))
    }
    pub(super) fn push(&mut self, key: Cow<'a, [u8]>) -> Result<()> {
        match self {
            Self::Ordinary(v) => {
                v.push(key);
                Ok(())
            }
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.push(key.as_ref()),
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn is_prepared(&self) -> bool {
        match self {
            Self::Ordinary(_) => false,
            #[cfg(not(redb_no_std))]
            Self::Source(_) => true,
        }
    }
    pub(super) fn push_split(&mut self, key: BranchSplitKey<'a>) -> Result<()> {
        match key {
            BranchSplitKey::Ordinary(key) => self.push(key),
            #[cfg(not(redb_no_std))]
            BranchSplitKey::Source(key) => match self {
                Self::Source(arena) => arena.push(key.as_slice()),
                Self::Ordinary(_) => Err(super::source_owner_refused()),
            },
        }
    }
    pub(super) fn split_key(
        &self,
        index: usize,
        _allocator: &PageAllocator,
    ) -> Result<BranchSplitKey<'a>> {
        match self {
            Self::Ordinary(v) => Ok(BranchSplitKey::Ordinary(v[index].clone())),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => Ok(BranchSplitKey::Source(_allocator.prepare_split_separator(
                v.get(index).ok_or_else(super::source_owner_refused)?,
            )?)),
        }
    }
    pub(super) fn into_parts(self) -> PartialBranchKeys {
        match self {
            Self::Ordinary(v) => {
                PartialBranchKeys::Ordinary(v.into_iter().map(Cow::into_owned).collect())
            }
            #[cfg(not(redb_no_std))]
            Self::Source(v) => PartialBranchKeys::Source(v),
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn heap_capacity_bytes(&self) -> usize {
        match self {
            Self::Ordinary(v) => v.capacity() * core::mem::size_of::<Cow<'_, [u8]>>(),
            Self::Source(v) => v.heap_capacity_bytes(),
        }
    }
}

pub(super) enum PartialBranchKeys {
    Ordinary(Vec<Vec<u8>>),
    #[cfg(not(redb_no_std))]
    Source(source::KeyArena),
}
impl PartialBranchKeys {
    fn len(&self) -> usize {
        match self {
            Self::Ordinary(v) => v.len(),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.len(),
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(move |i| match self {
            Self::Ordinary(v) => v[i].as_slice(),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.get(i).expect("existing partial key"),
        })
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn is_prepared(&self) -> bool {
        matches!(self, Self::Source(_))
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn heap_capacity_bytes(&self) -> usize {
        match self {
            Self::Ordinary(v) => {
                v.capacity() * core::mem::size_of::<Vec<u8>>()
                    + v.iter().map(Vec::capacity).sum::<usize>()
            }
            Self::Source(v) => v.heap_capacity_bytes(),
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn trace_conversion(&self) -> (usize, usize) {
        match self {
            Self::Ordinary(v) => (v.capacity(), v.iter().map(Vec::capacity).sum()),
            Self::Source(v) => (v.capacity(), v.payload_capacity()),
        }
    }
}
impl core::fmt::Debug for PartialBranchKeys {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

pub(super) enum BranchSplitKey<'a> {
    Ordinary(Cow<'a, [u8]>),
    #[cfg(not(redb_no_std))]
    Source(ScratchVec<u8>),
}
impl BranchSplitKey<'_> {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Ordinary(v) => v.len(),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.len(),
        }
    }
    pub(super) fn into_owned_separator(self) -> OwnedSeparator {
        match self {
            Self::Ordinary(v) => OwnedSeparator::Ordinary(v.to_vec()),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => OwnedSeparator::Source(v),
        }
    }
}

pub(super) enum OwnedSeparator {
    Ordinary(Vec<u8>),
    #[cfg(not(redb_no_std))]
    Source(ScratchVec<u8>),
}
impl OwnedSeparator {
    pub(super) fn from_cow(value: Cow<'_, [u8]>, _allocator: &PageAllocator) -> Result<Self> {
        #[cfg(not(redb_no_std))]
        if _allocator.is_checked_source() {
            return Ok(Self::Source(
                _allocator.prepare_split_separator(value.as_ref())?,
            ));
        }
        Ok(Self::Ordinary(value.into_owned()))
    }
    pub(super) fn copy(value: &[u8], _allocator: &PageAllocator) -> Result<Self> {
        #[cfg(not(redb_no_std))]
        if _allocator.is_checked_source() {
            return Ok(Self::Source(_allocator.prepare_split_separator(value)?));
        }
        Ok(Self::Ordinary(value.to_vec()))
    }
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Ordinary(v) => v.as_slice(),
            #[cfg(not(redb_no_std))]
            Self::Source(v) => v.as_slice(),
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn is_prepared(&self) -> bool {
        match self {
            Self::Ordinary(_) => false,
            Self::Source(value) => value.is_prepared(),
        }
    }
}

#[cfg(not(redb_no_std))]
mod source {
    use super::super::fixed_scratch::PreparedVec;
    use super::*;
    use crate::sync::Mutex;
    use crate::tree_store::page_store::{
        NativeControlAllocation, NativeFixedObservationAdmission, NativeNumericalEpoch,
        NativePageResources, point_vector_bytes,
    };
    use crate::tree_store::{NativeOwnedShared, PageResolver};
    use core::ops::Range;
    type Owner = NativeOwnedShared<Mutex<Slot>>;
    struct Buffers {
        ranges: Vec<Range<usize>>,
        bytes: Vec<u8>,
    }
    struct Slot {
        storage: Option<Buffers>,
        admission: NativeFixedObservationAdmission,
        keys: usize,
        maximum_key: usize,
        byte_limit: usize,
        _allocation: NativeControlAllocation,
    }
    #[derive(Clone)]
    pub(crate) struct NativeBranchKeyPool {
        slots: [Owner; 3],
        separators: [PreparedVec<u8>; 2],
        keys: usize,
        maximum_key: usize,
    }
    impl NativeBranchKeyPool {
        pub(crate) fn capacity_bytes(keys: usize, maximum_key: usize) -> Result<usize> {
            let payload = keys
                .checked_mul(maximum_key)
                .ok_or_else(super::super::source_owner_refused)?;
            let vector = |n| usize::try_from(n).map_err(|_| super::super::source_owner_refused());
            let arena = vector(
                point_vector_bytes::<Range<usize>>(keys)
                    .map_err(|_| super::super::source_owner_refused())?,
            )?
            .checked_add(vector(
                point_vector_bytes::<u8>(payload)
                    .map_err(|_| super::super::source_owner_refused())?,
            )?)
            .and_then(|n| n.checked_add(Owner::source_capacity_bytes().ok()?))
            .ok_or_else(super::super::source_owner_refused)?;
            arena
                .checked_mul(3)
                .and_then(|n| {
                    PreparedVec::<u8>::capacity_bytes(maximum_key)
                        .ok()?
                        .checked_mul(2)?
                        .checked_add(n)
                })
                .ok_or_else(super::super::source_owner_refused)
        }
        fn prepare_slot(
            resolver: &PageResolver,
            numerical: &NativeNumericalEpoch,
            keys: usize,
            maximum_key: usize,
        ) -> Result<Owner> {
            let byte_limit = keys
                .checked_mul(maximum_key)
                .ok_or_else(super::super::source_owner_refused)?;
            let backing = point_vector_bytes::<Range<usize>>(keys)
                .map_err(|_| super::super::source_owner_refused())?
                .checked_add(
                    point_vector_bytes::<u8>(byte_limit)
                        .map_err(|_| super::super::source_owner_refused())?,
                )
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(super::super::source_owner_refused)?;
            let allocation = resolver.admit_fixed_observation(backing)?;
            if !allocation.belongs_to(numerical.resources()) {
                return Err(super::super::source_owner_refused());
            }
            let admission = resolver.seal_fixed_observation_admission()?;
            admission.require_for(numerical.resources())?;
            // Concrete header and backing are admitted before any heap allocation.
            let owner = NativeOwnedShared::source(
                Mutex::new(Slot {
                    storage: None,
                    admission,
                    keys,
                    maximum_key,
                    byte_limit,
                    _allocation: allocation,
                }),
                numerical.resources(),
            )?;
            {
                let mut slot = owner.lock()?;
                let mut storage = Buffers {
                    ranges: Vec::new(),
                    bytes: Vec::new(),
                };
                storage
                    .ranges
                    .try_reserve_exact(keys)
                    .map_err(|_| super::super::source_owner_refused())?;
                storage
                    .bytes
                    .try_reserve_exact(byte_limit)
                    .map_err(|_| super::super::source_owner_refused())?;
                slot.storage = Some(storage);
            }
            Ok(owner)
        }
        pub(crate) fn prepare(
            resolver: &PageResolver,
            numerical: &NativeNumericalEpoch,
            keys: usize,
            maximum_key: usize,
        ) -> Result<Self> {
            numerical.require_open()?;
            let result = Self {
                slots: [
                    Self::prepare_slot(resolver, numerical, keys, maximum_key)?,
                    Self::prepare_slot(resolver, numerical, keys, maximum_key)?,
                    Self::prepare_slot(resolver, numerical, keys, maximum_key)?,
                ],
                separators: [
                    PreparedVec::prepare(resolver, numerical, maximum_key)?,
                    PreparedVec::prepare(resolver, numerical, maximum_key)?,
                ],
                keys,
                maximum_key,
            };
            result.require_available(numerical.resources())?;
            Ok(result)
        }
        #[cfg(test)]
        pub(super) fn poison_first_slot_for_fixture(&self) {
            let owner = self.slots[0].clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = owner.lock().unwrap();
                panic!("deterministic key slot poison");
            }));
            assert!(result.is_err());
        }
        pub(crate) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
            for owner in &self.slots {
                let slot = owner.lock()?;
                slot.admission.require_for(resources)?;
                if !resources.is_source_write()
                    || !slot._allocation.belongs_to(resources)
                    || slot.keys != self.keys
                    || slot.maximum_key != self.maximum_key
                    || slot.storage.as_ref().is_none_or(|v| {
                        !v.ranges.is_empty()
                            || !v.bytes.is_empty()
                            || v.ranges.capacity() < slot.keys
                            || v.bytes.capacity() < slot.byte_limit
                    })
                {
                    return Err(super::super::source_owner_refused());
                }
            }
            for slot in &self.separators {
                slot.require_available(resources)?;
            }
            Ok(())
        }
        pub(in crate::tree_store) fn checkout(
            &self,
            resources: &NativePageResources,
            requested: usize,
        ) -> Result<KeyArena> {
            if requested > self.keys {
                return Err(super::super::source_owner_refused());
            }
            for owner in &self.slots {
                let storage = {
                    let mut slot = owner.lock()?;
                    slot.admission.require_for(resources)?;
                    if !resources.is_source_write() || !slot._allocation.belongs_to(resources) {
                        return Err(super::super::source_owner_refused());
                    }
                    let Some(storage) = slot.storage.as_ref() else {
                        continue;
                    };
                    if !storage.ranges.is_empty()
                        || !storage.bytes.is_empty()
                        || storage.ranges.capacity() < self.keys
                        || storage.bytes.capacity() < slot.byte_limit
                    {
                        return Err(super::super::source_owner_refused());
                    }
                    slot.storage.take().expect("validated key storage")
                };
                return Ok(KeyArena {
                    storage: Some(storage),
                    owner: owner.clone(),
                    requested,
                    maximum_key: self.maximum_key,
                });
            }
            Err(super::super::source_owner_refused())
        }
        pub(in crate::tree_store) fn separator(
            &self,
            resources: &NativePageResources,
            value: &[u8],
        ) -> Result<ScratchVec<u8>> {
            if value.len() > self.maximum_key {
                return Err(super::super::source_owner_refused());
            }
            for slot in &self.separators {
                if let Some(mut storage) = slot.try_checkout(resources)? {
                    storage.try_extend_from_slice(value)?;
                    return Ok(storage);
                }
            }
            Err(super::super::source_owner_refused())
        }
    }
    // Bare backing before its closed owner, no borrowed key or pool backedge.
    pub(in crate::tree_store) struct KeyArena {
        storage: Option<Buffers>,
        owner: Owner,
        requested: usize,
        maximum_key: usize,
    }
    impl KeyArena {
        fn buffers(&self) -> &Buffers {
            self.storage.as_ref().expect("live key arena")
        }
        pub(super) fn len(&self) -> usize {
            self.buffers().ranges.len()
        }
        #[cfg(test)]
        pub(super) fn capacity(&self) -> usize {
            self.buffers().ranges.capacity()
        }
        pub(super) fn get(&self, index: usize) -> Option<&[u8]> {
            self.buffers()
                .ranges
                .get(index)
                .map(|range| &self.buffers().bytes[range.clone()])
        }
        pub(super) fn push(&mut self, key: &[u8]) -> Result<()> {
            let slot = self.owner.lock()?;
            slot.admission.require_current()?;
            let storage = self.storage.as_mut().expect("live key arena");
            let end = storage
                .bytes
                .len()
                .checked_add(key.len())
                .ok_or_else(super::super::source_owner_refused)?;
            if storage.ranges.len() >= self.requested
                || storage.ranges.len() == storage.ranges.capacity()
                || key.len() > self.maximum_key
                || end > slot.byte_limit
                || end > storage.bytes.capacity()
            {
                return Err(super::super::source_owner_refused());
            }
            let start = storage.bytes.len();
            storage.bytes.extend_from_slice(key);
            storage.ranges.push(start..end);
            Ok(())
        }
        #[cfg(test)]
        pub(super) fn payload_capacity(&self) -> usize {
            self.buffers().bytes.capacity()
        }
        #[cfg(test)]
        pub(super) fn heap_capacity_bytes(&self) -> usize {
            self.buffers().ranges.capacity() * core::mem::size_of::<Range<usize>>()
                + self.buffers().bytes.capacity()
        }
    }
    impl Drop for KeyArena {
        fn drop(&mut self) {
            let Some(mut storage) = self.storage.take() else {
                return;
            };
            storage.ranges.clear();
            storage.bytes.clear();
            // A poisoned return destroys real backing before the closed owner.
            if let Ok(mut slot) = self.owner.lock() {
                if slot.storage.is_none() {
                    slot.storage = Some(storage);
                    return;
                }
            }
            drop(storage);
        }
    }
}
#[cfg(not(redb_no_std))]
pub(crate) use source::NativeBranchKeyPool;

#[cfg(all(test, not(redb_no_std)))]
mod tests {
    use super::*;
    use crate::tree_store::{NativeNumericalEpoch, NativeReadContext, PageResolver};
    use crate::{NativeNumericalPool, ReadableDatabase, TableDefinition};
    use std::sync::{Arc, Barrier};
    const CEILING: usize = 64 * 1024 * 1024;
    fn fixture() -> (tempfile::NamedTempFile, crate::Database) {
        const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("actual_key_slot_rows");
        let file = crate::create_tempfile();
        let db = crate::Builder::new()
            .set_cache_size(0)
            .create(file.path())
            .unwrap();
        let write = db.begin_write().unwrap();
        for key in [b"real_one", b"real_two", b"real_tre"] {
            drop(
                write
                    .open_table(ROWS)
                    .unwrap()
                    .insert(key.as_slice(), b"value".as_slice())
                    .unwrap(),
            );
        }
        write.commit().unwrap();
        (file, db)
    }
    fn resolver(db: &crate::Database, numerical: &NativeNumericalEpoch) -> PageResolver {
        PageResolver::with_read_context(
            db.get_memory(),
            NativeReadContext::for_fixed_recipe(
                db.plan_current_read_context()
                    .unwrap()
                    .fixed_cleanup_limits_for_fixture(),
                numerical,
            )
            .unwrap(),
        )
    }
    // req: STO-020, REC-001
    #[test]
    fn native_branch_key_actual_three_arenas_two_separators_concurrent_return_and_retention() {
        let (_file, db) = fixture();
        let numerical_pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let numerical = numerical_pool.begin_managed_epoch(CEILING).unwrap();
        let actual = resolver(&db, &numerical);
        let resources = numerical.resources();
        let before = resources.current();
        // Three genuine input keys, each exactly eight encoded bytes. This is
        // only a finite slot fixture; the real fixed-executor demand is separate.
        let pool = NativeBranchKeyPool::prepare(&actual, &numerical, 3, 8).unwrap();
        let quote = NativeBranchKeyPool::capacity_bytes(3, 8).unwrap();
        assert_eq!(resources.current(), before + quote);
        let arrived = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let pool = pool.clone();
                let resources = resources.clone();
                let arrived = arrived.clone();
                let release = release.clone();
                std::thread::spawn(move || {
                    let mut key = pool.checkout(&resources, 1).unwrap();
                    key.push(b"real_one").unwrap();
                    let partial = BranchKeys::Source(key).into_parts();
                    assert!(partial.is_prepared());
                    assert_eq!(partial.iter().next(), Some(b"real_one".as_slice()));
                    arrived.wait();
                    release.wait();
                    drop(partial);
                })
            })
            .collect();
        arrived.wait();
        let mut third = pool.checkout(resources, 1).unwrap();
        third.push(b"real_tre").unwrap();
        assert!(
            third.push(b"real_two").is_err(),
            "original requested count prevents a second append"
        );
        assert_eq!(third.get(0), Some(b"real_tre".as_slice()));
        assert!(
            pool.checkout(resources, 1).is_err(),
            "no fourth arena or replacement"
        );
        assert!(pool.checkout(resources, 4).is_err());
        let first_separator = pool.separator(resources, b"real_one").unwrap();
        let second_separator = pool.separator(resources, b"real_two").unwrap();
        assert!(
            pool.separator(resources, b"real_tre").is_err(),
            "no third separator or replacement"
        );
        assert!(pool.separator(resources, b"too_wide!").is_err());
        assert_eq!(
            resources.current(),
            before + quote,
            "copy/refusal never grows pool charge"
        );
        drop(first_separator);
        drop(second_separator);
        drop(third);
        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        pool.require_available(resources).unwrap();
        let mut retained = pool.checkout(resources, 1).unwrap();
        retained.push(b"real_one").unwrap();
        let partial = BranchKeys::Source(retained).into_parts();
        drop(pool);
        assert!(
            resources.current() > before,
            "actual key backing outlives all pool wrappers"
        );
        assert_eq!(partial.iter().next(), Some(b"real_one".as_slice()));
        drop(partial);
        assert_eq!(
            resources.current(),
            before,
            "actual range/byte backing retires before final controls"
        );
    }
    // req: STO-020, REC-001
    #[test]
    fn native_branch_key_actual_same_counter_serial_refuses_and_poison_destroys_backing_first() {
        let (_file, db) = fixture();
        let numerical_pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let original = numerical_pool.begin_managed_epoch(CEILING).unwrap();
        let old_resources = original.resources().clone();
        let old_resolver = resolver(&db, &original);
        let old_pool = NativeBranchKeyPool::prepare(&old_resolver, &original, 3, 8).unwrap();
        let mut retained = old_pool.checkout(&old_resources, 1).unwrap();
        retained.push(b"real_one").unwrap();
        drop(original);
        let reopened = numerical_pool.begin_managed_epoch(CEILING).unwrap();
        let new_resolver = resolver(&db, &reopened);
        let new_pool = NativeBranchKeyPool::prepare(&new_resolver, &reopened, 3, 8).unwrap();
        let before = reopened.resources().current();
        assert!(new_pool.checkout(&old_resources, 1).is_err());
        assert!(new_pool.separator(&old_resources, b"real_one").is_err());
        assert!(old_pool.checkout(reopened.resources(), 1).is_err());
        assert!(
            old_pool
                .separator(reopened.resources(), b"real_one")
                .is_err()
        );
        assert!(retained.push(b"real_two").is_err());
        assert_eq!(retained.get(0), Some(b"real_one".as_slice()));
        let foreign_pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let foreign = foreign_pool.begin_managed_epoch(CEILING).unwrap();
        assert!(new_pool.checkout(foreign.resources(), 1).is_err());
        assert!(
            new_pool
                .separator(foreign.resources(), b"real_one")
                .is_err()
        );
        assert_eq!(reopened.resources().current(), before);
        drop(retained);
        drop(old_pool);
        drop(old_resolver);
        let mut retained = new_pool.checkout(reopened.resources(), 1).unwrap();
        retained.push(b"real_two").unwrap();
        // The owning module poisons the actual occupied slot mutex. No raw
        // buffer or numerical owner escapes; Drop must free the checkout bytes.
        new_pool.poison_first_slot_for_fixture();
        assert!(new_pool.checkout(reopened.resources(), 1).is_err());
        drop(new_pool);
        drop(new_resolver);
        assert!(reopened.resources().current() > 0);
        assert_eq!(retained.get(0), Some(b"real_two".as_slice()));
        drop(retained);
        assert_eq!(reopened.resources().current(), 0);
    }
}
