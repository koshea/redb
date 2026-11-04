use crate::tree_store::page_store::fast_hash::{FastHashMapU64, Shrink};
use alloc::collections::VecDeque;
use core::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct LRUCache<T> {
    // AtomicBool is the second chance flag. The u32 is the sequence number of the key's live
    // entry in lru_queue. remove() leaves the queue entry behind, and a later insert() of the
    // same key pushes a new one, so a queue entry whose sequence number does not match is stale.
    cache: FastHashMapU64<(T, AtomicBool, u32)>,
    lru_queue: VecDeque<(u64, u32)>,
    // Wrapping is harmless: a stale entry could only be mistaken for a live one if it survived
    // 2^32 inserts, and the queue is bounded to a small multiple of the cache size.
    next_seq: u32,
    // Public HashMap capacity can fall after an erase creates tombstones even
    // though its bucket allocation remains. This private map starts empty and
    // every insert/allocation passes below: a fresh allocation's capacity is
    // observed before any later erase. Shrinking never exceeds that allocation.
    // Retain the historical maximum rather than infer buckets from today's
    // lower-bound capacity. No map or mutable map reference escapes this type.
    #[cfg(not(redb_no_std))]
    hash_allocation_capacity_upper: usize,
    // Containers precede their allowance in physical destruction order.
    #[cfg(not(redb_no_std))]
    source_storage: Option<super::page_resources::NativeControlAllocation>,
}

impl<T> LRUCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: FastHashMapU64::default(),
            lru_queue: VecDeque::default(),
            next_seq: 0,
            #[cfg(not(redb_no_std))]
            hash_allocation_capacity_upper: 0,
            #[cfg(not(redb_no_std))]
            source_storage: None,
        }
    }

    pub(super) fn has_source_storage(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.source_storage.is_some()
        }
        #[cfg(redb_no_std)]
        {
            false
        }
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn source_storage_matches(
        &self,
        resources: &super::page_resources::NativePageResources,
    ) -> bool {
        self.source_storage
            .as_ref()
            .is_none_or(|guard| guard.belongs_to(resources))
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn queue_len(&self) -> usize {
        self.lru_queue.len()
    }

    // This fresh owner is constructed BEFORE any page-number/debug mutation.
    // reserve on an empty map has no tombstones; the pinned owning calculator
    // bounds precisely that fresh request. Queue growth has one external insert
    // of slack; all second-chance/requeue operations are pop-one/push-one.
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_storage(
        entries: usize,
        queued: usize,
        resources: Option<&super::page_resources::NativePageResources>,
    ) -> crate::Result<Self> {
        use super::page_resources::refused;
        let bytes = Self::prepared_storage_bytes(entries, queued)?;
        let allowance = resources
            .map(|owner| owner.admit_control(usize::try_from(bytes).map_err(|_| refused())?))
            .transpose()?;
        // Locals drop their real containers before the retained allowance on
        // every allocation failure. No partially allocated owner escapes.
        let mut result = Self::new();
        result.source_storage = allowance;
        result.cache.try_reserve(entries).map_err(|_| refused())?;
        result.hash_allocation_capacity_upper = result.cache.capacity();
        result
            .lru_queue
            .try_reserve_exact(queued)
            .map_err(|_| refused())?;
        if result.resource_capacity_bytes().map_err(|_| refused())? > bytes {
            return Err(refused());
        }
        Ok(result)
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn prepared_storage_bytes(entries: usize, queued: usize) -> crate::Result<u64> {
        use super::{container_bounds as bounds, page_resources::refused};
        let hash = bounds::hash_fresh_reserve_storage::<(u64, (T, AtomicBool, u32))>(entries)
            .map_err(|_| refused())?
            .allocator_bytes;
        let queue = bounds::vector_storage::<(u64, u32)>(queued)
            .map_err(|_| refused())?
            .allocator_bytes;
        hash.checked_add(queue).ok_or_else(refused)
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn replace_prepared_storage(&mut self, mut replacement: Self) {
        // The closed Source executor owns operations sequentially. Ordinary
        // pressure readers can remove/requeue existing entries but cannot add
        // an external key or grow queue length between preparation/selection.
        // Recheck the selected actual owner under its mutex before moving.
        assert!(replacement.cache.capacity() > self.cache.len());
        assert!(replacement.lru_queue.capacity() > self.lru_queue.len());
        replacement.next_seq = self.next_seq;
        for (key, entry) in self.cache.drain() {
            assert!(replacement.cache.insert(key, entry).is_none());
        }
        replacement.lru_queue.extend(self.lru_queue.drain(..));
        // Assignment destroys the OLD physical buckets/queue before its guard.
        // Values and their numerical owners moved together, without cloning.
        *self = replacement;
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
        if let Some((existing, second_chance, _)) = self.cache.get_mut(&key) {
            second_chance.store(false, Ordering::Release);
            return Some(core::mem::replace(existing, value));
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.cache.insert(key, (value, AtomicBool::new(false), seq));
        #[cfg(not(redb_no_std))]
        {
            self.hash_allocation_capacity_upper = self
                .hash_allocation_capacity_upper
                .max(self.cache.capacity());
        }
        self.lru_queue.push_back((key, seq));
        None
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
        let (value, _, _) = self.cache.remove(&key)?;
        if self.lru_queue.len() > 2 * self.cache.len() {
            // Cycle two elements of the LRU queue to ensure it doesn't grow without bound.
            // Stale entries are dropped, so each pass through the queue removes all of them.
            for _ in 0..2 {
                if let Some((removed_key, seq)) = self.lru_queue.pop_front()
                    && let Some(second_chance) = self.live_entry(removed_key, seq)
                {
                    second_chance.store(false, Ordering::Release);
                    self.lru_queue.push_back((removed_key, seq));
                }
            }
        }
        Some(value)
    }

    // Returns the second chance flag, if the queue entry is the live one for a cached key
    fn live_entry(&self, key: u64, seq: u32) -> Option<&AtomicBool> {
        self.cache
            .get(&key)
            .filter(|(_, _, live_seq)| *live_seq == seq)
            .map(|(_, second_chance, _)| second_chance)
    }

    pub(crate) fn get(&self, key: u64) -> Option<&T> {
        if let Some((value, second_chance, _)) = self.cache.get(&key) {
            second_chance.store(true, Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        if let Some((value, second_chance, _)) = self.cache.get_mut(&key) {
            second_chance.store(true, Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &T)> {
        self.cache.iter().map(|(k, (v, _, _))| (k, v))
    }

    pub(crate) fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut T)> {
        self.cache.iter_mut().map(|(k, (v, _, _))| (k, v))
    }

    pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
        while let Some((key, seq)) = self.lru_queue.pop_front() {
            if let Some(second_chance) = self.live_entry(key, seq) {
                if second_chance
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.lru_queue.push_back((key, seq));
                } else {
                    let (value, _, _) = self.cache.remove(&key).unwrap();
                    return Some((key, value));
                }
            }
        }
        None
    }

    pub(crate) fn clear(&mut self) {
        self.cache.shrink();
        self.cache.clear();
        self.lru_queue.shrink_to_fit();
        self.lru_queue.clear();
    }

    // Quote the actual private owner's container allocations. Values' nested
    // payloads and this inline LRU owner are accounted separately by the native
    // resource owner. Numerical bytes alone grant neither admission nor custody.
    pub(super) fn resource_capacity_bytes(
        &self,
    ) -> Result<u64, super::container_bounds::ContainerBoundError> {
        #[cfg(not(redb_no_std))]
        let hash = super::container_bounds::hash_storage_from_historical_fresh_capacity::<(
            u64,
            (T, AtomicBool, u32),
        )>(self.hash_allocation_capacity_upper)?
        .allocator_bytes;
        #[cfg(redb_no_std)]
        let hash =
            super::container_bounds::btree_storage::<u64, (T, AtomicBool, u32)>(self.cache.len())?;
        let queue =
            super::container_bounds::vector_storage::<(u64, u32)>(self.lru_queue.capacity())?
                .allocator_bytes;
        hash.checked_add(queue)
            .ok_or(super::container_bounds::ContainerBoundError::Overflow)
    }

    // Actual-owner capacity observation only. Production resource admission
    // must additionally bind this owner, budget future growth and preserve
    // numerical custody through complete physical destruction.
    #[cfg(all(test, not(redb_no_std)))]
    fn resource_capacity_for_test(&self) -> (usize, usize) {
        (
            self.hash_allocation_capacity_upper,
            self.lru_queue.capacity(),
        )
    }
}

// Source-owned write metadata must not remove/reinsert a borrowed None key:
// hashbrown may grow after tombstone erasure even at unchanged map length.
// A Some value remains represented by its None slot during writeback as well.
#[cfg(not(redb_no_std))]
impl<T> LRUCache<Option<T>> {
    pub(super) fn take_writeback_candidate(&mut self) -> Option<(u64, T)> {
        for _ in 0..self.cache.len() {
            let mut selected = None;
            while let Some((key, seq)) = self.lru_queue.pop_front() {
                let Some((value, second_chance, live_seq)) = self.cache.get_mut(&key) else {
                    continue;
                };
                if *live_seq != seq {
                    continue;
                }
                if second_chance
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.lru_queue.push_back((key, seq));
                    continue;
                }
                selected = Some((key, value.take()));
                break;
            }
            match selected {
                Some((key, Some(value))) => return Some((key, value)),
                Some((key, None)) => self.restore_writeback_candidate(key, None),
                None => break,
            }
        }
        None
    }

    pub(super) fn restore_writeback_candidate(&mut self, key: u64, value: Option<T>) {
        let (slot, second_chance, live_seq) = self.cache.get_mut(&key).unwrap();
        assert!(slot.is_none());
        *slot = value;
        second_chance.store(false, Ordering::Release);
        *live_seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        // Exactly the removed live queue slot is restored; capacity cannot grow.
        self.lru_queue.push_back((key, *live_seq));
    }

    pub(super) fn finish_writeback_candidate(&mut self, key: u64) {
        let (slot, _, _) = self.cache.remove(&key).unwrap();
        assert!(slot.is_none());
        // Original pop_lowest_priority removes directly, without remove()'s
        // stale-queue cycling. Preserve that exact successful-eviction behavior.
    }
}

#[cfg(test)]
mod test {
    use super::LRUCache;

    #[cfg(not(redb_no_std))]
    fn snapshot(
        cache: &LRUCache<Option<u64>>,
    ) -> (Vec<(u64, Option<u64>, bool, u32)>, Vec<(u64, u32)>, u32) {
        use core::sync::atomic::Ordering;
        let mut entries: Vec<_> = cache
            .cache
            .iter()
            .map(|(key, (value, flag, seq))| (*key, *value, flag.load(Ordering::Acquire), *seq))
            .collect();
        entries.sort_unstable_by_key(|entry| entry.0);
        (
            entries,
            cache.lru_queue.iter().copied().collect(),
            cache.next_seq,
        )
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn source_retained_none_requeue_matches_original_stale_and_wrapping_order() {
        fn fixture() -> LRUCache<Option<u64>> {
            let mut cache = LRUCache::new();
            cache.next_seq = u32::MAX - 3;
            for key in 1..=4 {
                cache.insert(key, Some(key));
            }
            cache.remove(2);
            cache.insert(2, Some(22)); // stale old queue entry and wrapped sequence
            cache.get_mut(1).unwrap().take(); // actual outstanding write slot
            cache.get_mut(2).unwrap().take();
            cache.get(3); // real second-chance flag before either selection
            cache
        }
        fn original(cache: &mut LRUCache<Option<u64>>) -> Option<(u64, u64)> {
            for _ in 0..cache.len() {
                let (key, value) = cache.pop_lowest_priority()?;
                if let Some(value) = value {
                    return Some((key, value));
                }
                cache.insert(key, None);
            }
            None
        }
        let mut old = fixture();
        let mut new = fixture();
        for _ in 0..4 {
            let expected = original(&mut old);
            let actual = new.take_writeback_candidate();
            assert_eq!(actual, expected);
            if let Some((key, _)) = actual {
                new.finish_writeback_candidate(key);
            }
            assert_eq!(snapshot(&new), snapshot(&old));
        }
        assert_eq!(
            new.len(),
            2,
            "borrowed slots survive repeated eviction attempts"
        );
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn source_retained_writeback_failure_matches_original_reinsert_without_growth() {
        let mut old = LRUCache::new();
        let mut new = LRUCache::prepare_storage(3, 3, None).unwrap();
        for key in 1..=3 {
            old.insert(key, Some(key));
            new.insert(key, Some(key));
        }
        old.get(1);
        new.get(1);
        let (key, value) = old.pop_lowest_priority().unwrap();
        let (actual_key, actual_value) = new.take_writeback_candidate().unwrap();
        assert_eq!((actual_key, Some(actual_value)), (key, value));
        let capacities = (new.cache.capacity(), new.lru_queue.capacity());
        old.insert(key, value); // exact original failed-writeback path
        new.restore_writeback_candidate(actual_key, Some(actual_value));
        assert_eq!(snapshot(&new), snapshot(&old));
        assert_eq!((new.cache.capacity(), new.lru_queue.capacity()), capacities);
        let retry = new.take_writeback_candidate().unwrap();
        assert_eq!(Some((retry.0, Some(retry.1))), old.pop_lowest_priority());
        new.finish_writeback_candidate(retry.0);
        assert_eq!(snapshot(&new), snapshot(&old));
    }

    // Writing to a cached page removes it from the read cache and re-inserts it on flush.
    // Nothing evicts when the cache never fills, so this must not grow the queue.
    #[test]
    fn queue_stays_bounded_without_eviction() {
        const KEYS: u64 = 64;

        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 0..KEYS {
            cache.insert(k, k);
        }
        for _ in 0..5_000 {
            for k in 0..KEYS {
                cache.remove(k);
                cache.insert(k, k);
            }
        }
        let keys = usize::try_from(KEYS).unwrap();
        assert_eq!(cache.len(), keys);
        assert!(cache.lru_queue.len() <= 3 * keys);

        // Every key is still evictable, exactly once
        let mut evicted = vec![];
        while let Some((k, _)) = cache.pop_lowest_priority() {
            evicted.push(k);
        }
        evicted.sort_unstable();
        assert_eq!(evicted, (0..KEYS).collect::<Vec<_>>());
        assert_eq!(cache.len(), 0);
        assert!(cache.lru_queue.is_empty());
    }

    // Churn concentrated on one key behind many long-lived keys
    #[test]
    fn queue_stays_bounded_with_hot_key() {
        const KEYS: u64 = 1024;

        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 0..KEYS {
            cache.insert(k, k);
        }
        for _ in 0..100_000 {
            cache.remove(KEYS - 1);
            cache.insert(KEYS - 1, KEYS - 1);
        }
        let keys = usize::try_from(KEYS).unwrap();
        assert_eq!(cache.len(), keys);
        assert!(cache.lru_queue.len() <= 3 * keys);
    }

    // A re-inserted key must be evicted at its new position, not at its stale one
    #[test]
    fn reinsert_moves_key_to_the_back() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 1..=3 {
            cache.insert(k, k);
        }
        cache.remove(1);
        cache.insert(1, 1);

        let mut evicted = vec![];
        while let Some((k, _)) = cache.pop_lowest_priority() {
            evicted.push(k);
        }
        assert_eq!(evicted, vec![2, 3, 1]);
    }

    #[test]
    fn second_chance_survives_one_eviction_pass() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 1..=3 {
            cache.insert(k, k);
        }
        assert_eq!(cache.get(1), Some(&1));

        assert_eq!(cache.pop_lowest_priority(), Some((2, 2)));
        assert_eq!(cache.pop_lowest_priority(), Some((3, 3)));
        assert_eq!(cache.pop_lowest_priority(), Some((1, 1)));
        assert_eq!(cache.pop_lowest_priority(), None);
    }

    #[test]
    fn replacing_a_value_keeps_its_queue_position() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        cache.insert(1, 1);
        cache.insert(2, 2);
        assert_eq!(cache.insert(1, 10), Some(1));
        assert_eq!(cache.lru_queue.len(), 2);

        assert_eq!(cache.pop_lowest_priority(), Some((1, 10)));
        assert_eq!(cache.pop_lowest_priority(), Some((2, 2)));
    }

    // req: STO-020, PRJ-004
    #[cfg(not(redb_no_std))]
    #[test]
    fn resource_capacity_survives_real_hash_tombstones_and_empty_queue() {
        use core::hash::{BuildHasher, Hasher};

        // Invert the unchanged native hasher's odd multiply/rotate, choosing
        // distinct keys with identical low32 bucket bits. No replacement hasher.
        fn colliding_key(index: u64) -> u64 {
            const INVERSE: u64 = 0x7814_94a5_5daa_ed0d;
            #[cfg(target_pointer_width = "64")]
            const ROTATE: u32 = 26;
            #[cfg(target_pointer_width = "32")]
            const ROTATE: u32 = 15;
            (index << 32).rotate_right(ROTATE).wrapping_mul(INVERSE)
        }

        let mut cache = LRUCache::new();
        for index in 0_u64..128 {
            let key = colliding_key(index);
            let mut hasher = cache.cache.hasher().build_hasher();
            hasher.write_u64(key);
            assert_eq!(hasher.finish(), index << 32);
            assert_eq!(hasher.finish() & u64::from(u32::MAX), 0);
            cache.insert(key, index);
        }
        let capacity = cache.cache.capacity();
        assert!((128..=256).contains(&capacity));
        for index in 128..u64::try_from(capacity).unwrap() {
            let key = colliding_key(index);
            let mut hasher = cache.cache.hasher().build_hasher();
            hasher.write_u64(key);
            assert_eq!(hasher.finish(), index << 32);
            assert_eq!(hasher.finish() & u64::from(u32::MAX), 0);
            cache.insert(key, index);
        }
        assert_eq!(cache.len(), capacity);
        assert_eq!(cache.resource_capacity_for_test().0, capacity);
        let retained_queue_capacity = cache.lru_queue.capacity();
        let retained_bytes = cache.resource_capacity_bytes().unwrap();
        assert!(retained_bytes > 0);
        let mut saw_underreported_capacity = false;
        for index in 0..u64::try_from(capacity).unwrap() {
            assert_eq!(cache.remove(colliding_key(index)), Some(index));
            saw_underreported_capacity |= cache.cache.capacity() < capacity;
            assert_eq!(cache.resource_capacity_for_test().0, capacity);
            assert_eq!(
                cache.resource_capacity_for_test().1,
                retained_queue_capacity
            );
            assert_eq!(
                cache.resource_capacity_bytes().unwrap(),
                retained_bytes,
                "actual byte quote retains hash allocation history and empty queue buffer"
            );
        }
        assert!(
            saw_underreported_capacity,
            "real deleted slots reduce public capacity without freeing buckets"
        );
        assert_eq!(cache.len(), 0);
        assert_eq!(
            cache.resource_capacity_for_test(),
            (capacity, retained_queue_capacity)
        );
        cache.clear();
        assert_eq!(
            cache.resource_capacity_for_test().0,
            capacity,
            "historical upper remains conservative after a physical shrink"
        );
        assert!(cache.lru_queue.capacity() <= retained_queue_capacity);
        for key in 0_u64..32 {
            cache.insert(key, key + 1);
        }
        for key in 0_u64..32 {
            assert_eq!(cache.get(key), Some(&(key + 1)));
        }
        assert_eq!(cache.resource_capacity_for_test().0, capacity);
    }
}
