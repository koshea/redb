use crate::tree_store::page_store::base::PageNumber;
use std::cell::RefCell;
use std::rc::Rc;

// Number of slots per thread. Sized to hold roughly the top two levels below the root
// of a large btree. Must be a power of two.
const SLOTS: usize = 128;
// Number of slots probed per page. Trying a second slot greatly reduces the number of
// hot pages that go uncached due to slot collisions.
const PROBE_LEN: usize = 2;

/// A small, bounded, thread-local cache of the hottest btree pages, used by lookups in
/// read transactions.
///
/// Concurrent readers of the same hot pages (the top of a btree) contend in the shared
/// page cache: every lookup acquires a lock stripe, updates the LRU second-chance flag,
/// and increments the page's `Arc` refcount, all of which are writes to memory shared
/// between threads. This cache gives each thread its own copy of the hot pages, so
/// serving them involves no shared memory at all: the cache and everything in it is
/// thread-local, and entries hold private copies of the page bytes rather than
/// references into the shared page cache, so hits touch only this thread's memory.
/// That is also why the implementation needs no atomics or locks: `Rc` and `RefCell`
/// suffice.
///
/// Entries are validated by tagging them with the database instance and the snapshot
/// generation (the read transaction's id) they were copied under, which the lookup
/// compares. A page number's bytes cannot change within a generation: pages reachable
/// from a live read snapshot are immutable (writes are copy-on-write), and a freed page
/// number can only be reused by a later write transaction, whose commit advances the
/// generation. A stale entry therefore never matches; it is simply overwritten. This
/// also means the cache stays warm across transactions while the database is not
/// written to, e.g. a sequence of short read transactions on separate tables.
///
/// The memory bound is `SLOTS` single (order-zero) pages per thread that reads a
/// database, e.g. 512KiB per thread with 4KiB pages, plus one page-copy memcpy per
/// fill. Only pages actually read are cached. Write transactions must not use the
/// cache (they can free and reallocate pages mid-transaction, within one generation),
/// and only branch pages are cached: leaf pages must be returned as `PageImpl`s from
/// the shared cache anyway, because `AccessGuard`s hold them.
pub(crate) struct ThreadLocalPageCache {
    slots: Box<[Option<CacheEntry>]>,
}

struct CacheEntry {
    tag: CacheTag,
    page_number: PageNumber,
    bytes: Rc<[u8]>,
}

/// Identifies the snapshot that a cached page copy is valid for.
#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) struct CacheTag {
    /// `TransactionalMemory::cache_instance_id` of the database
    pub(crate) instance: u64,
    /// The read transaction's id: the id of the last committed transaction when it
    /// began. All read transactions on an unchanged database share it.
    pub(crate) generation: u64,
}

thread_local! {
    static PAGE_CACHE: RefCell<ThreadLocalPageCache> = RefCell::new(ThreadLocalPageCache::new());
}

/// Runs `f` with this thread's page cache, or with `None` if the cache is unavailable:
/// either a reentrant call (user key-comparison code invoking a lookup while a lookup
/// holds the cache) or a thread whose thread-locals are being destroyed. Lookups run
/// uncached in those cases rather than panicking.
pub(crate) fn with_thread_local_page_cache<R>(
    f: impl FnOnce(Option<&mut ThreadLocalPageCache>) -> R,
) -> R {
    let mut f = Some(f);
    PAGE_CACHE
        .try_with(|cell| {
            let f = f.take().unwrap();
            match cell.try_borrow_mut() {
                Ok(mut cache) => f(Some(&mut cache)),
                Err(_) => f(None),
            }
        })
        .unwrap_or_else(|_| (f.take().unwrap())(None))
}

impl ThreadLocalPageCache {
    fn new() -> Self {
        Self {
            slots: (0..SLOTS).map(|_| None).collect(),
        }
    }

    fn probe_indices(page_number: PageNumber) -> [usize; PROBE_LEN] {
        let packed = u64::from_le_bytes(page_number.to_le_bytes());
        // Fibonacci hashing: the high bits of the product are well mixed
        let index = (packed.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize % SLOTS;
        [index, (index + 1) % SLOTS]
    }

    /// Returns the cached copy of the page, if present and valid for `tag`.
    pub(crate) fn get(&self, tag: CacheTag, page_number: PageNumber) -> Option<Rc<[u8]>> {
        for i in Self::probe_indices(page_number) {
            if let Some(entry) = &self.slots[i]
                && entry.page_number == page_number
                && entry.tag == tag
            {
                return Some(entry.bytes.clone());
            }
        }
        None
    }

    /// Caches a copy of `bytes`, preferring an empty or stale slot and otherwise
    /// evicting the second probe slot, so that a new hot set (a different table, or the
    /// next generation) displaces an old one instead of going uncached.
    pub(crate) fn insert(&mut self, tag: CacheTag, page_number: PageNumber, bytes: &[u8]) {
        let indices = Self::probe_indices(page_number);
        let slot = indices
            .into_iter()
            .find(|i| {
                self.slots[*i]
                    .as_ref()
                    .is_none_or(|entry| entry.tag != tag || entry.page_number == page_number)
            })
            .unwrap_or(indices[PROBE_LEN - 1]);
        self.slots[slot] = Some(CacheEntry {
            tag,
            page_number,
            bytes: Rc::from(bytes),
        });
    }
}

#[cfg(test)]
mod test {
    use super::{CacheTag, SLOTS, ThreadLocalPageCache};
    use crate::tree_store::page_store::base::PageNumber;

    fn page(index: u32) -> PageNumber {
        PageNumber::new(0, index, 0)
    }

    #[test]
    fn hit_requires_matching_tag() {
        let mut cache = ThreadLocalPageCache::new();
        let tag_a = CacheTag {
            instance: 1,
            generation: 7,
        };
        let other_generation = CacheTag {
            instance: 1,
            generation: 8,
        };
        let other_database = CacheTag {
            instance: 2,
            generation: 7,
        };

        assert!(cache.get(tag_a, page(1)).is_none());
        cache.insert(tag_a, page(1), &[1, 2, 3]);
        assert_eq!(cache.get(tag_a, page(1)).unwrap().as_ref(), [1, 2, 3]);
        assert!(cache.get(other_generation, page(1)).is_none());
        assert!(cache.get(other_database, page(1)).is_none());
        assert!(cache.get(tag_a, page(2)).is_none());

        // The next generation's copy of the same page number replaces the stale entry
        cache.insert(other_generation, page(1), &[9]);
        assert_eq!(cache.get(other_generation, page(1)).unwrap().as_ref(), [9]);
        assert!(cache.get(tag_a, page(1)).is_none());
    }

    #[test]
    fn bounded_with_eviction() {
        let mut cache = ThreadLocalPageCache::new();
        let tag = CacheTag {
            instance: 1,
            generation: 1,
        };

        let count = u32::try_from(3 * SLOTS).unwrap();
        // Insert far more pages than there are slots
        for i in 0..count {
            let bytes = i.to_le_bytes();
            cache.insert(tag, page(i), &bytes);
        }

        // Every lookup returns the exact requested page's bytes or nothing
        let mut hits = 0;
        for i in 0..count {
            if let Some(bytes) = cache.get(tag, page(i)) {
                assert_eq!(bytes.as_ref(), i.to_le_bytes());
                hits += 1;
            }
        }
        // The cache can never hold more entries than slots, but eviction keeps
        // recently inserted pages available
        assert!(hits <= SLOTS);
        assert!(hits > 0);

        // A page inserted when its slots were full is still served after evicting
        let colliding = count + 1;
        let bytes = colliding.to_le_bytes();
        cache.insert(tag, page(colliding), &bytes);
        assert_eq!(
            cache.get(tag, page(colliding)).unwrap().as_ref(),
            colliding.to_le_bytes()
        );
    }

    #[test]
    fn reinserting_same_page_updates_in_place() {
        let mut cache = ThreadLocalPageCache::new();
        let tag = CacheTag {
            instance: 1,
            generation: 1,
        };
        cache.insert(tag, page(1), &[1]);
        cache.insert(tag, page(1), &[1]);
        // Both probe slots must not be consumed by the same page
        let mut occupied = 0;
        for slot in &cache.slots {
            if slot.is_some() {
                occupied += 1;
            }
        }
        assert_eq!(occupied, 1);
    }
}
