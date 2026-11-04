//! Complete bounded numerical allocator-table observation, never write authority.
use super::resource_key_paths::{NativeKeyPathError, SUPPORTED_PATH_DEPTH, check_layout};
use super::{
    BRANCH, BranchAccessor, Btree, Checksum, DEFERRED, LEAF, LeafAccessor, Page, PageNumber,
    branch_checksum, leaf_checksum,
};
use crate::Value;
use crate::transaction_tracker::NativeResourceEpoch;
use crate::transactions::AllocatorStateKey;

type PurgeResult<T> = core::result::Result<T, NativeKeyPathError>;
fn add(a: u64, b: u64) -> PurgeResult<u64> {
    a.checked_add(b).ok_or(NativeKeyPathError::Overflow)
}

// Numerical work preflight from the ACTUAL header, not a completeness certificate
// or a supported shipping latency policy. The Source factory still owes admission.
pub(crate) struct NativeAllocatorPurgeWork {
    fetches_left: u64,
    bytes_left: u64,
    keys_left: u64,
    fetches: u64,
    bytes: u64,
    key_slots: u64,
    maximum_page_bytes: u64,
}
impl NativeAllocatorPurgeWork {
    pub(crate) fn from_actual_header(rows: u64, maximum_page_bytes: u64) -> PurgeResult<Self> {
        // L<=N, B<=L-1: traversal <=2*M-1, plus constructor cached root =>4*N-2.
        let fetches_left = if rows == 0 {
            0
        } else {
            rows.checked_mul(4)
                .and_then(|n| n.checked_sub(2))
                .ok_or(NativeKeyPathError::Overflow)?
        };
        Ok(Self {
            fetches_left,
            bytes_left: fetches_left
                .checked_mul(maximum_page_bytes)
                .ok_or(NativeKeyPathError::Overflow)?,
            // Leaf framing checks key slots and value slots in two loops;
            // global leaf ordering adds a third. Branch framing uses two.
            keys_left: fetches_left
                .checked_mul(3 * u64::from(u16::MAX))
                .ok_or(NativeKeyPathError::Overflow)?,
            fetches: 0,
            bytes: 0,
            key_slots: 0,
            maximum_page_bytes,
        })
    }
    // BEFORE each native fetch, including Btree::new's cached root. All checked
    // decrements are computed first, so a refusal does not partially consume work.
    pub(crate) fn before_fetch(&mut self, page: PageNumber, base: usize) -> PurgeResult<u64> {
        let base = u64::try_from(base).map_err(|_| NativeKeyPathError::Overflow)?;
        if page.page_order > 31 {
            return Err(NativeKeyPathError::Malformed);
        }
        let bytes = base
            .checked_mul(1_u64 << page.page_order)
            .ok_or(NativeKeyPathError::Overflow)?;
        if bytes > self.maximum_page_bytes {
            return Err(NativeKeyPathError::Malformed);
        }
        let left = self
            .fetches_left
            .checked_sub(1)
            .ok_or(NativeKeyPathError::VisitsExceeded)?;
        let bytes_left = self
            .bytes_left
            .checked_sub(bytes)
            .ok_or(NativeKeyPathError::VisitsExceeded)?;
        let fetches = add(self.fetches, 1)?;
        let total = add(self.bytes, bytes)?;
        self.fetches_left = left;
        self.bytes_left = bytes_left;
        self.fetches = fetches;
        self.bytes = total;
        Ok(bytes)
    }
    fn before_layout(&mut self, bytes: &[u8]) -> PurgeResult<()> {
        let raw = bytes.get(2..4).ok_or(NativeKeyPathError::Malformed)?;
        let passes = match bytes.first() {
            Some(&LEAF) => 3,   // key framing, value framing, global key order
            Some(&BRANCH) => 2, // key framing, native key-access preflight
            _ => return Err(NativeKeyPathError::Malformed),
        };
        let count = passes * u64::from(u16::from_le_bytes([raw[0], raw[1]]));
        let left = self
            .keys_left
            .checked_sub(count)
            .ok_or(NativeKeyPathError::VisitsExceeded)?;
        let used = add(self.key_slots, count)?;
        self.keys_left = left;
        self.key_slots = used;
        Ok(())
    }
    pub(crate) fn fetched(&self) -> u64 {
        self.fetches
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn shorten_for_test(&mut self, fetches: u64, bytes: u64, keys: u64) {
        self.fetches_left = self.fetches_left.min(fetches);
        self.bytes_left = self.bytes_left.min(bytes);
        self.keys_left = self.keys_left.min(keys);
    }
}

#[derive(Default, Debug)]
pub(crate) struct NativeAllocatorPurgeFacts {
    pub(crate) nodes: u64,
    pub(crate) leaves: u64,
    pub(crate) branches: u64,
    pub(crate) rows: u64,
    pub(crate) maximum_depth: usize,
    pub(crate) maximum_order: u8,
    pub(crate) page_bytes: u64,
    pub(crate) base_pages: u64,
    pub(crate) maximum_ancestor_page_bytes: u64,
    pub(crate) maximum_page_bytes: u64,
    pub(crate) fetches_including_cached_root: u64,
    pub(crate) fetched_bytes: u64,
    pub(crate) key_slots: u64,
}
#[derive(Clone, Copy)]
struct Frame {
    page: PageNumber,
    checksum: Checksum,
    children: usize,
    next_child: usize,
    path_bytes: u64,
}
impl Btree<AllocatorStateKey, &'static [u8]> {
    pub(crate) fn inspect_complete_allocator_purge(
        &self,
        epoch: &NativeResourceEpoch<'_>,
        work: &mut NativeAllocatorPurgeWork,
    ) -> PurgeResult<NativeAllocatorPurgeFacts> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let tracker = parts.actual_tracker();
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        self.require_current_native_resource_read()?;
        let result = self.inspect_allocator_inner(work)?;
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        Ok(result)
    }
    fn inspect_allocator_inner(
        &self,
        work: &mut NativeAllocatorPurgeWork,
    ) -> PurgeResult<NativeAllocatorPurgeFacts> {
        let mut facts = NativeAllocatorPurgeFacts::default();
        let Some(root) = self.root else {
            return Ok(facts);
        };
        if root.length == 0 {
            return Err(NativeKeyPathError::Malformed);
        }
        let max_nodes = root
            .length
            .checked_mul(2)
            .and_then(|n| n.checked_sub(1))
            .ok_or(NativeKeyPathError::Overflow)?;
        let mut stack = [None::<Frame>; SUPPORTED_PATH_DEPTH];
        let mut depth = 0;
        let mut next = (root.root, root.checksum);
        let mut last = None::<AllocatorStateKey>;
        loop {
            if depth >= SUPPORTED_PATH_DEPTH {
                return Err(NativeKeyPathError::UnsupportedDepth);
            }
            if stack[..depth]
                .iter()
                .flatten()
                .any(|frame| frame.page == next.0)
            {
                return Err(NativeKeyPathError::Malformed);
            }
            if next.1 == DEFERRED {
                return Err(NativeKeyPathError::UnsupportedState);
            }
            let expected_bytes = work.before_fetch(next.0, self.mem.resource_base_page_size())?;
            let page = self.mem.get_page(next.0, self.hint)?;
            if u64::try_from(page.memory().len()).map_err(|_| NativeKeyPathError::Overflow)?
                != expected_bytes
            {
                return Err(NativeKeyPathError::Malformed);
            }
            work.before_layout(page.memory())?;
            check_layout::<AllocatorStateKey, &'static [u8]>(&page)?;
            facts.nodes = add(facts.nodes, 1)?;
            if facts.nodes > max_nodes {
                return Err(NativeKeyPathError::Malformed);
            }
            facts.maximum_depth = facts.maximum_depth.max(depth + 1);
            facts.maximum_order = facts.maximum_order.max(next.0.page_order);
            facts.page_bytes = add(facts.page_bytes, expected_bytes)?;
            facts.base_pages = add(facts.base_pages, next.0.page_size_bytes(1))?;
            facts.maximum_page_bytes = facts.maximum_page_bytes.max(expected_bytes);
            let ancestor = if depth == 0 {
                0
            } else {
                stack[depth - 1]
                    .ok_or(NativeKeyPathError::Malformed)?
                    .path_bytes
            };
            let path_bytes = add(ancestor, expected_bytes)?;
            facts.maximum_ancestor_page_bytes = facts.maximum_ancestor_page_bytes.max(path_bytes);
            match page.memory()[0] {
                BRANCH => {
                    if branch_checksum(&page, AllocatorStateKey::fixed_width())? != next.1 {
                        return Err(NativeKeyPathError::Malformed);
                    }
                    facts.branches = add(facts.branches, 1)?;
                    let branch = BranchAccessor::new(&page, AllocatorStateKey::fixed_width());
                    let child = (
                        branch.child_page(0).ok_or(NativeKeyPathError::Malformed)?,
                        branch
                            .child_checksum(0)
                            .ok_or(NativeKeyPathError::Malformed)?,
                    );
                    stack[depth] = Some(Frame {
                        page: next.0,
                        checksum: next.1,
                        children: branch.count_children(),
                        next_child: 1,
                        path_bytes,
                    });
                    drop(page); // No ancestor PageImpl is retained by this observer.
                    depth += 1;
                    next = child;
                    continue;
                }
                LEAF => {
                    if leaf_checksum(&page, AllocatorStateKey::fixed_width(), None)? != next.1 {
                        return Err(NativeKeyPathError::Malformed);
                    }
                    facts.leaves = add(facts.leaves, 1)?;
                    let leaf =
                        LeafAccessor::new(page.memory(), AllocatorStateKey::fixed_width(), None);
                    for index in 0..leaf.num_pairs() {
                        let (key, _) = leaf
                            .entry_ranges(index)
                            .ok_or(NativeKeyPathError::Malformed)?;
                        let key = AllocatorStateKey::from_bytes(&page.memory()[key]);
                        if last.is_some_and(|previous| previous >= key) {
                            return Err(NativeKeyPathError::Malformed);
                        }
                        last = Some(key);
                        facts.rows = add(facts.rows, 1)?;
                        if facts.rows > root.length {
                            return Err(NativeKeyPathError::Malformed);
                        }
                    }
                }
                _ => return Err(NativeKeyPathError::Malformed),
            }
            drop(page);
            let mut found = false;
            while depth != 0 {
                let frame = stack[depth - 1]
                    .as_mut()
                    .ok_or(NativeKeyPathError::Malformed)?;
                if frame.next_child == frame.children {
                    stack[depth - 1] = None;
                    depth -= 1;
                    continue;
                }
                // Re-read this parent only for a later child, with the same
                // native checks/CPU work and without retaining sibling aliases.
                let expected = work.before_fetch(frame.page, self.mem.resource_base_page_size())?;
                let parent = self.mem.get_page(frame.page, self.hint)?;
                if u64::try_from(parent.memory().len()).map_err(|_| NativeKeyPathError::Overflow)?
                    != expected
                {
                    return Err(NativeKeyPathError::Malformed);
                }
                work.before_layout(parent.memory())?;
                check_layout::<AllocatorStateKey, &'static [u8]>(&parent)?;
                if parent.memory()[0] != BRANCH
                    || branch_checksum(&parent, AllocatorStateKey::fixed_width())? != frame.checksum
                {
                    return Err(NativeKeyPathError::Malformed);
                }
                let branch = BranchAccessor::new(&parent, AllocatorStateKey::fixed_width());
                if branch.count_children() != frame.children {
                    return Err(NativeKeyPathError::Malformed);
                }
                next = (
                    branch
                        .child_page(frame.next_child)
                        .ok_or(NativeKeyPathError::Malformed)?,
                    branch
                        .child_checksum(frame.next_child)
                        .ok_or(NativeKeyPathError::Malformed)?,
                );
                frame.next_child += 1;
                drop(parent);
                found = true;
                break;
            }
            if !found {
                break;
            }
        }
        if facts.rows != root.length {
            return Err(NativeKeyPathError::Malformed);
        }
        facts.fetches_including_cached_root = work.fetches;
        facts.fetched_bytes = work.bytes;
        facts.key_slots = work.key_slots;
        Ok(facts)
    }
}

#[cfg(all(test, not(redb_no_std)))]
impl Btree<AllocatorStateKey, &'static [u8]> {
    pub(crate) fn alter_allocator_purge_header_for_test(&mut self, checksum: bool) {
        let root = self.root.as_mut().unwrap();
        if checksum {
            root.checksum ^= 1;
        } else {
            root.length += 1;
        }
    }
}
