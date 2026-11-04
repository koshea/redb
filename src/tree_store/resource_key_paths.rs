//! Fixed-work native key descent; no height certificate or mutation authority.
use super::{
    BRANCH, BranchAccessor, Btree, Key, LEAF, LeafAccessor, Page, PageNumber, Value,
    branch_checksum, leaf_checksum,
};
use crate::StorageError;
use crate::transaction_tracker::NativeResourceEpoch;
use crate::transactions::{AllocatorStateKey, TransactionIdWithPagination};
use core::any::TypeId;

pub(super) const SUPPORTED_PATH_DEPTH: usize = 64;

#[derive(Debug)]
pub(crate) enum NativeKeyPathError {
    VisitsExceeded,
    UnsupportedDepth,
    Unpinned,
    ForeignOwner,
    UnsupportedKey,
    UnsupportedState,
    Epoch(crate::transaction_tracker::NativeResourcePlanningRefusal),
    Malformed,
    Overflow,
    Native(StorageError),
}
impl From<StorageError> for NativeKeyPathError {
    fn from(error: StorageError) -> Self {
        Self::Native(error)
    }
}
type PathResult<T> = core::result::Result<T, NativeKeyPathError>;

// Only the closed compiled-recipe owner may use this numerical budget. The
// constructor grants neither physical-read admission nor a source capability.
pub(crate) struct CompiledPathVisits {
    remaining: usize,
    visited: usize,
}
impl CompiledPathVisits {
    pub(crate) fn for_prepared_keys(keys: &[NativePreparedNormalKey<'_>]) -> PathResult<Self> {
        let remaining = keys
            .len()
            .checked_mul(SUPPORTED_PATH_DEPTH)
            .ok_or(NativeKeyPathError::Overflow)?;
        Ok(Self {
            remaining,
            visited: 0,
        })
    }
    pub(super) fn visit(&mut self) -> PathResult<()> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or(NativeKeyPathError::VisitsExceeded)?;
        self.visited = self
            .visited
            .checked_add(1)
            .ok_or(NativeKeyPathError::Overflow)?;
        Ok(())
    }
    // A first search may exhaust its leaf, then descend into the next
    // captured sibling. Stored sibling descriptors avoid parent re-reads:
    // at most two native depth-64 paths are fetched for one exact probe.
    pub(crate) fn for_freed_probe() -> Self {
        Self {
            remaining: 2 * SUPPORTED_PATH_DEPTH,
            visited: 0,
        }
    }
    pub(crate) fn visited(&self) -> usize {
        self.visited
    }
}

#[derive(Clone, Copy)]
pub(crate) struct NativeKeyPathCost {
    pub(super) depth: usize,
    pub(super) observed_page_bytes: u64,
    pub(super) observed_base_pages: u64,
}
impl NativeKeyPathCost {
    pub(crate) fn depth(&self) -> usize {
        self.depth
    }
    pub(crate) fn observed_page_bytes(&self) -> u64 {
        self.observed_page_bytes
    }
    pub(crate) fn observed_base_pages(&self) -> u64 {
        self.observed_base_pages
    }
}

pub(crate) struct NativeBorrowedKeyPath<'tree, K: Key + 'static, V: Value + 'static> {
    actual: &'tree Btree<K, V>,
    cost: NativeKeyPathCost,
}
impl<K: Key, V: Value> NativeBorrowedKeyPath<'_, K, V> {
    pub(crate) fn cost(&self) -> NativeKeyPathCost {
        self.cost
    }
    pub(crate) fn same_tree(&self, actual: &Btree<K, V>) -> bool {
        core::ptr::eq(self.actual, actual)
    }
}
impl<K: Key, V: Value> Drop for NativeBorrowedKeyPath<'_, K, V> {
    fn drop(&mut self) {}
}

fn checked_add(a: usize, b: usize) -> PathResult<usize> {
    a.checked_add(b).ok_or(NativeKeyPathError::Overflow)
}
fn checked_mul(a: usize, b: usize) -> PathResult<usize> {
    a.checked_mul(b).ok_or(NativeKeyPathError::Overflow)
}
fn key_supported<K: Key + 'static>() -> bool {
    let ty = TypeId::of::<K>();
    ty == TypeId::of::<&'static [u8]>()
        || ty == TypeId::of::<&'static str>()
        || ty == TypeId::of::<u64>()
        || ty == TypeId::of::<TransactionIdWithPagination>()
        || ty == TypeId::of::<AllocatorStateKey>()
}
fn check_key<K: Key + 'static>(key: &[u8]) -> PathResult<()> {
    if K::fixed_width().is_some_and(|size| size != key.len()) {
        return Err(NativeKeyPathError::Malformed);
    }
    if (TypeId::of::<K>() == TypeId::of::<&'static str>()
        || TypeId::of::<K>() == TypeId::of::<alloc::string::String>())
        && core::str::from_utf8(key).is_err()
    {
        return Err(NativeKeyPathError::Malformed);
    }
    if TypeId::of::<K>() == TypeId::of::<AllocatorStateKey>()
        && key.first().is_none_or(|tag| *tag > 5)
    {
        return Err(NativeKeyPathError::Malformed);
    }
    Ok(())
}

// Native-owned framing preflight: all offsets used by the existing native
// accessors/comparator are in range and ordered BEFORE they are used. It is a
// finite one-page inspection, never a population walk or semantic row decoder.
pub(super) fn check_layout<K: Key + 'static, V: Value>(page: &impl Page) -> PathResult<()> {
    let bytes = page.memory();
    let count_bytes = bytes.get(2..4).ok_or(NativeKeyPathError::Malformed)?;
    let count = usize::from(u16::from_le_bytes([count_bytes[0], count_bytes[1]]));
    if count == 0 {
        return Err(NativeKeyPathError::Malformed);
    }
    let fixed_key = K::fixed_width();
    let fixed_value = V::fixed_width();
    match bytes.first().copied() {
        Some(LEAF) => {
            let arrays = usize::from(fixed_key.is_none()) + usize::from(fixed_value.is_none());
            let start = checked_add(4, checked_mul(checked_mul(arrays, count)?, 4)?)?;
            if start > bytes.len() {
                return Err(NativeKeyPathError::Malformed);
            }
            if let Some(width) = fixed_key {
                if checked_add(start, checked_mul(width, count)?)? > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
            }
            // Check fixed-value total before entry_ranges() can add offsets.
            let last_key_end = if let Some(width) = fixed_key {
                checked_add(start, checked_mul(width, count)?)?
            } else {
                let offset = checked_add(4, checked_mul(count - 1, 4)?)?;
                let raw = bytes
                    .get(offset..checked_add(offset, 4)?)
                    .ok_or(NativeKeyPathError::Malformed)?;
                u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize
            };
            if last_key_end > bytes.len() {
                return Err(NativeKeyPathError::Malformed);
            }
            if let Some(width) = fixed_value {
                if checked_add(last_key_end, checked_mul(width, count)?)? > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
            }
            let accessor = LeafAccessor::new(bytes, fixed_key, fixed_value);
            let mut key_end = start;
            for index in 0..count {
                let (key, _) = accessor
                    .entry_ranges(index)
                    .ok_or(NativeKeyPathError::Malformed)?;
                if key.start != key_end || key.end < key.start || key.end > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
                check_key::<K>(&bytes[key.clone()])?;
                key_end = key.end;
            }
            if let Some(width) = fixed_value {
                if checked_add(key_end, checked_mul(width, count)?)? > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
            }
            let mut value_end = key_end;
            for index in 0..count {
                let (_, value) = accessor
                    .entry_ranges(index)
                    .ok_or(NativeKeyPathError::Malformed)?;
                if value.start != value_end || value.end < value.start || value.end > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
                value_end = value.end;
            }
            Ok(())
        }
        Some(BRANCH) => {
            let children = checked_add(count, 1)?;
            let start = checked_add(
                8,
                checked_add(
                    checked_mul(children, 24)?,
                    if fixed_key.is_none() {
                        checked_mul(count, 4)?
                    } else {
                        0
                    },
                )?,
            )?;
            if start > bytes.len() {
                return Err(NativeKeyPathError::Malformed);
            }
            if let Some(width) = fixed_key {
                if checked_add(start, checked_mul(width, count)?)? > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
            }
            let accessor = BranchAccessor::new(page, fixed_key);
            let mut prior = start;
            // The accessor's key() slices internally. Validate the two encoded
            // endpoints first, then use that same existing accessor/comparator.
            for index in 0..count {
                let end = if let Some(width) = fixed_key {
                    checked_add(start, checked_mul(width, index + 1)?)?
                } else {
                    let offset = checked_add(
                        checked_add(8, checked_mul(children, 24)?)?,
                        checked_mul(index, 4)?,
                    )?;
                    let end = bytes
                        .get(offset..checked_add(offset, 4)?)
                        .ok_or(NativeKeyPathError::Malformed)?;
                    u32::from_le_bytes([end[0], end[1], end[2], end[3]]) as usize
                };
                if end < prior || end > bytes.len() {
                    return Err(NativeKeyPathError::Malformed);
                }
                check_key::<K>(&bytes[prior..end])?;
                prior = end;
            }
            // The previous loop established that native key() cannot panic.
            for index in 0..count {
                let _ = accessor.key(index).ok_or(NativeKeyPathError::Malformed)?;
            }
            Ok(())
        }
        _ => Err(NativeKeyPathError::Malformed),
    }
}

pub(in crate::tree_store) fn check_btree_key<K: Key + 'static>(key: &[u8]) -> crate::Result<()> {
    check_key::<K>(key).map_err(|_| StorageError::Corrupted("Corrupted btree key".into()))
}

pub(in crate::tree_store) fn check_btree_node<K: Key + 'static, V: Value + 'static>(
    page: &impl Page,
) -> crate::Result<()> {
    check_layout::<K, V>(page)
        .map_err(|_| StorageError::Corrupted("Corrupted btree node".into()))?;
    let bytes = page.memory();
    if bytes.first() == Some(&LEAF)
        && TypeId::of::<V>() == TypeId::of::<crate::tree_store::InternalTableDefinition>()
    {
        let accessor = LeafAccessor::new(bytes, K::fixed_width(), V::fixed_width());
        for index in 0..accessor.num_pairs() {
            let (_, value) = accessor
                .entry_ranges(index)
                .ok_or_else(|| StorageError::Corrupted("Corrupted btree node".into()))?;
            if crate::tree_store::InternalTableDefinition::native_resource_matches::<
                AllocatorStateKey,
                &'static [u8],
            >(&bytes[value])
            .is_err()
            {
                return Err(StorageError::Corrupted(
                    "Malformed native table definition".into(),
                ));
            }
        }
    }
    if bytes.first() == Some(&LEAF)
        && TypeId::of::<K>() == TypeId::of::<AllocatorStateKey>()
        && TypeId::of::<V>() == TypeId::of::<&'static [u8]>()
    {
        let accessor = LeafAccessor::new(bytes, K::fixed_width(), V::fixed_width());
        for index in 0..accessor.num_pairs() {
            let (key, value) = accessor
                .entry_ranges(index)
                .ok_or_else(|| StorageError::Corrupted("Corrupted btree node".into()))?;
            if bytes.get(key.start) == Some(&5) && value.len() != core::mem::size_of::<u64>() {
                return Err(StorageError::Corrupted("Corrupted btree node".into()));
            }
        }
    }
    Ok(())
}

impl<K: Key, V: Value> Btree<K, V> {
    pub(super) fn require_current_native_resource_read(&self) -> PathResult<()> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let memory = parts.actual_memory();
        let transaction_id = parts.transaction_id();
        if !self
            .mem
            .resource_matches_current_read(memory, transaction_id)?
        {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        Ok(())
    }
    pub(crate) fn inspect_native_resource_key<'tree>(
        &'tree self,
        key: &K::SelfType<'_>,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<NativeBorrowedKeyPath<'tree, K, V>> {
        if !key_supported::<K>() {
            return Err(NativeKeyPathError::UnsupportedKey);
        }
        let query = K::as_bytes(key);
        self.inspect_encoded_native_resource_key(query.as_ref(), visits)
    }
    fn inspect_encoded_native_resource_key<'tree>(
        &'tree self,
        query: &[u8],
        visits: &mut CompiledPathVisits,
    ) -> PathResult<NativeBorrowedKeyPath<'tree, K, V>> {
        if !key_supported::<K>() {
            return Err(NativeKeyPathError::UnsupportedKey);
        }
        self.require_current_native_resource_read()?;
        let mut cost = NativeKeyPathCost {
            depth: 0,
            observed_page_bytes: 0,
            observed_base_pages: 0,
        };
        check_key::<K>(query)?;
        let Some(root) = self.root else {
            return Ok(NativeBorrowedKeyPath { actual: self, cost });
        };
        let mut next = root.root;
        let mut checksum = root.checksum;
        if checksum == super::DEFERRED {
            return Err(NativeKeyPathError::UnsupportedState);
        }
        let mut seen = [None::<PageNumber>; SUPPORTED_PATH_DEPTH];
        for depth in 0..SUPPORTED_PATH_DEPTH {
            if seen[..depth].contains(&Some(next)) {
                return Err(NativeKeyPathError::Malformed);
            }
            visits.visit()?;
            seen[depth] = Some(next);
            let page = self.mem.get_page(next, self.hint)?;
            check_layout::<K, V>(&page)?;
            cost.depth = depth + 1;
            cost.observed_page_bytes = cost
                .observed_page_bytes
                .checked_add(
                    u64::try_from(page.memory().len()).map_err(|_| NativeKeyPathError::Overflow)?,
                )
                .ok_or(NativeKeyPathError::Overflow)?;
            cost.observed_base_pages = cost
                .observed_base_pages
                .checked_add(next.page_size_bytes(1))
                .ok_or(NativeKeyPathError::Overflow)?;
            match page.memory()[0] {
                LEAF => {
                    if leaf_checksum(&page, K::fixed_width(), V::fixed_width())? != checksum {
                        return Err(NativeKeyPathError::Malformed);
                    }
                    return Ok(NativeBorrowedKeyPath { actual: self, cost });
                }
                BRANCH => {
                    if branch_checksum(&page, K::fixed_width())? != checksum {
                        return Err(NativeKeyPathError::Malformed);
                    }
                    let branch = BranchAccessor::new(&page, K::fixed_width());
                    let (index, child) = branch.child_for_key::<K>(query);
                    next = child;
                    checksum = branch
                        .child_checksum(index)
                        .ok_or(NativeKeyPathError::Malformed)?;
                    if checksum == super::DEFERRED {
                        return Err(NativeKeyPathError::UnsupportedState);
                    }
                }
                _ => return Err(NativeKeyPathError::Malformed),
            }
        }
        Err(NativeKeyPathError::UnsupportedDepth)
    }
}

// This owner retains both actual read/table custody and the matching epoch.
// Its destructor preserves those borrows even when no later method is called.
pub(crate) struct NativeEpochKeyPath<'tree, 'epoch, 'tracker, K: Key + 'static, V: Value + 'static>
{
    path: NativeBorrowedKeyPath<'tree, K, V>,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
}
impl<K: Key, V: Value> NativeEpochKeyPath<'_, '_, '_, K, V> {
    pub(crate) fn cost(&self) -> NativeKeyPathCost {
        self.path.cost()
    }
    pub(crate) fn require_current(&self) -> PathResult<()> {
        self.epoch
            .require_current()
            .map_err(NativeKeyPathError::Epoch)?;
        self.path.actual.require_current_native_resource_read()?;
        self.epoch
            .require_current()
            .map_err(NativeKeyPathError::Epoch)
    }
}
impl<K: Key, V: Value> Drop for NativeEpochKeyPath<'_, '_, '_, K, V> {
    fn drop(&mut self) {}
}
impl<K: Key, V: Value> Btree<K, V> {
    pub(crate) fn inspect_native_resource_key_at_epoch<'tree, 'epoch, 'tracker>(
        &'tree self,
        key: &K::SelfType<'_>,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<NativeEpochKeyPath<'tree, 'epoch, 'tracker, K, V>> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let tracker = parts.actual_tracker();
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        let path = self.inspect_native_resource_key(key, visits)?;
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        Ok(NativeEpochKeyPath { path, epoch })
    }
}

// Borrowed physical keys from an already compiled closed mutation recipe.
// This selector is numerical-only and grants no read/mutation/source authority.
#[derive(Clone, Copy)]
pub(crate) enum NativePreparedNormalKey<'key> {
    Bytes(&'key [u8]),
    Str(&'key str),
    U64(u64),
}
impl<K: Key, V: Value> Btree<K, V> {
    pub(crate) fn inspect_prepared_resource_key_at_epoch<'tree, 'epoch, 'tracker>(
        &'tree self,
        key: &NativePreparedNormalKey<'_>,
        epoch: &'epoch NativeResourceEpoch<'tracker>,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<NativeEpochKeyPath<'tree, 'epoch, 'tracker, K, V>> {
        // The generic core uses the original native Key implementation. This
        // dispatch never casts/transmutes a foreign associated SelfType.
        match key {
            NativePreparedNormalKey::Bytes(bytes)
                if TypeId::of::<K>() == TypeId::of::<&'static [u8]>() =>
            {
                self.inspect_encoded_resource_key_at_epoch(bytes, epoch, visits)
            }
            NativePreparedNormalKey::Str(text)
                if TypeId::of::<K>() == TypeId::of::<&'static str>() =>
            {
                self.inspect_encoded_resource_key_at_epoch(text.as_bytes(), epoch, visits)
            }
            NativePreparedNormalKey::U64(value) if TypeId::of::<K>() == TypeId::of::<u64>() => {
                self.inspect_encoded_resource_key_at_epoch(&value.to_le_bytes(), epoch, visits)
            }
            _ => Err(NativeKeyPathError::UnsupportedKey),
        }
    }
    fn inspect_encoded_resource_key_at_epoch<'tree, 'epoch, 'tracker>(
        &'tree self,
        query: &[u8],
        epoch: &'epoch NativeResourceEpoch<'tracker>,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<NativeEpochKeyPath<'tree, 'epoch, 'tracker, K, V>> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let tracker = parts.actual_tracker();
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        let path = self.inspect_encoded_native_resource_key(query, visits)?;
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        Ok(NativeEpochKeyPath { path, epoch })
    }
}

// This is an allocation-free component, not a complete Normal mutation quote.
// The production recipe owner must already have checked its actual compiled
// mutation/encoded-byte envelope before calling it. It borrows that exact key
// slice; neither a caller-supplied path count nor a detached scalar is accepted.
// Delete sibling windows and automatic native records must be inspected by the
// recipe owner separately, or the complete write plan must refuse support.
pub(crate) struct NativeEpochKeyBatch<
    'tree,
    'keys,
    'epoch,
    'tracker,
    K: Key + 'static,
    V: Value + 'static,
> {
    actual: &'tree Btree<K, V>,
    keys: &'keys [NativePreparedNormalKey<'keys>],
    epoch: &'epoch NativeResourceEpoch<'tracker>,
    maximum_depth: usize,
    observed_page_bytes: u64,
    observed_base_pages: u64,
    visited: usize,
}
impl<K: Key, V: Value> NativeEpochKeyBatch<'_, '_, '_, '_, K, V> {
    pub(crate) fn require_current(&self) -> PathResult<()> {
        self.epoch
            .require_current()
            .map_err(NativeKeyPathError::Epoch)?;
        self.actual.require_current_native_resource_read()?;
        self.epoch
            .require_current()
            .map_err(NativeKeyPathError::Epoch)
    }
    pub(crate) fn maximum_depth(&self) -> usize {
        self.maximum_depth
    }
    pub(crate) fn observed_page_bytes(&self) -> u64 {
        self.observed_page_bytes
    }
    pub(crate) fn observed_base_pages(&self) -> u64 {
        self.observed_base_pages
    }
    pub(crate) fn visited(&self) -> usize {
        self.visited
    }
    pub(crate) fn prepared_keys(&self) -> &[NativePreparedNormalKey<'_>] {
        self.keys
    }
    pub(crate) fn matches_tree_and_keys(
        &self,
        actual: &Btree<K, V>,
        keys: &[NativePreparedNormalKey<'_>],
    ) -> bool {
        core::ptr::eq(self.actual, actual) && core::ptr::eq(self.keys, keys)
    }
}
impl<K: Key, V: Value> Drop for NativeEpochKeyBatch<'_, '_, '_, '_, K, V> {
    fn drop(&mut self) {}
}
impl<K: Key, V: Value> Btree<K, V> {
    pub(crate) fn inspect_prepared_resource_keys_at_epoch<'tree, 'keys, 'epoch, 'tracker>(
        &'tree self,
        keys: &'keys [NativePreparedNormalKey<'keys>],
        epoch: &'epoch NativeResourceEpoch<'tracker>,
    ) -> PathResult<NativeEpochKeyBatch<'tree, 'keys, 'epoch, 'tracker, K, V>> {
        // Even an empty batch validates its actual owner and epoch.
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        let tracker = parts.actual_tracker();
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        let mut visits = CompiledPathVisits::for_prepared_keys(keys)?;
        let mut result = NativeEpochKeyBatch {
            actual: self,
            keys,
            epoch,
            maximum_depth: 0,
            observed_page_bytes: 0,
            observed_base_pages: 0,
            visited: 0,
        };
        for key in keys {
            let path = self.inspect_prepared_resource_key_at_epoch(key, epoch, &mut visits)?;
            let cost = path.cost();
            result.maximum_depth = result.maximum_depth.max(cost.depth());
            result.observed_page_bytes = result
                .observed_page_bytes
                .checked_add(cost.observed_page_bytes())
                .ok_or(NativeKeyPathError::Overflow)?;
            result.observed_base_pages = result
                .observed_base_pages
                .checked_add(cost.observed_base_pages())
                .ok_or(NativeKeyPathError::Overflow)?;
        }
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        result.visited = visits.visited();
        Ok(result)
    }
}

#[cfg(all(test, not(redb_no_std)))]
mod tests {
    use super::*;
    use crate::db::TransactionGuard;
    use crate::tree_store::btree_base::{DEFERRED, LeafBuilder};
    use crate::tree_store::{
        AllocationPolicy, BtreeHeader, BtreeMut, InMemoryBackend, InternalTableDefinition,
        NativeOwnedPageVec, NativeOwnedShared, PAGE_SIZE, PageAllocator, PageHint, PageTracker,
        TableType, TransactionalMemory,
    };
    use alloc::boxed::Box;
    use alloc::sync::Arc;
    use alloc::vec;

    fn test_allocator() -> PageAllocator {
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();
        PageAllocator::new(Arc::new(mem), AllocationPolicy::Default)
    }

    #[test]
    // req: STO-020, REC-001, REC-003, REC-005, PRJ-004
    fn btree_rejects_malformed_node_offsets_and_allocator_keys() {
        let allocator = test_allocator();
        let allocated = NativeOwnedShared::ordinary(PageTracker::new_tracking());

        let key_bytes = AllocatorStateKey::as_bytes(&AllocatorStateKey::TransactionId);
        let val_bytes = 0u64.to_le_bytes();
        let mut builder = LeafBuilder::new(
            &allocator,
            &allocated,
            1,
            AllocatorStateKey::fixed_width(),
            None,
        );
        builder.push(&key_bytes, &val_bytes);
        let mut page = builder.build().unwrap();
        let root_num = page.get_page_number();
        let accessor = LeafAccessor::new(page.memory(), AllocatorStateKey::fixed_width(), None);
        let (key_range, val_range) = accessor.entry_ranges(0).unwrap();
        page.memory_mut()[key_range.start] = 0xff;
        drop(page);

        let root = Some(BtreeHeader::new(root_num, DEFERRED, 1));
        let tree = Btree::<AllocatorStateKey, &'static [u8]>::new(
            root,
            PageHint::None,
            NativeOwnedShared::ordinary(TransactionGuard::untracked()),
            allocator.resolver(),
        )
        .unwrap();
        assert!(matches!(
            tree.get(&AllocatorStateKey::TransactionId),
            Err(StorageError::Corrupted(_))
        ));
        assert!(matches!(tree.first(), Err(StorageError::Corrupted(_))));
        assert!(matches!(tree.last(), Err(StorageError::Corrupted(_))));
        assert!(matches!(
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))
                .and_then(|mut r| r.next().transpose()),
            Err(StorageError::Corrupted(_))
        ));
        assert!(matches!(
            tree.range_bounds(
                core::ops::Bound::Included(vec![0xff; 5]),
                core::ops::Bound::Unbounded
            ),
            Err(StorageError::Corrupted(_))
        ));
        assert!(matches!(
            tree.range_bounds(
                core::ops::Bound::Included(vec![3; 4]),
                core::ops::Bound::Unbounded
            ),
            Err(StorageError::Corrupted(_))
        ));
        drop(tree);

        let mut page = allocator.get_page_mut(root_num).unwrap();
        page.memory_mut()[key_range.start] = 5;
        let bad_val_end = u32::try_from(val_range.start + 3).unwrap();
        page.memory_mut()[4..8].copy_from_slice(&bad_val_end.to_le_bytes());
        drop(page);
        let tree = Btree::<AllocatorStateKey, &'static [u8]>::new(
            root,
            PageHint::None,
            NativeOwnedShared::ordinary(TransactionGuard::untracked()),
            allocator.resolver(),
        )
        .unwrap();
        assert!(matches!(
            tree.get(&AllocatorStateKey::TransactionId),
            Err(StorageError::Corrupted(_))
        ));
        drop(tree);

        let mut page = allocator.get_page_mut(root_num).unwrap();
        page.memory_mut()[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        drop(page);
        let tree = Btree::<AllocatorStateKey, &'static [u8]>::new(
            root,
            PageHint::None,
            NativeOwnedShared::ordinary(TransactionGuard::untracked()),
            allocator.resolver(),
        )
        .unwrap();
        assert!(matches!(
            tree.get(&AllocatorStateKey::TransactionId),
            Err(StorageError::Corrupted(_))
        ));
        drop(tree);

        let def = InternalTableDefinition::new::<AllocatorStateKey, &'static [u8]>(
            TableType::Normal,
            None,
            0,
        );
        let mut def_bytes = InternalTableDefinition::as_bytes(&def);
        def_bytes[0] = 0xff;
        let mut sys_builder = LeafBuilder::new(&allocator, &allocated, 1, None, None);
        sys_builder.push(b"allocator_state", &def_bytes);
        let sys_page = sys_builder.build().unwrap();
        let sys_root = Some(BtreeHeader::new(sys_page.get_page_number(), DEFERRED, 1));
        drop(sys_page);
        let sys_tree = Btree::<&'static str, InternalTableDefinition>::new(
            sys_root,
            PageHint::None,
            NativeOwnedShared::ordinary(TransactionGuard::untracked()),
            allocator.resolver(),
        )
        .unwrap();
        assert!(matches!(
            sys_tree.get(&"allocator_state"),
            Err(StorageError::Corrupted(_))
        ));
        drop(sys_tree);
        let mut sys_mut = BtreeMut::<&'static str, InternalTableDefinition>::new(
            sys_root,
            NativeOwnedShared::ordinary(TransactionGuard::untracked()),
            allocator,
            NativeOwnedShared::ordinary(crate::sync::Mutex::new(NativeOwnedPageVec::default())),
            allocated,
        );
        assert!(matches!(
            sys_mut.get_mut(&"allocator_state"),
            Err(StorageError::Corrupted(_))
        ));
    }
}
