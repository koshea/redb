use super::allocator_reopen_facts::{
    AllocatorCapacityFacts, AllocatorParseRecorder, AllocatorResourceError,
};
use super::owned_allocator_vec::NativeOwnedAllocatorVec;
use crate::tree_store::page_store::base::MAX_REGIONS;
use crate::tree_store::page_store::bitmap::{
    BtreeBitmap, BtreeBitmapEncodedLayout, read_encoded_u32,
};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::page_manager::{INITIAL_REGIONS, MAX_MAX_PAGE_ORDER};
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::{self, max};
use core::mem::size_of;

// Borrowed length-table framing only. This is not an allocator/read/Source
// authority. Ordinary and guarded materializers share the loop below.
#[derive(Clone, Copy)]
pub(super) struct RegionTrackerEncodedLayout<'a> {
    encoded: &'a [u8],
    orders: usize,
    checked: bool,
    consumed_end: Option<usize>,
}
impl<'a> RegionTrackerEncodedLayout<'a> {
    pub(super) fn inspect(encoded: &'a [u8]) -> Result<Self, AllocatorResourceError> {
        let orders = usize::try_from(read_encoded_u32(encoded, 0, true)?)
            .map_err(|_| AllocatorResourceError::Overflow)?;
        let mut start = orders
            .checked_mul(size_of::<u32>())
            .and_then(|n| size_of::<u32>().checked_add(n))
            .ok_or(AllocatorResourceError::Overflow)?;
        if start > encoded.len() {
            return Err(AllocatorResourceError::Incomplete);
        }
        for index in 0..orders {
            let offset = index
                .checked_mul(size_of::<u32>())
                .and_then(|n| size_of::<u32>().checked_add(n))
                .ok_or(AllocatorResourceError::Overflow)?;
            let length = usize::try_from(read_encoded_u32(encoded, offset, true)?)
                .map_err(|_| AllocatorResourceError::Overflow)?;
            let end = start
                .checked_add(length)
                .ok_or(AllocatorResourceError::Overflow)?;
            BtreeBitmapEncodedLayout::inspect(
                encoded
                    .get(start..end)
                    .ok_or(AllocatorResourceError::Incomplete)?,
            )?;
            start = end;
        }
        Ok(Self {
            encoded,
            orders,
            checked: true,
            consumed_end: Some(start),
        })
    }
    fn ordinary(encoded: &'a [u8]) -> Self {
        // Exactly the original first header expression; subsequent length reads
        // remain interleaved with temporary Vec pushes in the sole materializer.
        let orders = u32::from_le_bytes(encoded[..size_of::<u32>()].try_into().unwrap()) as usize;
        Self {
            encoded,
            orders,
            checked: false,
            consumed_end: None,
        }
    }
    pub(super) fn consumed_end(self) -> Option<usize> {
        self.consumed_end
    }
    pub(super) fn source_resize_capacity_bound(
        self,
        target_regions: u32,
    ) -> super::allocator_growth::GrowthResult {
        use super::{allocator_growth as g, container_bounds as b};
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        if checked.orders != usize::from(MAX_MAX_PAGE_ORDER) + 1 {
            return Err(AllocatorResourceError::Incomplete);
        }
        let mut result = g::unchanged(
            b::vector_storage::<BtreeBitmap>(checked.orders)
                .map_err(g::map)?
                .allocator_bytes,
        );
        let mut start = size_of::<u32>() + checked.orders * size_of::<u32>();
        for index in 0..checked.orders {
            let length = read_encoded_u32(
                checked.encoded,
                size_of::<u32>() + index * size_of::<u32>(),
                true,
            )? as usize;
            let end = start
                .checked_add(length)
                .ok_or(AllocatorResourceError::Overflow)?;
            let bitmap = BtreeBitmapEncodedLayout::inspect(
                checked
                    .encoded
                    .get(start..end)
                    .ok_or(AllocatorResourceError::Incomplete)?,
            )?;
            let target = bitmap.source_logical_len()?.max(target_regions);
            result = g::monotone_preparation(result, bitmap.source_resize_capacity_bound(target)?)?;
            start = end;
        }
        Ok(result)
    }

    pub(super) fn source_capacity_bound(self) -> super::allocator_growth::GrowthResult {
        use super::{allocator_growth as g, container_bounds as b};
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        let outer = b::vector_storage::<BtreeBitmap>(checked.orders)
            .map_err(g::map)?
            .allocator_bytes;
        let workspace = b::vector_storage::<usize>(checked.orders)
            .map_err(g::map)?
            .allocator_bytes;
        let mut retained = outer;
        let mut start = size_of::<u32>() + checked.orders * size_of::<u32>();
        for index in 0..checked.orders {
            let length = read_encoded_u32(
                checked.encoded,
                size_of::<u32>() + index * size_of::<u32>(),
                true,
            )? as usize;
            let end = start
                .checked_add(length)
                .ok_or(AllocatorResourceError::Overflow)?;
            let bitmap = BtreeBitmapEncodedLayout::inspect(
                checked
                    .encoded
                    .get(start..end)
                    .ok_or(AllocatorResourceError::Incomplete)?,
            )?;
            retained = retained
                .checked_add(bitmap.source_capacity_bound()?)
                .ok_or(AllocatorResourceError::Overflow)?;
            start = end;
        }
        Ok(g::Growth {
            retained_after_bytes: retained,
            simultaneous_peak_bytes: retained
                .checked_add(workspace)
                .ok_or(AllocatorResourceError::Overflow)?,
        })
    }
}

// Tracks the page orders that MAY BE free in each region. This data structure is optimistic, so
// a region may not actually have a page free for a given order
pub(crate) struct RegionTracker {
    order_trackers: NativeOwnedAllocatorVec<BtreeBitmap>,
}

impl RegionTracker {
    pub(crate) fn new(regions: u32, orders: u8) -> Self {
        let mut data = vec![];
        for _ in 0..orders {
            data.push(BtreeBitmap::new_padded(regions, regions, MAX_REGIONS));
        }
        Self {
            order_trackers: data.into(),
        }
    }

    // Format:
    // num_orders: u32 number of order allocators
    // allocator_lens: u32 length of each allocator
    // data: BtreeBitmap data for each order
    pub(super) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![];
        let orders: u32 = self.order_trackers.len().try_into().unwrap();
        let allocator_lens: Vec<u32> = self
            .order_trackers
            .iter()
            .map(|x| x.to_vec().len().try_into().unwrap())
            .collect();
        result.extend(orders.to_le_bytes());
        for allocator_len in allocator_lens {
            result.extend(allocator_len.to_le_bytes());
        }
        for order in &self.order_trackers {
            result.extend(&order.to_vec());
        }
        result
    }

    pub(super) fn from_bytes(page: &[u8]) -> Self {
        Self::from_bytes_recording(page, None)
    }

    pub(super) fn from_bytes_recording(
        page: &[u8],
        facts: Option<&mut AllocatorParseRecorder>,
    ) -> Self {
        let layout = RegionTrackerEncodedLayout::ordinary(page);
        #[cfg(not(redb_no_std))]
        let value = Self::from_layout(layout, facts, None);
        #[cfg(redb_no_std)]
        let value = Self::from_layout(layout, facts);
        value.expect("ordinary RegionTracker materialization has no fallible Source admission")
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn from_bytes_source_recording(
        page: &[u8],
        resources: &super::page_resources::NativePageResources,
        facts: Option<&mut AllocatorParseRecorder>,
    ) -> crate::Result<Self> {
        let layout = RegionTrackerEncodedLayout::inspect(page)
            .map_err(|_| super::page_resources::refused())?;
        let _ = layout
            .source_capacity_bound()
            .map_err(|_| super::page_resources::refused())?;
        Self::from_layout(layout, facts, Some(resources))
    }
    fn from_layout(
        layout: RegionTrackerEncodedLayout<'_>,
        mut facts: Option<&mut AllocatorParseRecorder>,
        #[cfg(not(redb_no_std))] resources: Option<&super::page_resources::NativePageResources>,
    ) -> crate::Result<Self> {
        let page = layout.encoded;
        #[cfg(not(redb_no_std))]
        if resources.is_some() && !layout.checked {
            return Err(super::page_resources::refused());
        }
        let mut start = size_of::<u32>();
        #[cfg(not(redb_no_std))]
        let mut allocator_lens = if let Some(resources) = resources {
            NativeOwnedAllocatorVec::source(layout.orders, resources)?
        } else {
            NativeOwnedAllocatorVec::default()
        };
        #[cfg(redb_no_std)]
        let mut allocator_lens = NativeOwnedAllocatorVec::default();
        #[cfg(not(redb_no_std))]
        if resources.is_some() {
            if let Some(facts) = facts.as_deref_mut() {
                facts.replace_vec::<usize>(0, allocator_lens.capacity());
            }
        }
        for _ in 0..layout.orders {
            let allocator_len = read_encoded_u32(page, start, layout.checked)
                .map_err(|_| super::page_resources::refused())?
                as usize;
            let old_capacity = allocator_lens.capacity();
            allocator_lens.push(allocator_len);
            if let Some(facts) = facts.as_deref_mut() {
                facts.replace_vec::<usize>(old_capacity, allocator_lens.capacity());
            }
            start += size_of::<u32>();
        }
        #[cfg(not(redb_no_std))]
        let mut data = if let Some(resources) = resources {
            NativeOwnedAllocatorVec::source(layout.orders, resources)?
        } else {
            NativeOwnedAllocatorVec::default()
        };
        #[cfg(redb_no_std)]
        let mut data = NativeOwnedAllocatorVec::default();
        #[cfg(not(redb_no_std))]
        if resources.is_some() {
            if let Some(facts) = facts.as_deref_mut() {
                facts.replace_vec::<BtreeBitmap>(0, data.capacity());
            }
        }
        let lengths_capacity = allocator_lens.capacity();
        for allocator_len in allocator_lens.iter().copied() {
            let end = if layout.checked {
                start
                    .checked_add(allocator_len)
                    .ok_or_else(super::page_resources::refused)?
            } else {
                start + allocator_len
            };
            let bytes = if layout.checked {
                page.get(start..end)
                    .ok_or_else(super::page_resources::refused)?
            } else {
                &page[start..end]
            };
            let bitmap_layout = if layout.checked {
                BtreeBitmapEncodedLayout::inspect(bytes)
                    .map_err(|_| super::page_resources::refused())?
            } else {
                BtreeBitmapEncodedLayout::ordinary(bytes)
            };
            #[cfg(not(redb_no_std))]
            let bitmap = BtreeBitmap::from_layout(bitmap_layout, facts.as_deref_mut(), resources)?;
            #[cfg(redb_no_std)]
            let bitmap = BtreeBitmap::from_layout(bitmap_layout, facts.as_deref_mut())?;
            let old_capacity = data.capacity();
            data.push(bitmap);
            if let Some(facts) = facts.as_deref_mut() {
                facts.replace_vec::<BtreeBitmap>(old_capacity, data.capacity());
            }
            start = end;
        }
        // Original consuming iteration frees this temporary after its loop.
        // Keep the actual length buffer before its numerical guard on errors.
        drop(allocator_lens);
        if let Some(facts) = facts {
            facts.replace_vec::<usize>(lengths_capacity, 0);
        }
        Ok(Self {
            order_trackers: data,
        })
    }

    pub(super) fn record_capacity(&self, facts: &mut AllocatorParseRecorder) {
        facts.replace_vec::<BtreeBitmap>(0, self.order_trackers.capacity());
        for bitmap in &self.order_trackers {
            bitmap.record_capacity(facts);
        }
    }

    pub(crate) fn find_free(&self, order: u8) -> Option<u32> {
        self.order_trackers[order as usize].find_first_unset()
    }

    pub(crate) fn mark_free(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        for i in 0..=order {
            self.order_trackers[i].clear(region);
        }
    }

    pub(crate) fn mark_full(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        assert!(order < self.order_trackers.len());
        for i in order..self.order_trackers.len() {
            self.order_trackers[i].set(region);
        }
    }

    #[cfg(not(redb_no_std))]
    fn prepare_source_resize(
        &mut self,
        new_capacity: u32,
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<()> {
        for bitmap in &mut self.order_trackers {
            bitmap.prepare_source_resize(bitmap.len().max(new_capacity), resources)?;
        }
        Ok(())
    }

    fn resize(&mut self, new_capacity: u32) {
        for order in &mut self.order_trackers {
            order.resize(new_capacity, true);
        }
    }

    fn len(&self) -> u32 {
        self.order_trackers[0].len()
    }
}

pub(super) struct Allocators {
    pub(super) region_tracker: RegionTracker,
    pub(super) region_allocators: NativeOwnedAllocatorVec<BuddyAllocator>,
    #[cfg(not(redb_no_std))]
    source_storage_present: bool,
    #[cfg(all(test, not(redb_no_std)))]
    handoff_failure_after: Option<usize>,
}

// This owner is private numerical preparation, never write authority. The
// future complete claim must additionally bind/recheck actual mem/epoch/root.
#[cfg(not(redb_no_std))]
pub(super) struct PreparedAllocatorResize {
    origin: *const Allocators,
    original_regions: usize,
    original_last_pages: u32,
    target: DatabaseLayout,
    new_regions: NativeOwnedAllocatorVec<BuddyAllocator>,
}

#[cfg(not(redb_no_std))]
pub(super) struct ClaimedAllocatorResize<'a> {
    // Exclusive actual native allocator borrow survives file resize/sync. This
    // is not a caller-supplied root or an authority to publish a transaction.
    owner: &'a mut Allocators,
    prepared: PreparedAllocatorResize,
}
#[cfg(not(redb_no_std))]
impl PreparedAllocatorResize {
    pub(super) fn claim(
        self,
        owner: &mut Allocators,
        new_layout: DatabaseLayout,
    ) -> crate::Result<ClaimedAllocatorResize<'_>> {
        if !core::ptr::eq(self.origin, owner as *const Allocators)
            || self.target != new_layout
            || self.original_regions != owner.region_allocators.len()
            || self.original_last_pages != owner.region_allocators.last().unwrap().len()
            || !owner.source_storage_present
        {
            return Err(super::page_resources::refused());
        }
        Ok(ClaimedAllocatorResize {
            owner,
            prepared: self,
        })
    }
}
#[cfg(not(redb_no_std))]
impl ClaimedAllocatorResize<'_> {
    // Called ONLY after a successful physical resize/sync while this genuine
    // exclusive owner borrow remains held. No fresh quota/Vec allocation or
    // fallible stale check may first occur after that physical mutation.
    pub(super) fn apply(self) {
        let layout = self.prepared.target;
        self.owner.resize_to_inner(layout, Some(self.prepared));
    }
}

impl Allocators {
    pub(super) fn from_parts(
        region_tracker: RegionTracker,
        region_allocators: Vec<BuddyAllocator>,
    ) -> Self {
        #[cfg(not(redb_no_std))]
        let value = Self::from_parsed_parts(region_tracker, region_allocators.into(), false);
        #[cfg(redb_no_std)]
        let value = Self::from_parsed_parts(region_tracker, region_allocators.into());
        value
    }
    pub(super) fn from_parsed_parts(
        region_tracker: RegionTracker,
        region_allocators: NativeOwnedAllocatorVec<BuddyAllocator>,
        #[cfg(not(redb_no_std))] source_storage_present: bool,
    ) -> Self {
        Self {
            region_tracker,
            region_allocators,
            #[cfg(not(redb_no_std))]
            source_storage_present,
            #[cfg(all(test, not(redb_no_std)))]
            handoff_failure_after: None,
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn has_source_storage(&self) -> bool {
        self.source_storage_present
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn fail_ordinary_handoff_after_for_test(&mut self, count: usize) {
        self.handoff_failure_after = Some(count);
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn handoff_ordinary(&mut self) -> crate::Result<()> {
        if !self.source_storage_present {
            return Ok(());
        }
        let mut handoff = super::owned_allocator_vec::NativeOrdinaryAllocatorHandoff::default();
        #[cfg(test)]
        {
            handoff.fail_after = self.handoff_failure_after.take();
        }
        self.region_allocators.handoff_ordinary(&mut handoff)?;
        for allocator in &mut self.region_allocators {
            allocator.handoff_ordinary(&mut handoff)?;
        }
        self.region_tracker
            .order_trackers
            .handoff_ordinary(&mut handoff)?;
        for bitmap in &mut self.region_tracker.order_trackers {
            bitmap.handoff_ordinary(&mut handoff)?;
        }
        self.source_storage_present = false;
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn source_resize_storage_quote(
        &self,
        current: DatabaseLayout,
        next: DatabaseLayout,
    ) -> super::allocator_growth::GrowthResult {
        use super::{allocator_growth as g, container_bounds};
        if next.num_regions() < current.num_regions() || next.len() < current.len() {
            return Err(super::allocator_reopen_facts::AllocatorResourceError::Unsupported);
        }
        let base = self.resize_quote(current, next)?;
        let added = (next.num_regions() as usize)
            .checked_sub(self.region_allocators.len())
            .ok_or(super::allocator_reopen_facts::AllocatorResourceError::Stale)?;
        let temporary = container_bounds::vector_storage::<BuddyAllocator>(added)
            .map_err(g::map)?
            .allocator_bytes;
        let frame = u64::try_from(
            core::mem::size_of::<PreparedAllocatorResize>()
                + core::mem::size_of::<ClaimedAllocatorResize<'_>>(),
        )
        .map_err(|_| super::allocator_reopen_facts::AllocatorResourceError::Overflow)?;
        g::sum(
            base,
            g::Growth {
                retained_after_bytes: 0,
                simultaneous_peak_bytes: temporary
                    .checked_add(frame)
                    .ok_or(super::allocator_reopen_facts::AllocatorResourceError::Overflow)?,
            },
        )
    }

    // Must run under the complete actual native writer/epoch preparation BEFORE
    // storage.resize/sync_file, page reservation or bitmap semantic mutation.
    // It changes only backing representation, never logical allocator bits.
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_source_resize(
        &mut self,
        current: DatabaseLayout,
        next: DatabaseLayout,
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<PreparedAllocatorResize> {
        self.source_resize_storage_quote(current, next)
            .map_err(|_| super::page_resources::refused())?;
        self.prepare_resize_backing(next, resources)
    }
    // Parsed maintenance allocators may legitimately be larger than the current
    // header layout. Preserve their original backing until actual resize/drop;
    // no invented old DatabaseLayout or post-hoc baseline tagging is used.
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_source_reopen_resize(
        &mut self,
        next: DatabaseLayout,
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<PreparedAllocatorResize> {
        if self.region_allocators.is_empty()
            || next.num_regions() == 0
            || self.region_tracker.order_trackers.len() != usize::from(MAX_MAX_PAGE_ORDER) + 1
            || self
                .region_allocators
                .iter()
                .any(|a| a.get_max_order() > MAX_MAX_PAGE_ORDER)
        {
            return Err(super::page_resources::refused());
        }
        self.prepare_resize_backing(next, resources)
    }
    #[cfg(not(redb_no_std))]
    fn prepare_resize_backing(
        &mut self,
        next: DatabaseLayout,
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<PreparedAllocatorResize> {
        self.source_storage_present = true; // includes physically installed partial preparation on Err
        self.region_allocators.prepare_capacity(
            (next.num_regions() as usize).max(self.region_allocators.len()),
            resources,
        )?;
        let old_regions = self.region_allocators.len();
        for i in 0..old_regions.min(next.num_regions() as usize) {
            let pages = next.region_layout(i as u32).num_pages();
            if pages > self.region_allocators[i].len() {
                self.region_allocators[i].prepare_source_resize(pages, resources)?;
            }
        }
        self.region_tracker
            .prepare_source_resize(next.num_regions(), resources)?;
        let added = (next.num_regions() as usize).saturating_sub(old_regions);
        let mut new_regions = NativeOwnedAllocatorVec::source(added, resources)?;
        for i in old_regions..next.num_regions() as usize {
            new_regions.push(BuddyAllocator::source_new(
                next.region_layout(i as u32).num_pages(),
                next.full_region_layout().num_pages(),
                resources,
            )?);
        }
        Ok(PreparedAllocatorResize {
            origin: self as *const Self,
            original_regions: old_regions,
            original_last_pages: self.region_allocators.last().unwrap().len(),
            target: next,
            new_regions,
        })
    }

    pub(super) fn resize_quote(
        &self,
        current: DatabaseLayout,
        next: DatabaseLayout,
    ) -> super::allocator_growth::GrowthResult {
        use super::allocator_reopen_facts::AllocatorResourceError as E;
        use super::{allocator_growth as g, container_bounds as b};
        if current.num_regions() as usize != self.region_allocators.len()
            || next.num_regions() == 0
            || next.num_regions() > MAX_REGIONS
            || current.full_region_layout() != next.full_region_layout()
            || self.region_tracker.order_trackers.len() != usize::from(MAX_MAX_PAGE_ORDER) + 1
        {
            return Err(E::Incomplete);
        }
        let count = next.num_regions() as usize;
        let mut result = b::vector_pushes::<BuddyAllocator>(
            self.region_allocators.capacity(),
            self.region_allocators.len(),
            count.saturating_sub(self.region_allocators.len()),
        )
        .map_err(g::map)?;
        for (index, allocator) in self.region_allocators.iter().enumerate() {
            if allocator.len() != current.region_layout(index as u32).num_pages() {
                return Err(E::Stale);
            }
            if index < count {
                result = g::sum(
                    result,
                    allocator.growth_quote(next.region_layout(index as u32).num_pages())?,
                )?;
            } else {
                // Removed child buffers stay physically live until native drain
                // destroys them. The outer Vec retains its original allocation.
                let old = allocator.growth_quote(allocator.len())?;
                result = g::sum(
                    result,
                    g::Growth {
                        retained_after_bytes: 0,
                        simultaneous_peak_bytes: old.simultaneous_peak_bytes,
                    },
                )?;
            }
        }
        for index in self.region_allocators.len()..count {
            result = g::sum(
                result,
                BuddyAllocator::new_quote(
                    next.region_layout(index as u32).num_pages(),
                    next.full_region_layout().num_pages(),
                )?,
            )?;
        }
        result = g::sum(
            result,
            g::unchanged(
                b::vector_storage::<BtreeBitmap>(self.region_tracker.order_trackers.capacity())
                    .map_err(g::map)?
                    .allocator_bytes,
            ),
        )?;
        for bitmap in &self.region_tracker.order_trackers {
            // Native resize grows optimistic trackers but never shrinks them.
            result = g::sum(
                result,
                bitmap.growth_quote(bitmap.len().max(next.num_regions()))?,
            )?;
        }
        Ok(result)
    }
    pub(super) fn new(layout: DatabaseLayout) -> Self {
        let mut region_allocators = vec![];
        let initial_regions = max(INITIAL_REGIONS, layout.num_regions());
        let mut region_tracker = RegionTracker::new(initial_regions, MAX_MAX_PAGE_ORDER + 1);
        for i in 0..layout.num_regions() {
            let region_layout = layout.region_layout(i);
            let allocator = BuddyAllocator::new(
                region_layout.num_pages(),
                layout.full_region_layout().num_pages(),
            );
            let max_order = allocator.get_max_order();
            region_tracker.mark_free(max_order, i);
            region_allocators.push(allocator);
        }

        Self::from_parts(region_tracker, region_allocators)
    }

    pub(crate) fn xxh3_hash(&self) -> u128 {
        // Ignore the region tracker because it is an optimistic cache, and so may not match
        // between repairs of the allocators
        let mut result = 0;
        for allocator in &self.region_allocators {
            result ^= allocator.xxh3_hash();
        }
        result
    }

    pub(super) fn reopen_capacity_facts(
        &self,
    ) -> Result<AllocatorCapacityFacts, AllocatorResourceError> {
        let mut facts = AllocatorParseRecorder::new();
        facts.replace_vec::<BuddyAllocator>(0, self.region_allocators.capacity());
        for region in &self.region_allocators {
            region.record_capacity(&mut facts);
        }
        self.region_tracker.record_capacity(&mut facts);
        Ok(facts.finish()?.retained())
    }

    pub(super) fn resize_to(&mut self, new_layout: DatabaseLayout) {
        #[cfg(not(redb_no_std))]
        self.resize_to_inner(new_layout, None);
        #[cfg(redb_no_std)]
        self.resize_to_inner(new_layout);
    }
    fn resize_to_inner(
        &mut self,
        new_layout: DatabaseLayout,
        #[cfg(not(redb_no_std))] mut prepared: Option<PreparedAllocatorResize>,
    ) {
        let shrink = match (new_layout.num_regions() as usize).cmp(&self.region_allocators.len()) {
            cmp::Ordering::Less => true,
            cmp::Ordering::Equal => {
                let allocator = self.region_allocators.last().unwrap();
                let last_region = new_layout
                    .trailing_region_layout()
                    .unwrap_or_else(|| new_layout.full_region_layout());
                match last_region.num_pages().cmp(&allocator.len()) {
                    cmp::Ordering::Less => true,
                    cmp::Ordering::Equal => {
                        // No-op
                        return;
                    }
                    cmp::Ordering::Greater => false,
                }
            }
            cmp::Ordering::Greater => false,
        };

        if shrink {
            // Drop all regions that were removed
            for i in new_layout.num_regions()..(self.region_allocators.len().try_into().unwrap()) {
                self.region_tracker.mark_full(0, i);
            }
            self.region_allocators
                .drain((new_layout.num_regions() as usize)..);

            // Resize the last region
            let last_region = new_layout
                .trailing_region_layout()
                .unwrap_or_else(|| new_layout.full_region_layout());
            let allocator = self.region_allocators.last_mut().unwrap();
            if allocator.len() > last_region.num_pages() {
                allocator.resize(last_region.num_pages());
            }
        } else {
            let old_num_regions = self.region_allocators.len();
            for i in 0..new_layout.num_regions() {
                let new_region = new_layout.region_layout(i);
                if (i as usize) < old_num_regions {
                    let allocator = &mut self.region_allocators[i as usize];
                    assert!(new_region.num_pages() >= allocator.len());
                    if new_region.num_pages() != allocator.len() {
                        allocator.resize(new_region.num_pages());
                        let highest_free = allocator.highest_free_order().unwrap();
                        self.region_tracker.mark_free(highest_free, i);
                    }
                } else {
                    // brand new region
                    #[cfg(not(redb_no_std))]
                    let allocator = if let Some(prepared) = prepared.as_mut() {
                        // Ordered native regions, already allocated before I/O.
                        prepared
                            .new_regions
                            .drain(..1)
                            .next()
                            .expect("complete native prepared region inventory")
                    } else {
                        BuddyAllocator::new(
                            new_region.num_pages(),
                            new_layout.full_region_layout().num_pages(),
                        )
                    };
                    #[cfg(redb_no_std)]
                    let allocator = BuddyAllocator::new(
                        new_region.num_pages(),
                        new_layout.full_region_layout().num_pages(),
                    );
                    let highest_free = allocator.highest_free_order().unwrap();
                    if i >= self.region_tracker.len() {
                        self.region_tracker.resize(i + 1);
                    }
                    self.region_tracker.mark_free(highest_free, i);
                    self.region_allocators.push(allocator);
                }
            }
        }
    }
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "allocator_growth_tests.rs"]
mod growth_tests;

#[cfg(all(test, not(redb_no_std)))]
mod tracker_materialization_tests {
    use super::super::page_resources::NativePageResources;
    use super::*;
    use std::panic::catch_unwind;

    #[test]
    // req: STO-020, REC-001
    fn native_region_tracker_layout_prices_temporary_and_retained_native_buffers() {
        let mut tracker = RegionTracker::new(4097, MAX_MAX_PAGE_ORDER + 1);
        tracker.mark_free(2, 0);
        tracker.mark_free(3, 4096);
        let encoded = tracker.to_vec();
        let layout = RegionTrackerEncodedLayout::inspect(&encoded).unwrap();
        assert_eq!(layout.consumed_end(), Some(encoded.len()));
        let bound = layout.source_capacity_bound().unwrap();
        assert!(bound.simultaneous_peak_bytes > bound.retained_after_bytes);
        let resources = NativePageResources::from_retained_pool_data(
            usize::try_from(bound.simultaneous_peak_bytes).unwrap(),
            (),
        )
        .unwrap();
        let mut facts = AllocatorParseRecorder::new();
        let source =
            RegionTracker::from_bytes_source_recording(&encoded, &resources, Some(&mut facts))
                .unwrap();
        assert_eq!(
            source.to_vec(),
            RegionTracker::from_bytes(&encoded).to_vec()
        );
        assert_eq!(source.find_free(2), Some(0));
        assert_eq!(source.find_free(3), Some(4096));
        assert_eq!(
            resources.current(),
            usize::try_from(bound.retained_after_bytes).unwrap()
        );
        assert_eq!(
            resources.admitted_high_water_for_test(),
            usize::try_from(bound.simultaneous_peak_bytes).unwrap()
        );
        let parsed = facts.finish().unwrap();
        let mut actual = AllocatorParseRecorder::new();
        source.record_capacity(&mut actual);
        assert_eq!(parsed.retained(), actual.finish().unwrap().retained());
        drop(source);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_region_tracker_malformed_refusal_trailing_compatibility_and_partial_refund() {
        let encoded = RegionTracker::new(4097, MAX_MAX_PAGE_ORDER + 1).to_vec();
        let bound = RegionTrackerEncodedLayout::inspect(&encoded)
            .unwrap()
            .source_capacity_bound()
            .unwrap();
        let mut malformed = Vec::new();
        for end in 0..size_of::<u32>() {
            malformed.push(encoded[..end].to_vec());
        }
        malformed.push(encoded[..encoded.len() - 1].to_vec());
        let mut impossible_length = encoded.clone();
        impossible_length[size_of::<u32>()..2 * size_of::<u32>()]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        malformed.push(impossible_length);
        for bytes in malformed {
            assert!(RegionTrackerEncodedLayout::inspect(&bytes).is_err());
            let resources = NativePageResources::from_retained_pool_data(
                usize::try_from(bound.simultaneous_peak_bytes).unwrap(),
                (),
            )
            .unwrap();
            let mut facts = AllocatorParseRecorder::new();
            assert!(
                RegionTracker::from_bytes_source_recording(&bytes, &resources, Some(&mut facts),)
                    .is_err()
            );
            assert_eq!(resources.current(), 0);
            assert_eq!(facts.finish().unwrap().retained().buffer_count(), 0);
            assert!(catch_unwind(|| RegionTracker::from_bytes(&bytes)).is_err());
        }
        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&[1, 4, 9]);
        let layout = RegionTrackerEncodedLayout::inspect(&trailing).unwrap();
        assert_eq!(layout.consumed_end(), Some(encoded.len()));
        assert_eq!(layout.source_capacity_bound().unwrap(), bound);
        assert_eq!(RegionTracker::from_bytes(&trailing).to_vec(), encoded);
        let resources = NativePageResources::from_retained_pool_data(
            usize::try_from(bound.simultaneous_peak_bytes).unwrap() - 1,
            (),
        )
        .unwrap();
        let mut facts = AllocatorParseRecorder::new();
        assert!(
            RegionTracker::from_bytes_source_recording(&trailing, &resources, Some(&mut facts),)
                .is_err()
        );
        assert!(facts.finish().unwrap().retained().buffer_count() > 0);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_guarded_parsed_allocator_resize_preserves_old_backing_until_replacement() {
        let layout = |pages: u64| DatabaseLayout::calculate(pages * 4096, 8192, 1, 4096);
        let initial = layout(4097);
        let small = layout(65);
        let grown = layout(8193);
        let original = Allocators::new(initial);
        let tracker_bytes = original.region_tracker.to_vec();
        let regions: Vec<_> = original
            .region_allocators
            .iter()
            .map(BuddyAllocator::to_vec)
            .collect();
        let tracker_bound = RegionTrackerEncodedLayout::inspect(&tracker_bytes)
            .unwrap()
            .source_capacity_bound()
            .unwrap();
        let mut parsed_peak =
            super::super::container_bounds::vector_storage::<BuddyAllocator>(regions.len())
                .unwrap()
                .allocator_bytes
                + tracker_bound.simultaneous_peak_bytes;
        for bytes in &regions {
            parsed_peak +=
                super::super::buddy_allocator::BuddyAllocatorEncodedLayout::inspect(bytes)
                    .unwrap()
                    .source_capacity_bound()
                    .unwrap();
        }
        let materialize = |resources: &NativePageResources| {
            let mut native_regions =
                NativeOwnedAllocatorVec::source(regions.len(), resources).unwrap();
            for bytes in &regions {
                native_regions.push(BuddyAllocator::from_bytes_source(bytes, resources).unwrap());
            }
            let tracker =
                RegionTracker::from_bytes_source_recording(&tracker_bytes, resources, None)
                    .unwrap();
            Allocators::from_parsed_parts(tracker, native_regions, true)
        };
        // Numerical component headroom; this is not a Source64 factory budget.
        let resources = NativePageResources::from_retained_pool_data(8 * 1024 * 1024, ()).unwrap();
        let mut source = materialize(&resources);
        let mut ordinary = original;
        let before = resources.current();
        let prepared = source
            .prepare_source_reopen_resize(small, &resources)
            .unwrap();
        prepared.claim(&mut source, small).unwrap().apply();
        ordinary.resize_to(small);
        assert_eq!(source.xxh3_hash(), ordinary.xxh3_hash());
        assert_eq!(
            source.region_allocators[0].to_vec(),
            ordinary.region_allocators[0].to_vec()
        );
        assert_eq!(
            resources.current(),
            before,
            "shrinking logical lengths cannot refund retained words"
        );
        let prepared = source
            .prepare_source_reopen_resize(grown, &resources)
            .unwrap();
        prepared.claim(&mut source, grown).unwrap().apply();
        ordinary.resize_to(grown);
        assert_eq!(source.xxh3_hash(), ordinary.xxh3_hash());
        assert_eq!(
            source.region_allocators.len(),
            ordinary.region_allocators.len()
        );
        for (source, ordinary) in source
            .region_allocators
            .iter()
            .zip(ordinary.region_allocators.iter())
        {
            assert_eq!(source.to_vec(), ordinary.to_vec());
        }
        assert!(resources.admitted_high_water_for_test() > before);
        drop(source);
        assert_eq!(resources.current(), 0);

        // The same parser peak fits, but a later actual growth replacement does
        // not. Installed partial backing remains paid without changing bits.
        let resources =
            NativePageResources::from_retained_pool_data(usize::try_from(parsed_peak).unwrap(), ())
                .unwrap();
        let mut source = materialize(&resources);
        let before_hash = source.xxh3_hash();
        assert!(
            source
                .prepare_source_reopen_resize(grown, &resources)
                .is_err()
        );
        assert_eq!(source.xxh3_hash(), before_hash);
        assert!(source.has_source_storage());
        assert!(resources.current() > 0);
        drop(source);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_borrowed_tracker_resize_quote_separates_parse_workspace_from_growth() {
        let mut original = RegionTracker::new(4097, MAX_MAX_PAGE_ORDER + 1);
        original.mark_free(2, 0);
        let encoded = original.to_vec();
        let layout = RegionTrackerEncodedLayout::inspect(&encoded).unwrap();
        let parsed = layout.source_capacity_bound().unwrap();
        let resized = layout.source_resize_capacity_bound(200_001).unwrap();
        let peak = parsed
            .simultaneous_peak_bytes
            .max(resized.simultaneous_peak_bytes);
        assert!(resized.retained_after_bytes > parsed.retained_after_bytes);
        let resources =
            NativePageResources::from_retained_pool_data(usize::try_from(peak).unwrap(), ())
                .unwrap();
        let mut source =
            RegionTracker::from_bytes_source_recording(&encoded, &resources, None).unwrap();
        assert_eq!(
            resources.current(),
            usize::try_from(parsed.retained_after_bytes).unwrap()
        );
        source.prepare_source_resize(200_001, &resources).unwrap();
        source.resize(200_001);
        original.resize(200_001);
        assert_eq!(source.to_vec(), original.to_vec());
        assert_eq!(source.find_free(2), Some(0));
        assert!(resources.current() as u64 <= resized.retained_after_bytes);
        assert!(resources.admitted_high_water_for_test() as u64 <= peak);
        drop(source);
        assert_eq!(resources.current(), 0);
    }
}
