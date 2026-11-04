use super::allocator_reopen_facts::AllocatorParseRecorder;
use super::owned_allocator_vec::NativeOwnedAllocatorVec;
use crate::tree_store::PageNumber;
use crate::tree_store::page_store::bitmap::{
    BtreeBitmap, BtreeBitmapEncodedLayout, NativeEncodedChildren, read_encoded_u32,
};
#[cfg(test)]
use crate::tree_store::page_store::fast_hash::PageNumberHashSet;
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::min;
use core::mem::size_of;

const MAX_ORDER_OFFSET: usize = 0;
const PADDING: usize = 3;
const NUM_PAGES_OFFSET: usize = MAX_ORDER_OFFSET + size_of::<u8>() + PADDING;
const FREE_END_OFFSETS: usize = NUM_PAGES_OFFSET + size_of::<u32>();

// Borrowed native framing/capacity only, never an allocator or Source proof.
#[derive(Clone, Copy)]
pub(super) struct BuddyAllocatorEncodedLayout<'a> {
    encoded: &'a [u8],
    max_order: u8,
    num_pages: u32,
    checked: bool,
    consumed_end: Option<usize>,
}
impl<'a> BuddyAllocatorEncodedLayout<'a> {
    pub(super) fn inspect(
        encoded: &'a [u8],
    ) -> Result<Self, super::allocator_reopen_facts::AllocatorResourceError> {
        use super::allocator_reopen_facts::AllocatorResourceError as E;
        let max_order = *encoded.get(MAX_ORDER_OFFSET).ok_or(E::Incomplete)?;
        let num_pages = read_encoded_u32(encoded, NUM_PAGES_OFFSET, true)?;
        let count = usize::from(max_order) + 1;
        let mut orders = NativeEncodedChildren::checked(encoded, FREE_END_OFFSETS, count)?;
        for order in orders.by_ref() {
            BtreeBitmapEncodedLayout::inspect(order?)?;
        }
        Ok(Self {
            encoded,
            max_order,
            num_pages,
            checked: true,
            consumed_end: Some(orders.consumed_end()),
        })
    }
    fn ordinary(encoded: &'a [u8]) -> Self {
        let max_order = encoded[MAX_ORDER_OFFSET];
        let num_pages = read_encoded_u32(encoded, NUM_PAGES_OFFSET, false)
            .expect("ordinary fixed framing has no typed error");
        let _ =
            NativeEncodedChildren::ordinary(encoded, FREE_END_OFFSETS, usize::from(max_order) + 1);
        Self {
            encoded,
            max_order,
            num_pages,
            checked: false,
            consumed_end: None,
        }
    }
    pub(super) fn consumed_end(self) -> Option<usize> {
        self.consumed_end
    }
    fn count(self) -> usize {
        usize::from(self.max_order) + 1
    }
    fn orders(self) -> BuddyBitmapOrders<'a> {
        let children = if self.checked {
            NativeEncodedChildren::checked(self.encoded, FREE_END_OFFSETS, self.count())
                .expect("checked immutable Buddy layout")
        } else {
            NativeEncodedChildren::ordinary(self.encoded, FREE_END_OFFSETS, self.count())
        };
        BuddyBitmapOrders {
            children,
            checked: self.checked,
        }
    }
    pub(super) fn source_resize_capacity_bound(
        self,
        mut new_pages: u32,
    ) -> super::allocator_growth::GrowthResult {
        use super::{allocator_growth as g, container_bounds as b};
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        if new_pages == 0 || checked.max_order > MAX_MAX_PAGE_ORDER {
            return Err(super::allocator_reopen_facts::AllocatorResourceError::Incomplete);
        }
        if new_pages <= checked.num_pages {
            return Ok(g::unchanged(checked.source_capacity_bound()?));
        }
        let mut result = g::unchanged(
            b::vector_storage::<BtreeBitmap>(checked.count())
                .map_err(g::map)?
                .allocator_bytes,
        );
        for bitmap in checked.orders() {
            result =
                g::monotone_preparation(result, bitmap?.source_resize_capacity_bound(new_pages)?)?;
            new_pages = next_higher_order(new_pages);
        }
        Ok(result)
    }
    pub(super) fn source_capacity_bound(
        self,
    ) -> Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        use super::allocator_reopen_facts::AllocatorResourceError as E;
        use super::{
            container_bounds as b, read_only_resource::map_bound,
            resource_build_profile::NativeResourceBuildProfile,
        };
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        NativeResourceBuildProfile::require().map_err(map_bound)?;
        let mut total = b::vector_storage::<BtreeBitmap>(checked.count())
            .map_err(map_bound)?
            .allocator_bytes;
        for order in checked.orders() {
            total = total
                .checked_add(order?.source_capacity_bound()?)
                .ok_or(E::Overflow)?;
        }
        Ok(total)
    }
}
struct BuddyBitmapOrders<'a> {
    children: NativeEncodedChildren<'a>,
    checked: bool,
}
impl<'a> Iterator for BuddyBitmapOrders<'a> {
    type Item =
        Result<BtreeBitmapEncodedLayout<'a>, super::allocator_reopen_facts::AllocatorResourceError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.children.next().map(|child| {
            let child = child?;
            if self.checked {
                BtreeBitmapEncodedLayout::inspect(child)
            } else {
                Ok(BtreeBitmapEncodedLayout::ordinary(child))
            }
        })
    }
}

fn calculate_usable_order(pages: u32) -> u8 {
    let max_order = (32 - pages.leading_zeros() - 1).try_into().unwrap();
    min(MAX_MAX_PAGE_ORDER, max_order)
}

fn next_higher_order(page_number: u32) -> u32 {
    page_number / 2
}

fn buddy_page(page_number: u32) -> u32 {
    page_number ^ 1
}

// Handles allocation of dynamically sized pages, supports pages of up to page_size * 2^max_order bytes
//
// Pages are marked free at only a single order, and it must always be the largest order
pub(crate) struct BuddyAllocator {
    free: NativeOwnedAllocatorVec<BtreeBitmap>,
    len: u32,
    max_order: u8,
}

impl BuddyAllocator {
    pub(super) fn growth_quote(&self, mut pages: u32) -> super::allocator_growth::GrowthResult {
        use super::{allocator_growth as g, container_bounds as b};
        if pages == 0 || self.free.len() != usize::from(self.max_order) + 1 {
            return Err(super::allocator_reopen_facts::AllocatorResourceError::Incomplete);
        }
        let mut result = g::unchanged(
            b::vector_storage::<BtreeBitmap>(self.free.capacity())
                .map_err(g::map)?
                .allocator_bytes,
        );
        for bitmap in &self.free {
            result = g::sum(result, bitmap.growth_quote(pages)?)?;
            pages = next_higher_order(pages);
        }
        Ok(result)
    }

    pub(super) fn new_quote(
        mut pages: u32,
        mut capacity: u32,
    ) -> super::allocator_growth::GrowthResult {
        use super::{allocator_growth as g, container_bounds as b};
        if pages == 0 || capacity == 0 || pages > capacity {
            return Err(super::allocator_reopen_facts::AllocatorResourceError::Incomplete);
        }
        let orders = usize::from(calculate_usable_order(capacity)) + 1;
        let mut result = b::vector_pushes::<BtreeBitmap>(0, 0, orders).map_err(g::map)?;
        for _ in 0..orders {
            result = g::sum(result, BtreeBitmap::padded_new_quote(pages, capacity)?)?;
            pages = next_higher_order(pages);
            capacity = next_higher_order(capacity);
        }
        Ok(result)
    }
    fn initialize(
        num_pages: u32,
        max_page_capacity: u32,
        #[cfg(not(redb_no_std))] resources: Option<&super::page_resources::NativePageResources>,
    ) -> crate::Result<Self> {
        let max_order = calculate_usable_order(max_page_capacity);
        let mut capacity_for_order = max_page_capacity;
        let mut pages_for_order = num_pages;
        #[cfg(not(redb_no_std))]
        let mut free = if let Some(resources) = resources {
            NativeOwnedAllocatorVec::source(usize::from(max_order) + 1, resources)?
        } else {
            NativeOwnedAllocatorVec::default()
        };
        #[cfg(redb_no_std)]
        let mut free = NativeOwnedAllocatorVec::default();
        for _ in 0..=max_order {
            #[cfg(not(redb_no_std))]
            let bitmap = if let Some(resources) = resources {
                BtreeBitmap::source_new_padded(
                    pages_for_order,
                    pages_for_order,
                    capacity_for_order,
                    resources,
                )?
            } else {
                BtreeBitmap::new_padded(pages_for_order, pages_for_order, capacity_for_order)
            };
            #[cfg(redb_no_std)]
            let bitmap =
                BtreeBitmap::new_padded(pages_for_order, pages_for_order, capacity_for_order);
            free.push(bitmap);
            pages_for_order = next_higher_order(pages_for_order);
            capacity_for_order = next_higher_order(capacity_for_order);
        }
        Ok(Self::finish_new(free, num_pages, max_order))
    }
    pub(crate) fn new(num_pages: u32, max_page_capacity: u32) -> Self {
        #[cfg(not(redb_no_std))]
        let value = Self::initialize(num_pages, max_page_capacity, None);
        #[cfg(redb_no_std)]
        let value = Self::initialize(num_pages, max_page_capacity);
        value.expect("ordinary buddy initialization has no fallible Source admission")
    }

    fn finish_new(
        mut free: NativeOwnedAllocatorVec<BtreeBitmap>,
        num_pages: u32,
        max_order: u8,
    ) -> Self {
        // Mark the available pages, starting with the highest order
        let mut accounted_pages = 0;
        for order in (0..=max_order).rev() {
            let order_size = 2u32.pow(order.into());
            while accounted_pages + order_size <= num_pages {
                let page = accounted_pages / order_size;
                free[order as usize].clear(page);
                accounted_pages += order_size;
            }
        }
        assert_eq!(accounted_pages, num_pages);

        Self {
            free,
            len: num_pages,
            max_order,
        }
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn source_new(
        num_pages: u32,
        max_page_capacity: u32,
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<Self> {
        if num_pages == 0 || num_pages > max_page_capacity {
            return Err(super::page_resources::refused());
        }
        Self::initialize(num_pages, max_page_capacity, Some(resources))
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_source_resize(
        &mut self,
        mut pages: u32,
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<()> {
        for bitmap in &mut self.free {
            bitmap.prepare_source_resize(pages, resources)?;
            pages = next_higher_order(pages);
        }
        Ok(())
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn handoff_ordinary(
        &mut self,
        handoff: &mut super::owned_allocator_vec::NativeOrdinaryAllocatorHandoff,
    ) -> crate::Result<()> {
        self.free.handoff_ordinary(handoff)?;
        for bitmap in &mut self.free {
            bitmap.handoff_ordinary(handoff)?;
        }
        Ok(())
    }

    pub(crate) fn xxh3_hash(&self) -> u128 {
        let mut result = 0;
        for x in &self.free {
            result ^= x.xxh3_hash();
        }

        result
    }

    // Data structure format:
    // max_order: u8
    // padding: 3 bytes
    // num_pages: u32
    // free_ends: array of u32, with ending offset for BtreeBitmap structure for the given order
    // ... BtreeBitmap structures
    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let serialized: Vec<Vec<u8>> = self.free.iter().map(BtreeBitmap::to_vec).collect();

        let header_bytes = 1 + 3 + size_of::<u32>();
        let offsets_bytes = (self.max_order as usize + 1) * size_of::<u32>();
        let data_bytes: usize = serialized.iter().map(Vec::len).sum();

        let mut result = Vec::with_capacity(header_bytes + offsets_bytes + data_bytes);
        result.push(self.max_order);
        result.extend([0u8; 3]);
        result.extend(self.len.to_le_bytes());

        let mut data_offset = header_bytes + offsets_bytes;
        for bitmap in &serialized {
            data_offset += bitmap.len();
            let offset_u32: u32 = data_offset.try_into().unwrap();
            result.extend(offset_u32.to_le_bytes());
        }
        for bitmap in &serialized {
            result.extend_from_slice(bitmap);
        }

        result
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Self {
        Self::from_bytes_recording(data, None)
    }

    pub(super) fn from_bytes_recording(
        data: &[u8],
        facts: Option<&mut AllocatorParseRecorder>,
    ) -> Self {
        let layout = BuddyAllocatorEncodedLayout::ordinary(data);
        #[cfg(not(redb_no_std))]
        let value = Self::from_layout(layout, facts, None);
        #[cfg(redb_no_std)]
        let value = Self::from_layout(layout, facts);
        value.expect("ordinary Buddy materialization has no fallible Source admission")
    }

    #[cfg(not(redb_no_std))]
    pub(super) fn from_bytes_source(
        data: &[u8],
        resources: &super::page_resources::NativePageResources,
    ) -> crate::Result<Self> {
        Self::from_bytes_source_recording(data, resources, None)
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn from_bytes_source_recording(
        data: &[u8],
        resources: &super::page_resources::NativePageResources,
        facts: Option<&mut AllocatorParseRecorder>,
    ) -> crate::Result<Self> {
        let layout = BuddyAllocatorEncodedLayout::inspect(data)
            .map_err(|_| super::page_resources::refused())?;
        let _ = layout
            .source_capacity_bound()
            .map_err(|_| super::page_resources::refused())?;
        Self::from_layout(layout, facts, Some(resources))
    }

    fn from_layout(
        layout: BuddyAllocatorEncodedLayout<'_>,
        mut facts: Option<&mut AllocatorParseRecorder>,
        #[cfg(not(redb_no_std))] resources: Option<&super::page_resources::NativePageResources>,
    ) -> crate::Result<Self> {
        #[cfg(not(redb_no_std))]
        if resources.is_some() && !layout.checked {
            return Err(super::page_resources::refused());
        }
        #[cfg(not(redb_no_std))]
        let mut free = if let Some(resources) = resources {
            NativeOwnedAllocatorVec::source(layout.count(), resources)?
        } else {
            NativeOwnedAllocatorVec::default()
        };
        #[cfg(redb_no_std)]
        let mut free = NativeOwnedAllocatorVec::default();
        #[cfg(not(redb_no_std))]
        if resources.is_some() {
            if let Some(facts) = facts.as_deref_mut() {
                facts.replace_vec::<BtreeBitmap>(0, free.capacity());
            }
        }
        for order in layout.orders() {
            let order = order.map_err(|_| super::page_resources::refused())?;
            #[cfg(not(redb_no_std))]
            let bitmap = BtreeBitmap::from_layout(order, facts.as_deref_mut(), resources)?;
            #[cfg(redb_no_std)]
            let bitmap = BtreeBitmap::from_layout(order, facts.as_deref_mut())?;
            let old_capacity = free.capacity();
            free.push(bitmap);
            if let Some(facts) = facts.as_deref_mut() {
                facts.replace_vec::<BtreeBitmap>(old_capacity, free.capacity());
            }
        }
        Ok(Self {
            free,
            len: layout.num_pages,
            max_order: layout.max_order,
        })
    }

    pub(super) fn record_capacity(&self, facts: &mut AllocatorParseRecorder) {
        facts.replace_vec::<BtreeBitmap>(0, self.free.capacity());
        for bitmap in &self.free {
            bitmap.record_capacity(facts);
        }
    }

    #[inline]
    pub(crate) fn highest_free_order(&self) -> Option<u8> {
        (0..=self.max_order)
            .rev()
            .find(|order| self.get_order_free(*order).has_unset())
    }

    pub(crate) fn count_allocated_pages(&self) -> u32 {
        self.len() - self.count_free_pages()
    }

    pub(crate) fn count_free_pages(&self) -> u32 {
        let mut pages = 0;
        for order in 0..=self.max_order {
            pages += self.get_order_free(order).count_unset() * 2u32.pow(order.into());
        }
        pages
    }

    pub(crate) fn get_max_order(&self) -> u8 {
        self.max_order
    }

    fn find_free_order(&self, mut page: u32) -> Option<u8> {
        for order in 0..=self.max_order {
            if page < self.get_order_free(order).len() && !self.get_order_free(order).get(page) {
                return Some(order);
            }
            page = next_higher_order(page);
        }
        None
    }

    pub(crate) fn trailing_free_pages(&self) -> u32 {
        let mut free_pages = 0;
        let mut next_page = self.len() - 1;
        while let Some(order) = self.find_free_order(next_page) {
            let order_size = 2u32.pow(order.into());
            free_pages += order_size;
            if order_size > next_page {
                break;
            }
            next_page -= order_size;
        }

        free_pages
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    #[cfg_attr(not(test), expect(clippy::unused_self))]
    #[cfg_attr(not(test), expect(unused_variables))]
    pub(crate) fn check_allocated_pages(&self, region: u32, allocated_pages: &[PageNumber]) {
        #[cfg(test)]
        // Check the result against the free index to be sure it matches
        {
            let mut free_check = PageNumberHashSet::default();
            for i in 0..self.len() {
                if self.find_free_order(i).is_none() {
                    free_check.insert(PageNumber::new(region, i, 0));
                }
            }

            let mut check_result = PageNumberHashSet::default();
            for page in allocated_pages {
                check_result.extend(page.to_order0());
            }
            assert_eq!(free_check, check_result);
        }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    pub(crate) fn resize(&mut self, new_size: u32) {
        self.debug_check_consistency();
        if new_size > self.len() {
            let mut processed_pages = self.len();
            let mut pages_for_order = new_size;
            for order in &mut self.free {
                order.resize(pages_for_order, true);
                pages_for_order = next_higher_order(pages_for_order);
            }
            // Align to the highest order possible
            while processed_pages < new_size {
                let order: u8 = processed_pages.trailing_zeros().try_into().unwrap();
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if order >= self.max_order || processed_pages + order_size > new_size {
                    break;
                }
                self.free_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.max_order).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= new_size {
                    let page = processed_pages / order_size;
                    self.free_inner(page, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, new_size);
        } else {
            let mut processed_pages = new_size;
            // Align to the highest order possible
            while processed_pages < self.len() {
                let order: u8 = processed_pages.trailing_zeros().try_into().unwrap();
                if order >= self.max_order {
                    break;
                }
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if processed_pages + order_size > self.len() {
                    break;
                }
                assert!(self.record_alloc_inner(page, order));
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.max_order).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= self.len() {
                    let page = processed_pages / order_size;
                    assert!(self.record_alloc_inner(page, order));
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, self.len());
            let mut pages_for_order = new_size;
            for order in &mut self.free {
                order.resize(pages_for_order, true);
                pages_for_order = next_higher_order(pages_for_order);
            }
        }
        self.len = new_size;
        self.debug_check_consistency();
    }

    #[allow(unused_variables)]
    fn debug_check_consistency(&self) {
        // Don't enable when fuzzing, because this is kind of expensive
        #[cfg(all(debug_assertions, not(fuzzing)))]
        {
            let mut processed = 0;
            // Ensure that no page is free at multiple orders
            while processed < self.len() {
                let mut found = false;
                let mut page = processed;
                for order in 0..=self.max_order {
                    let allocator = &self.free[order as usize];
                    if allocator.len() <= page {
                        break;
                    }
                    if !allocator.get(page) {
                        assert!(!found);
                        found = true;
                    }
                    page = next_higher_order(page);
                }
                processed += 1;
            }

            // Ensure that all buddy pages are merged, except at the highest order
            for order in (0..self.max_order).rev() {
                let order_len = self.len() / (2u32.pow(order.into()));
                let allocator = &self.free[order as usize];
                for page in 0..order_len {
                    if !allocator.get(page) {
                        let buddy = buddy_page(page);
                        if buddy < allocator.len() {
                            let buddy_allocated = allocator.get(buddy);
                            assert!(buddy_allocated, "order={order} page={page} buddy={buddy}");
                        }
                    }
                }
            }
        }
    }

    // Allocates a page of the given order at the lowest index possible. Will split a higher order page,
    // to get one of lower index
    pub(crate) fn alloc_lowest(&mut self, order: u8) -> Option<u32> {
        // Lowest index at the requested order
        let mut best_index_at_order = self.alloc_inner(order)?;
        // Best (index, order) found
        let mut best = (best_index_at_order, order);

        // Find the lowest index at which we can fill this allocation, across all orders
        let mut multiplier = 2;
        for i in (order + 1)..=self.max_order {
            if let Some(index) = self.alloc_inner(i) {
                let index_at_order = index * multiplier;
                if index_at_order < best_index_at_order {
                    self.free_inner(best.0, best.1);
                    best_index_at_order = index_at_order;
                    best = (index, i);
                } else {
                    self.free_inner(index, i);
                }
            }
            multiplier *= 2;
        }

        // Split the page, until we get to the requested order
        while best.1 > order {
            let (best_index, best_order) = best;
            let (free1, free2) = (best_index * 2, best_index * 2 + 1);
            let allocator = self.get_order_free_mut(best_order - 1);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);
            best = (free1, best_order - 1);
        }
        assert_eq!(best.0, best_index_at_order);

        Some(best.0)
    }

    pub(crate) fn alloc(&mut self, order: u8) -> Option<u32> {
        self.alloc_inner(order)
    }

    pub(crate) fn alloc_inner(&mut self, order: u8) -> Option<u32> {
        if order > self.max_order {
            return None;
        }
        let allocator = self.get_order_free_mut(order);
        if let Some(x) = allocator.alloc() {
            Some(x)
        } else {
            // Try to allocate a higher order page and split it
            let upper_page = self.alloc_inner(order + 1)?;
            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            let allocator = self.get_order_free_mut(order);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);

            Some(free1)
        }
    }

    // `page_number` must be free. Returns false if it cannot be marked allocated: the order is too
    // large for this region, the page is out of range, or the space is already in use. Rebuilding
    // the allocator state feeds this page numbers read from the file, where any of the three means
    // the file is corrupt; the callers that compute page numbers themselves assert instead.
    #[must_use]
    pub(crate) fn record_alloc(&mut self, page_number: u32, order: u8) -> bool {
        // Split parent pages as necessary, and update the free index
        self.record_alloc_inner(page_number, order)
    }

    #[must_use]
    pub(crate) fn record_alloc_inner(&mut self, page_number: u32, order: u8) -> bool {
        // Marking a page that is already allocated walks up the orders looking for a free parent to
        // split, so running off the top is how a duplicate or overlapping page number shows up
        if order > self.max_order {
            return false;
        }
        let allocator = self.get_order_free_mut(order);
        if page_number >= allocator.len() {
            return false;
        }
        if allocator.get(page_number) {
            // Need to split parent page
            let upper_page = next_higher_order(page_number);
            if !self.record_alloc_inner(upper_page, order + 1) {
                return false;
            }
            let allocator = self.get_order_free_mut(order);

            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            debug_assert!(free1 == page_number || free2 == page_number);
            if free1 == page_number {
                allocator.clear(free2);
            } else {
                allocator.clear(free1);
            }
        } else {
            allocator.set(page_number);
        }

        true
    }

    /// data must have been initialized by `Self::init_new()`
    ///
    /// Returns the order of the resulting free block, which is `>= order` when buddies merge.
    pub(crate) fn free(&mut self, page_number: u32, order: u8) -> u8 {
        debug_assert!(self.get_order_free_mut(order).get(page_number));

        // Update the free index and merge free pages
        self.free_inner(page_number, order)
    }

    // Returns the order of the resulting free block, i.e. the order at which merging stopped.
    pub(crate) fn free_inner(&mut self, page_number: u32, order: u8) -> u8 {
        if order == self.max_order {
            let allocator = self.get_order_free_mut(order);
            allocator.clear(page_number);
            return order;
        }

        let allocator = self.get_order_free_mut(order);
        let buddy = buddy_page(page_number);
        if buddy >= allocator.len() || allocator.get(buddy) {
            allocator.clear(page_number);
            order
        } else {
            // Merge into higher order page
            allocator.set(buddy);
            self.free_inner(next_higher_order(page_number), order + 1)
        }
    }

    fn get_order_free_mut(&mut self, order: u8) -> &mut BtreeBitmap {
        &mut self.free[order as usize]
    }

    fn get_order_free(&self, order: u8) -> &BtreeBitmap {
        &self.free[order as usize]
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;

    #[test]
    fn record_alloc_buddy() {
        let num_pages = 256;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        for page in 0..num_pages {
            assert!(allocator.record_alloc(page, 0));
        }
        assert_eq!(allocator.count_allocated_pages(), num_pages);

        assert!(allocator.alloc(0).is_none());

        for page in 0..num_pages {
            allocator.free(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);
    }

    #[test]
    fn buddy_merge() {
        let num_pages = 256;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        for _ in 0..num_pages {
            allocator.alloc(0).unwrap();
        }
        for page in 0..num_pages {
            allocator.free(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);

        // Test that everything got merged back together, so that we fill order 7 allocations
        for _ in 0..(num_pages / 2u32.pow(7)) {
            allocator.alloc(7).unwrap();
        }
    }

    #[test]
    fn alloc_large() {
        let num_pages = 256;
        let max_order = 7;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        let mut allocated = vec![];
        for order in 0..=max_order {
            allocated.push((allocator.alloc(order).unwrap(), order));
        }
        assert_eq!(allocator.count_allocated_pages(), num_pages - 1);

        for order in 1..=max_order {
            assert!(allocator.alloc(order).is_none());
        }

        for (page, order) in allocated {
            allocator.free(page, order);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);
    }

    #[test]
    fn serialized_size() {
        // Check that serialized size is as expected for a full region
        let max_region_pages = 1024 * 1024;
        let allocator = BuddyAllocator::new(max_region_pages, max_region_pages);
        let max_region_pages = u64::from(max_region_pages);
        // 2x because that's the integral of 1/2^x to account for all the 21 orders
        let allocated_state_bits = 2 * max_region_pages;
        // + u32 * 21 because of the length field
        let allocated_state_bytes = allocated_state_bits / 8 + 4 * 21;

        // Add 2/64 to account for the intermediate index of the bitmap tree
        let free_state_bits = allocated_state_bits + allocated_state_bits * 2 / 64;
        // Add u32s to account for the stored offsets. Might be a bit of an over estimate
        let free_state_bytes = free_state_bits / 8 + 4 * 21 * 3 + 21 * 4;
        // BuddyAllocator overhead
        let buddy_state_bytes = free_state_bytes + allocated_state_bytes + 21 * 2 * 4 + 8;

        assert!((allocator.to_vec().len() as u64) <= buddy_state_bytes);
    }
}

#[cfg(all(test, not(redb_no_std)))]
mod encoded_layout_source_tests {
    use super::super::bitmap::U64GroupedBitmap;
    use super::super::container_bounds;
    use super::super::page_resources::NativePageResources;
    use super::*;
    use std::panic::catch_unwind;

    fn genuine_bytes() -> Vec<u8> {
        let mut original = BuddyAllocator::new(4097, 8192);
        // Keep genuine allocated pages inside the later shrink boundary.
        assert_eq!(original.alloc_lowest(0), Some(0));
        assert_eq!(original.alloc_lowest(2), Some(1));
        original.to_vec()
    }
    fn limit(bytes: u64) -> NativePageResources {
        NativePageResources::from_retained_pool_data(usize::try_from(bytes).unwrap(), ()).unwrap()
    }

    #[test]
    // req: STO-020, REC-001
    fn native_nested_allocator_checked_layout_matches_genuine_materialization() {
        let encoded = genuine_bytes();
        let layout = BuddyAllocatorEncodedLayout::inspect(&encoded).unwrap();
        assert_eq!(layout.consumed_end(), Some(encoded.len()));
        let needed = layout.source_capacity_bound().unwrap();
        let resources = limit(needed);
        let mut facts = AllocatorParseRecorder::new();
        let mut source =
            BuddyAllocator::from_bytes_source_recording(&encoded, &resources, Some(&mut facts))
                .unwrap();
        let mut ordinary = BuddyAllocator::from_bytes(&encoded);
        assert_eq!(source.free.capacity(), layout.count());
        assert_eq!(source.to_vec(), encoded);
        assert_eq!(
            source.count_allocated_pages(),
            ordinary.count_allocated_pages()
        );
        assert_eq!(source.alloc(1), ordinary.alloc(1));
        assert_eq!(source.to_vec(), ordinary.to_vec());
        assert_eq!(resources.current(), usize::try_from(needed).unwrap());
        let observed = facts.finish().unwrap().retained();
        assert!(observed.buffer_count() > layout.count());
        assert!(observed.capacity_bytes() <= resources.current());
        drop(source);
        assert_eq!(resources.current(), 0);

        let mut tree = BtreeBitmap::new_padded(8193, 8193, 1_048_576);
        for bit in [0, 64, 8192] {
            tree.clear(bit);
        }
        let encoded = tree.to_vec();
        let layout = BtreeBitmapEncodedLayout::inspect(&encoded).unwrap();
        let needed = layout.source_capacity_bound().unwrap();
        let resources = limit(needed);
        let source = BtreeBitmap::from_bytes_source(&encoded, &resources).unwrap();
        assert_eq!(source.to_vec(), tree.to_vec());
        for bit in [0, 64, 8192] {
            assert!(!source.get(bit));
        }
        assert!(source.get(1));
        assert_eq!(resources.current(), usize::try_from(needed).unwrap());
        drop(source);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_nested_allocator_pre_resize_backing_stays_charged_until_drop() {
        let encoded = genuine_bytes();
        let needed = BuddyAllocatorEncodedLayout::inspect(&encoded)
            .unwrap()
            .source_capacity_bound()
            .unwrap();
        let resources = limit(needed);
        let mut source = BuddyAllocator::from_bytes_source(&encoded, &resources).unwrap();
        let mut ordinary = BuddyAllocator::from_bytes(&encoded);
        source.resize(65);
        ordinary.resize(65);
        let smaller = source.to_vec();
        assert_eq!(smaller, ordinary.to_vec());
        assert_eq!(
            source.count_allocated_pages(),
            ordinary.count_allocated_pages()
        );
        assert!(smaller.len() < encoded.len());
        let smaller_bound = BuddyAllocatorEncodedLayout::inspect(&smaller)
            .unwrap()
            .source_capacity_bound()
            .unwrap();
        assert!(smaller_bound < needed);
        assert_eq!(resources.current(), usize::try_from(needed).unwrap());
        drop(source);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_nested_allocator_checked_refusal_preserves_legacy_and_trailing_bytes() {
        let encoded = genuine_bytes();
        let mut malformed = Vec::new();
        for end in 0..FREE_END_OFFSETS {
            malformed.push(encoded[..end].to_vec());
        }
        malformed.push(encoded[..encoded.len() - 1].to_vec());
        let mut backwards = encoded.clone();
        backwards[FREE_END_OFFSETS..FREE_END_OFFSETS + size_of::<u32>()]
            .copy_from_slice(&0u32.to_le_bytes());
        malformed.push(backwards);
        for bytes in malformed {
            assert!(BuddyAllocatorEncodedLayout::inspect(&bytes).is_err());
            let resources = limit(1_048_576);
            let mut facts = AllocatorParseRecorder::new();
            assert!(
                BuddyAllocator::from_bytes_source_recording(&bytes, &resources, Some(&mut facts),)
                    .is_err()
            );
            assert_eq!(resources.current(), 0);
            assert_eq!(facts.finish().unwrap().retained().buffer_count(), 0);
            assert!(catch_unwind(|| BuddyAllocator::from_bytes(&bytes)).is_err());
        }
        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&[17, 23, 99]);
        let layout = BuddyAllocatorEncodedLayout::inspect(&trailing).unwrap();
        assert_eq!(layout.consumed_end(), Some(encoded.len()));
        let needed = layout.source_capacity_bound().unwrap();
        assert_eq!(
            needed,
            BuddyAllocatorEncodedLayout::inspect(&encoded)
                .unwrap()
                .source_capacity_bound()
                .unwrap()
        );
        let resources = limit(needed);
        let source = BuddyAllocator::from_bytes_source(&trailing, &resources).unwrap();
        assert_eq!(source.to_vec(), encoded);
        assert_eq!(BuddyAllocator::from_bytes(&trailing).to_vec(), encoded);
        drop(source);
        assert_eq!(resources.current(), 0);

        let encoded = BtreeBitmap::new_padded(4097, 4097, 8192).to_vec();
        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&[5, 8, 13]);
        let layout = BtreeBitmapEncodedLayout::inspect(&trailing).unwrap();
        assert_eq!(layout.consumed_end(), Some(encoded.len()));
        let resources = limit(layout.source_capacity_bound().unwrap());
        let source = BtreeBitmap::from_bytes_source(&trailing, &resources).unwrap();
        assert_eq!(source.to_vec(), encoded);
        assert_eq!(BtreeBitmap::from_bytes(&trailing).to_vec(), encoded);
        drop(source);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_nested_allocator_n_minus_one_and_mid_child_failure_refund() {
        let encoded = genuine_bytes();
        let layout = BuddyAllocatorEncodedLayout::inspect(&encoded).unwrap();
        let needed = layout.source_capacity_bound().unwrap();
        let resources = limit(needed.checked_sub(1).unwrap());
        // External complete-quote admission is separate from the per-vector
        // constructor. Its refusal cannot allocate any child backing.
        assert!(
            resources
                .admit_control(usize::try_from(needed).unwrap())
                .is_err()
        );
        assert_eq!(resources.current(), 0);
        let mut facts = AllocatorParseRecorder::new();
        assert!(
            BuddyAllocator::from_bytes_source_recording(&encoded, &resources, Some(&mut facts),)
                .is_err()
        );
        assert!(facts.finish().unwrap().retained().buffer_count() > 0);
        assert_eq!(resources.current(), 0);

        let mut orders = layout.orders();
        let first = orders.next().unwrap().unwrap();
        let second = orders.next().unwrap().unwrap();
        assert!(first.count() > 0 && second.count() > 0);
        let outer = container_bounds::vector_storage::<BtreeBitmap>(layout.count())
            .unwrap()
            .allocator_bytes;
        let first_bytes = first.source_capacity_bound().unwrap();
        let second_outer = container_bounds::vector_storage::<U64GroupedBitmap>(second.count())
            .unwrap()
            .allocator_bytes;
        let partial_limit = outer
            .checked_add(first_bytes)
            .unwrap()
            .checked_add(second_outer)
            .unwrap()
            .checked_sub(1)
            .unwrap();
        assert!(partial_limit < needed);
        let resources = limit(partial_limit);
        let mut facts = AllocatorParseRecorder::new();
        assert!(
            BuddyAllocator::from_bytes_source_recording(&encoded, &resources, Some(&mut facts),)
                .is_err()
        );
        // First child was fully materialized and moved into the outer owner;
        // the next child's outer backing was refused before construction.
        assert_eq!(
            facts.finish().unwrap().retained().buffer_count(),
            first.count() + 2
        );
        assert_eq!(resources.current(), 0);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_borrowed_buddy_resize_quote_covers_actual_guarded_preparation() {
        let encoded = genuine_bytes();
        let layout = BuddyAllocatorEncodedLayout::inspect(&encoded).unwrap();
        for target in [65, 4097, 8192] {
            let parsed = layout.source_capacity_bound().unwrap();
            let resized = layout.source_resize_capacity_bound(target).unwrap();
            let complete_peak = parsed.max(resized.simultaneous_peak_bytes);
            let resources = limit(complete_peak);
            assert_eq!(resources.current(), 0);
            let mut source = BuddyAllocator::from_bytes_source(&encoded, &resources).unwrap();
            if target > source.len() {
                source.prepare_source_resize(target, &resources).unwrap();
            }
            source.resize(target);
            let mut ordinary = BuddyAllocator::from_bytes(&encoded);
            ordinary.resize(target);
            assert_eq!(source.to_vec(), ordinary.to_vec());
            assert_eq!(
                source.count_allocated_pages(),
                ordinary.count_allocated_pages()
            );
            assert!(resources.current() as u64 <= resized.retained_after_bytes);
            assert!(resources.admitted_high_water_for_test() as u64 <= complete_peak);
            drop(source);
            assert_eq!(resources.current(), 0);
        }
    }

    #[test]
    // req: STO-020, REC-001
    fn native_borrowed_resize_complete_quote_refuses_n_minus_one_before_materialization() {
        let encoded = genuine_bytes();
        let layout = BuddyAllocatorEncodedLayout::inspect(&encoded).unwrap();
        let parsed = layout.source_capacity_bound().unwrap();
        let resized = layout.source_resize_capacity_bound(8192).unwrap();
        let peak = parsed.max(resized.simultaneous_peak_bytes);
        assert!(
            peak > parsed,
            "real guarded growth requires more than the parser-only allowance"
        );
        let resources = limit(peak.checked_sub(1).unwrap());
        // Actual native numerical admission, not a numeric comparison or a
        // Source authority. The conservative upper is not an internal exact
        // minimum: this test does not require construction at peak-1 to fail.
        assert!(
            resources
                .admit_control(usize::try_from(peak).unwrap())
                .is_err()
        );
        assert_eq!(resources.current(), 0);
        assert_eq!(resources.admitted_high_water_for_test(), 0);
        let resources = limit(peak);
        let preflight = resources
            .admit_control(usize::try_from(peak).unwrap())
            .unwrap();
        assert_eq!(resources.current(), usize::try_from(peak).unwrap());
        drop(preflight);
        assert_eq!(resources.current(), 0);
        let mut source = BuddyAllocator::from_bytes_source(&encoded, &resources).unwrap();
        source.prepare_source_resize(8192, &resources).unwrap();
        source.resize(8192);
        let mut ordinary = BuddyAllocator::from_bytes(&encoded);
        ordinary.resize(8192);
        assert_eq!(source.to_vec(), ordinary.to_vec());
        drop(source);
        assert_eq!(resources.current(), 0);
    }
}
