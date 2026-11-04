use core::ops::Range;

fn round_up_to_multiple_of(value: u64, multiple: u64) -> u64 {
    if value.is_multiple_of(multiple) {
        value
    } else {
        value + multiple - value % multiple
    }
}

// Regions are laid out starting with the allocator state header, followed by the pages aligned
// to the next page
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RegionLayout {
    num_pages: u32,
    // Offset where data pages start
    header_pages: u32,
    page_size: u32,
}

impl RegionLayout {
    pub(super) fn new(num_pages: u32, header_pages: u32, page_size: u32) -> Self {
        assert!(num_pages > 0);
        Self {
            num_pages,
            header_pages,
            page_size,
        }
    }

    pub(super) fn calculate(
        desired_usable_bytes: u64,
        page_capacity: u32,
        region_header_pages: u32,
        page_size: u32,
    ) -> RegionLayout {
        assert!(desired_usable_bytes <= u64::from(page_capacity) * u64::from(page_size));
        let num_pages =
            round_up_to_multiple_of(desired_usable_bytes, page_size.into()) / u64::from(page_size);

        Self {
            num_pages: num_pages.try_into().unwrap(),
            header_pages: region_header_pages,
            page_size,
        }
    }

    pub(super) fn data_section(&self) -> Range<u64> {
        let header_bytes = u64::from(self.header_pages) * u64::from(self.page_size);
        header_bytes..(header_bytes + self.usable_bytes())
    }

    pub(super) fn get_header_pages(&self) -> u32 {
        self.header_pages
    }

    pub(super) fn num_pages(&self) -> u32 {
        self.num_pages
    }

    pub(super) fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(super) fn len(&self) -> u64 {
        u64::from(self.header_pages) * u64::from(self.page_size) + self.usable_bytes()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        u64::from(self.page_size) * u64::from(self.num_pages)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseLayout {
    full_region_layout: RegionLayout,
    num_full_regions: u32,
    trailing_partial_region: Option<RegionLayout>,
}

impl DatabaseLayout {
    // The sole growth geometry is shared by execution and prospective resource
    // planning. It allocates nothing and refuses overflow before file resize.
    pub(super) fn growth_for(
        &self,
        required_order: u8,
    ) -> Result<Self, super::allocator_reopen_facts::AllocatorResourceError> {
        use super::allocator_reopen_facts::AllocatorResourceError as E;
        use super::base::{MAX_PAGE_INDEX, MAX_REGIONS};
        use super::page_manager::MAX_MAX_PAGE_ORDER;
        let full = self.full_region_layout;
        let pages = full.num_pages;
        let page_size = u64::from(full.page_size);
        if pages == 0
            || pages > MAX_PAGE_INDEX + 1
            || page_size == 0
            || !full.page_size.is_power_of_two()
            || required_order > MAX_MAX_PAGE_ORDER
        {
            return Err(E::Unsupported);
        }
        let required_pages = 1_u64
            .checked_shl(u32::from(required_order))
            .ok_or(E::Overflow)?;
        if required_pages > u64::from(pages) {
            return Err(E::Unsupported);
        }
        let region_bytes = u64::from(pages).checked_mul(page_size).ok_or(E::Overflow)?;
        let required = required_pages.checked_mul(page_size).ok_or(E::Overflow)?;
        let trailing = if let Some(tail) = self.trailing_partial_region {
            // Native calculate() legitimately represents one exactly-full
            // region as a trailing region, so equality remains supported.
            if tail.num_pages == 0
                || tail.num_pages > pages
                || tail.header_pages != full.header_pages
                || tail.page_size != full.page_size
            {
                return Err(E::Incomplete);
            }
            u64::from(tail.num_pages)
                .checked_mul(page_size)
                .ok_or(E::Overflow)?
        } else {
            0
        };
        let regions = self
            .num_full_regions
            .checked_add(u32::from(self.trailing_partial_region.is_some()))
            .ok_or(E::Overflow)?;
        if regions == 0 || regions > MAX_REGIONS {
            return Err(E::Unsupported);
        }
        let usable = u64::from(self.num_full_regions)
            .checked_mul(region_bytes)
            .and_then(|n| n.checked_add(trailing))
            .ok_or(E::Overflow)?;
        let twice_required = required.checked_mul(2).ok_or(E::Overflow)?;
        let desired = if self.num_full_regions > 0 {
            if self.trailing_partial_region.is_some() {
                let remainder = region_bytes.checked_sub(trailing).ok_or(E::Incomplete)?;
                let extra = if twice_required < remainder {
                    remainder
                } else {
                    remainder.checked_add(region_bytes).ok_or(E::Overflow)?
                };
                usable.checked_add(extra).ok_or(E::Overflow)?
            } else {
                usable.checked_add(region_bytes).ok_or(E::Overflow)?
            }
        } else {
            usable
                .checked_mul(2)
                .ok_or(E::Overflow)?
                .max(usable.checked_add(twice_required).ok_or(E::Overflow)?)
        };
        let next_regions = desired.div_ceil(region_bytes);
        if next_regions == 0 || next_regions > u64::from(MAX_REGIONS) {
            return Err(E::Unsupported);
        }
        // Check the complete physical length before calling the original
        // native layout constructor, whose trusted arithmetic is unchecked.
        let next_full = if desired <= region_bytes {
            0
        } else {
            desired / region_bytes
        };
        let next_trailing = desired
            .checked_sub(next_full.checked_mul(region_bytes).ok_or(E::Overflow)?)
            .ok_or(E::Overflow)?;
        let header_bytes = u64::from(full.header_pages)
            .checked_mul(page_size)
            .ok_or(E::Overflow)?;
        let full_bytes = region_bytes.checked_add(header_bytes).ok_or(E::Overflow)?;
        page_size
            .checked_add(next_full.checked_mul(full_bytes).ok_or(E::Overflow)?)
            .and_then(|n| {
                if next_trailing == 0 {
                    Some(n)
                } else {
                    n.checked_add(header_bytes)?.checked_add(next_trailing)
                }
            })
            .ok_or(E::Overflow)?;
        Ok(Self::calculate(
            desired,
            pages,
            full.header_pages,
            full.page_size,
        ))
    }
    pub(super) fn new(
        full_regions: u32,
        full_region: RegionLayout,
        trailing_region: Option<RegionLayout>,
    ) -> Self {
        Self {
            full_region_layout: full_region,
            num_full_regions: full_regions,
            trailing_partial_region: trailing_region,
        }
    }

    pub(super) fn reduce_last_region(&mut self, pages: u32) {
        if let Some(ref mut trailing) = self.trailing_partial_region {
            assert!(pages <= trailing.num_pages);
            trailing.num_pages -= pages;
            if trailing.num_pages == 0 {
                self.trailing_partial_region = None;
            }
        } else {
            self.num_full_regions -= 1;
            let full_layout = self.full_region_layout;
            if full_layout.num_pages > pages {
                self.trailing_partial_region = Some(RegionLayout::new(
                    full_layout.num_pages - pages,
                    full_layout.header_pages,
                    full_layout.page_size,
                ));
            }
        }
    }

    pub(super) fn recalculate(
        file_len: u64,
        region_header_pages_u32: u32,
        region_max_data_pages_u32: u32,
        page_size_u32: u32,
    ) -> Self {
        let page_size = u64::from(page_size_u32);
        let region_header_pages = u64::from(region_header_pages_u32);
        let region_max_data_pages = u64::from(region_max_data_pages_u32);
        // Super-header
        let mut remaining = file_len - page_size;
        let full_region_size = (region_header_pages + region_max_data_pages) * page_size;
        let full_regions = remaining / full_region_size;
        remaining -= full_regions * full_region_size;
        let trailing = if remaining >= (region_header_pages + 1) * page_size {
            remaining -= region_header_pages * page_size;
            let remaining: u32 = remaining.try_into().unwrap();
            let data_pages = remaining / page_size_u32;
            assert!(data_pages < region_max_data_pages_u32);
            Some(RegionLayout::new(
                data_pages,
                region_header_pages_u32,
                page_size_u32,
            ))
        } else {
            None
        };
        let full_layout = RegionLayout::new(
            region_max_data_pages_u32,
            region_header_pages_u32,
            page_size_u32,
        );

        Self {
            full_region_layout: full_layout,
            num_full_regions: full_regions.try_into().unwrap(),
            trailing_partial_region: trailing,
        }
    }

    pub(super) fn calculate(
        desired_usable_bytes: u64,
        page_capacity: u32,
        region_header_pages: u32,
        page_size: u32,
    ) -> Self {
        let full_region_layout = RegionLayout::new(page_capacity, region_header_pages, page_size);
        if desired_usable_bytes <= full_region_layout.usable_bytes() {
            // Single region layout
            let region_layout = RegionLayout::calculate(
                desired_usable_bytes,
                page_capacity,
                region_header_pages,
                page_size,
            );
            DatabaseLayout {
                full_region_layout,
                num_full_regions: 0,
                trailing_partial_region: Some(region_layout),
            }
        } else {
            // Multi region layout
            let full_regions = desired_usable_bytes / full_region_layout.usable_bytes();
            let remaining_desired =
                desired_usable_bytes - full_regions * full_region_layout.usable_bytes();
            assert!(full_regions > 0);
            let trailing_region = if remaining_desired > 0 {
                Some(RegionLayout::calculate(
                    remaining_desired,
                    page_capacity,
                    region_header_pages,
                    page_size,
                ))
            } else {
                None
            };
            if let Some(ref region) = trailing_region {
                // All regions must have the same header size
                assert_eq!(region.header_pages, full_region_layout.header_pages);
            }
            DatabaseLayout {
                full_region_layout,
                num_full_regions: full_regions.try_into().unwrap(),
                trailing_partial_region: trailing_region,
            }
        }
    }

    pub(super) fn full_region_layout(&self) -> &RegionLayout {
        &self.full_region_layout
    }

    pub(super) fn trailing_region_layout(&self) -> Option<&RegionLayout> {
        self.trailing_partial_region.as_ref()
    }

    pub(super) fn num_full_regions(&self) -> u32 {
        self.num_full_regions
    }

    pub(super) fn num_regions(&self) -> u32 {
        if self.trailing_partial_region.is_some() {
            self.num_full_regions + 1
        } else {
            self.num_full_regions
        }
    }

    pub(super) fn len(&self) -> u64 {
        let last = self.num_regions() - 1;
        self.region_base_address(last) + self.region_layout(last).len()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        let trailing = self
            .trailing_partial_region
            .as_ref()
            .map(RegionLayout::usable_bytes)
            .unwrap_or_default();
        u64::from(self.num_full_regions) * self.full_region_layout.usable_bytes() + trailing
    }

    pub(super) fn region_base_address(&self, region: u32) -> u64 {
        assert!(region < self.num_regions());
        u64::from(self.full_region_layout.page_size())
            + u64::from(region) * self.full_region_layout.len()
    }

    pub(super) fn region_layout(&self, region: u32) -> RegionLayout {
        assert!(region < self.num_regions());
        if region == self.num_full_regions {
            self.trailing_partial_region.unwrap()
        } else {
            self.full_region_layout
        }
    }
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "growth_layout_tests.rs"]
mod growth_tests;
