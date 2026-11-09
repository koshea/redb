//! Pinned Rust 1.99.1 / hashbrown 0.17.1 numerical layout bounds only.
//! Inline owners and initialized children's allocations are accounted separately.
use core::alloc::Layout;
use core::mem::{align_of, size_of};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ContainerBoundError {
    Overflow,
    InvalidLayout,
    UnsupportedTarget,
    UnsupportedBuild,
    UnsupportedElementLayout,
}

type BoundResult<T> = core::result::Result<T, ContainerBoundError>;

// Verified container request and the existing first-party allocator allowance
// are distinct. This value is a quote, never admission or ownership authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AllocationBound {
    pub(super) requested_bytes: usize,
    pub(super) alignment: usize,
    pub(super) allocator_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ContainerGrowthBound {
    pub(super) retained_after_bytes: u64,
    pub(super) simultaneous_peak_bytes: u64,
}

fn add(a: usize, b: usize) -> BoundResult<usize> {
    a.checked_add(b).ok_or(ContainerBoundError::Overflow)
}
fn mul(a: usize, b: usize) -> BoundResult<usize> {
    a.checked_mul(b).ok_or(ContainerBoundError::Overflow)
}
fn sum(a: u64, b: u64) -> BoundResult<u64> {
    a.checked_add(b).ok_or(ContainerBoundError::Overflow)
}
fn count_bytes(count: usize, bytes: u64) -> BoundResult<u64> {
    u64::try_from(count)
        .ok()
        .and_then(|count| count.checked_mul(bytes))
        .ok_or(ContainerBoundError::Overflow)
}

pub(super) fn allocation(bytes: usize, alignment: usize) -> BoundResult<AllocationBound> {
    Layout::from_size_align(bytes, alignment).map_err(|_| ContainerBoundError::InvalidLayout)?;
    let allocator_bytes = if bytes == 0 {
        0
    } else {
        // Exact contract from riffdb-types::owned_capacity::allocation_upper_bound.
        u64::try_from(bytes)
            .ok()
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| {
                u64::try_from(alignment)
                    .ok()
                    .and_then(|alignment| alignment.checked_mul(2))
                    .and_then(|alignment| bytes.checked_add(alignment))
            })
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or(ContainerBoundError::Overflow)?
    };
    Ok(AllocationBound {
        requested_bytes: bytes,
        alignment,
        allocator_bytes,
    })
}

// This closed initial profile matches the actual rehearsal compiler target.
// Other targets must provide their pinned selected Group layout before use.
fn hash_group_width() -> BoundResult<usize> {
    super::resource_build_profile::NativeResourceBuildProfile::require()?;
    if cfg!(all(
        target_arch = "x86_64",
        target_feature = "sse2",
        not(miri)
    )) {
        Ok(16)
    } else {
        Err(ContainerBoundError::UnsupportedTarget)
    }
}

fn buckets_for_fresh_capacity<T>(capacity: usize) -> BoundResult<usize> {
    let width = hash_group_width()?;
    if capacity == 0 {
        return Ok(0);
    }
    if capacity < 15 {
        let minimum = match (width, size_of::<T>()) {
            (16, 0..=1) => 14,
            (16, 2..=3) | (8, 0..=1) => 7,
            _ => 3,
        };
        let adjusted = capacity.max(minimum);
        return Ok(if adjusted < 4 {
            4
        } else if adjusted < 8 {
            8
        } else {
            16
        });
    }
    let adjusted = mul(capacity, 8)? / 7;
    adjusted
        .checked_next_power_of_two()
        .ok_or(ContainerBoundError::Overflow)
}

fn hash_storage_for_buckets<T>(buckets: usize) -> BoundResult<AllocationBound> {
    let width = hash_group_width()?;
    let alignment = align_of::<T>().max(width);
    if buckets == 0 {
        return allocation(0, alignment);
    }
    if buckets < 4 || !buckets.is_power_of_two() {
        return Err(ContainerBoundError::InvalidLayout);
    }
    // Pinned TableLayout::calculate_layout_for: payload, alignment gap,
    // one control byte per bucket and the trailing replicated control group.
    let payload = mul(size_of::<T>(), buckets)?;
    let control_offset = add(payload, alignment - 1)? & !(alignment - 1);
    let total = add(control_offset, add(buckets, width)?)?;
    allocation(total, alignment)
}

// Fresh empty HashMap::try_reserve(requested_entries), before any erasure.
// This is a request bound, not a witness inferred from an existing map's
// current capacity. The same pinned bucket geometry owns both calculations.
pub(super) fn hash_fresh_reserve_storage<T>(
    requested_entries: usize,
) -> BoundResult<AllocationBound> {
    hash_storage_for_buckets::<T>(buckets_for_fresh_capacity::<T>(requested_entries)?)
}

// The caller must retain the actual native owner's maximum fresh capacity ever
// observed after all intercepted allocation-producing operations. Current
// HashMap::capacity() is NOT this witness: tombstones can lower it with the same
// allocation retained. This pure function cannot mint that historical provenance.
pub(super) fn hash_storage_from_historical_fresh_capacity<T>(
    capacity_upper: usize,
) -> BoundResult<AllocationBound> {
    hash_storage_for_buckets::<T>(buckets_for_fresh_capacity::<T>(capacity_upper)?)
}

pub(super) fn hash_reserve_growth<T>(
    historical_fresh_capacity: usize,
    length: usize,
    additional: usize,
) -> BoundResult<ContainerGrowthBound> {
    let buckets = buckets_for_fresh_capacity::<T>(historical_fresh_capacity)?;
    let before = hash_storage_for_buckets::<T>(buckets)?;
    let full_capacity = if buckets == 0 {
        0
    } else if buckets < 8 {
        buckets - 1
    } else {
        mul(buckets / 8, 7)?
    };
    if length > full_capacity {
        return Err(ContainerBoundError::InvalidLayout);
    }
    if additional == 0 {
        return Ok(ContainerGrowthBound {
            retained_after_bytes: before.allocator_bytes,
            simultaneous_peak_bytes: before.allocator_bytes,
        });
    }
    // An unknown tombstone pattern may rehash in place or resize. Reserve for
    // resize_inner(max(items+additional, old_full_capacity+1)) in either case.
    let required = add(length, additional)?.max(add(full_capacity, 1)?);
    let after = hash_storage_for_buckets::<T>(buckets_for_fresh_capacity::<T>(required)?)?;
    Ok(ContainerGrowthBound {
        retained_after_bytes: before.allocator_bytes.max(after.allocator_bytes),
        simultaneous_peak_bytes: sum(before.allocator_bytes, after.allocator_bytes)?,
    })
}

pub(super) fn vector_storage<T>(observed_capacity: usize) -> BoundResult<AllocationBound> {
    // ZST Vec/VecDeque report usize::MAX capacity without a buffer allocation.
    let bytes = if size_of::<T>() == 0 {
        0
    } else {
        mul(observed_capacity, size_of::<T>())?
    };
    allocation(bytes, align_of::<T>())
}

pub(super) fn vector_amortized_capacity<T>(
    capacity: usize,
    length: usize,
    additional: usize,
) -> BoundResult<usize> {
    super::resource_build_profile::NativeResourceBuildProfile::require()?;
    let required = add(length, additional)?;
    if length > capacity {
        return Err(ContainerBoundError::InvalidLayout);
    }
    if size_of::<T>() == 0 || required <= capacity {
        return Ok(capacity);
    }
    let minimum = if size_of::<T>() == 1 {
        8
    } else if size_of::<T>() <= 1024 {
        4
    } else {
        1
    };
    Ok(mul(capacity, 2)?.max(required).max(minimum))
}

pub(super) fn vector_amortized_growth<T>(
    capacity: usize,
    length: usize,
    additional: usize,
) -> BoundResult<ContainerGrowthBound> {
    let next = vector_amortized_capacity::<T>(capacity, length, additional)?;
    let before = vector_storage::<T>(capacity)?;
    let after = vector_storage::<T>(next)?;
    let peak = if next == capacity {
        before.allocator_bytes
    } else {
        sum(before.allocator_bytes, after.allocator_bytes)?
    };
    Ok(ContainerGrowthBound {
        retained_after_bytes: after.allocator_bytes,
        simultaneous_peak_bytes: peak,
    })
}

// Bound repeated pushes, not one bulk reserve. Vec::resize may grow a bitmap
// one word at a time across multiple native region extensions. A bulk-reserve
// quote alone can underprice its eventual geometric capacity.
pub(super) fn vector_pushes<T>(
    mut capacity: usize,
    length: usize,
    additional: usize,
) -> BoundResult<ContainerGrowthBound> {
    let required = add(length, additional)?;
    if length > capacity {
        return Err(ContainerBoundError::InvalidLayout);
    }
    super::resource_build_profile::NativeResourceBuildProfile::require()?;
    let mut peak = vector_storage::<T>(capacity)?.allocator_bytes;
    if size_of::<T>() != 0 {
        while capacity < required {
            let next = vector_amortized_capacity::<T>(capacity, capacity, 1)?;
            let both = sum(
                vector_storage::<T>(capacity)?.allocator_bytes,
                vector_storage::<T>(next)?.allocator_bytes,
            )?;
            peak = peak.max(both);
            capacity = next;
        }
    }
    Ok(ContainerGrowthBound {
        retained_after_bytes: vector_storage::<T>(capacity)?.allocator_bytes,
        simultaneous_peak_bytes: peak,
    })
}

fn btree_internal_node<K, V>() -> BoundResult<AllocationBound> {
    super::resource_build_profile::NativeResourceBuildProfile::require()?;
    if size_of::<usize>() > 8 || align_of::<usize>() > 8 {
        return Err(ContainerBoundError::UnsupportedTarget);
    }
    // Closed native element profile. Over-aligned application/generic types
    // keep ordinary std semantics but are unsupported by this resource quote.
    if align_of::<K>() > 16 || align_of::<V>() > 16 {
        return Err(ContainerBoundError::UnsupportedElementLayout);
    }
    // Reuse the admitted first-party tree_storage layout envelope. Pinned std
    // has eleven K/V slots, a parent pointer/two u16 fields and twelve edges.
    // With pointer alignment <=8 and element alignment <=16, fixed fields
    // occupy <=108 bytes. All leaf field gaps + leaf tail + enclosing internal
    // edge gap/tail occupy <=76 bytes, including BOTH nested tail paddings.
    // The existing 256-byte fixed envelope exceeds 108+76=184; both element
    // alignments remain additionally charged. LeafNode is repr(Rust), so this
    // is a conservative envelope, not an exact field-order reproduction.
    let slots = mul(add(size_of::<K>(), size_of::<V>())?, 11)?;
    let requested = add(add(add(slots, 256)?, align_of::<K>())?, align_of::<V>())?;
    allocation(
        requested,
        align_of::<K>()
            .max(align_of::<V>())
            .max(align_of::<usize>()),
    )
}

pub(super) fn btree_storage<K, V>(entries: usize) -> BoundResult<u64> {
    // Every nonroot stable node owns at least one entry; an empty root may
    // remain after clear/removal. Charge each possible node separately so its
    // allocator envelope includes distinct allocation overhead and alignment.
    count_bytes(
        add(entries, 1)?,
        btree_internal_node::<K, V>()?.allocator_bytes,
    )
}

pub(super) fn btree_insert_growth<K, V>(
    entries: usize,
    additional: usize,
) -> BoundResult<ContainerGrowthBound> {
    if additional == 0 {
        let retained = btree_storage::<K, V>(entries)?;
        return Ok(ContainerGrowthBound {
            retained_after_bytes: retained,
            simultaneous_peak_bytes: retained,
        });
    }
    let maximum_entries = add(entries, additional)?;
    let node = btree_internal_node::<K, V>()?.allocator_bytes;
    let retained = count_bytes(add(maximum_entries, 1)?, node)?;
    // Pinned split propagation allocates at most one right sibling per level
    // and one new root. Height is at most the number of entries; reserve an
    // entire maximum_entries+2 node path alongside the stable graph. This is
    // deliberately independent of occupancy and does not clone child payloads.
    let transient_path = count_bytes(add(maximum_entries, 2)?, node)?;
    Ok(ContainerGrowthBound {
        retained_after_bytes: retained,
        simultaneous_peak_bytes: sum(retained, transient_path)?,
    })
}

#[cfg(all(test, not(redb_no_std)))]
#[path = "container_bounds_tests.rs"]
mod tests;
