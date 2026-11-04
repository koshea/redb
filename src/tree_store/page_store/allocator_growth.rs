//! Prospective native allocator metadata only; no admission or write authority.
use super::allocator_reopen_facts::AllocatorResourceError as E;
use super::container_bounds::ContainerGrowthBound;

pub(super) type Growth = ContainerGrowthBound;
pub(super) type GrowthResult = Result<Growth, E>;

pub(super) fn sum(a: Growth, b: Growth) -> GrowthResult {
    Ok(Growth {
        retained_after_bytes: a
            .retained_after_bytes
            .checked_add(b.retained_after_bytes)
            .ok_or(E::Overflow)?,
        simultaneous_peak_bytes: a
            .simultaneous_peak_bytes
            .checked_add(b.simultaneous_peak_bytes)
            .ok_or(E::Overflow)?,
    })
}
pub(super) fn unchanged(bytes: u64) -> Growth {
    Growth {
        retained_after_bytes: bytes,
        simultaneous_peak_bytes: bytes,
    }
}
pub(super) fn map(error: super::container_bounds::ContainerBoundError) -> E {
    use super::container_bounds::ContainerBoundError as B;
    match error {
        B::Overflow => E::Overflow,
        B::InvalidLayout => E::Incomplete,
        B::UnsupportedTarget | B::UnsupportedBuild | B::UnsupportedElementLayout => E::Unsupported,
    }
}

// Only the closed guarded allocator preparation: capacities grow monotonically,
// and NativeOwnedAllocatorVec::prepare_capacity drops each old backing before
// the next replacement. This is numeric geometry, never Source authority.
pub(super) fn monotone_preparation(a: Growth, b: Growth) -> GrowthResult {
    let a_old = a
        .simultaneous_peak_bytes
        .checked_sub(a.retained_after_bytes)
        .ok_or(E::Incomplete)?;
    let b_old = b
        .simultaneous_peak_bytes
        .checked_sub(b.retained_after_bytes)
        .ok_or(E::Incomplete)?;
    let retained = a
        .retained_after_bytes
        .checked_add(b.retained_after_bytes)
        .ok_or(E::Overflow)?;
    Ok(Growth {
        retained_after_bytes: retained,
        simultaneous_peak_bytes: retained.checked_add(a_old.max(b_old)).ok_or(E::Overflow)?,
    })
}

pub(super) fn parsed_source_vec_preparation<T>(
    old_capacity: usize,
    required: usize,
) -> GrowthResult {
    use super::container_bounds as b;
    let old = b::vector_storage::<T>(old_capacity)
        .map_err(map)?
        .allocator_bytes;
    if required <= old_capacity {
        return Ok(unchanged(old));
    }
    let prepared = b::vector_storage::<T>(required)
        .map_err(map)?
        .allocator_bytes;
    Ok(Growth {
        retained_after_bytes: prepared,
        simultaneous_peak_bytes: prepared.checked_add(old).ok_or(E::Overflow)?,
    })
}
