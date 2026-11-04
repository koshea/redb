//! Sole native allocation allowance for the closed point census controls.
use super::container_bounds::vector_storage;
use super::resource_build_profile::NativeResourceBuildProfile;
use crate::tree_store::NativeKeyPathError as E;
use core::mem::size_of;
pub(crate) fn point_frontier_capacity<T, Owner, Scratch>(records: usize) -> Result<u64, E> {
    NativeResourceBuildProfile::require().map_err(|_| E::UnsupportedState)?;
    let vector = vector_storage::<T>(records)
        .map_err(|_| E::Overflow)?
        .allocator_bytes;
    let fixed = size_of::<Owner>()
        .checked_mul(2)
        .and_then(|n| {
            size_of::<Scratch>()
                .checked_mul(2)
                .and_then(|s| n.checked_add(s))
        })
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(E::Overflow)?;
    vector.checked_add(fixed).ok_or(E::Overflow)
}

// These allocations are newly constructed vectors, not an estimate of an
// existing Vec's physical ownership. One allowance per actual byte-vector.
pub(crate) fn point_vector_bytes<T>(capacity: usize) -> Result<u64, E> {
    NativeResourceBuildProfile::require().map_err(|_| E::UnsupportedState)?;
    Ok(vector_storage::<T>(capacity)
        .map_err(|_| E::Overflow)?
        .allocator_bytes)
}
pub(crate) fn point_byte_forest_bytes(vectors: usize, total_bytes: usize) -> Result<u64, E> {
    NativeResourceBuildProfile::require().map_err(|_| E::UnsupportedState)?;
    let payload = u64::try_from(total_bytes).map_err(|_| E::Overflow)?;
    let controls = u64::try_from(vectors).map_err(|_| E::Overflow)?;
    payload
        .checked_mul(2)
        .and_then(|n| controls.checked_mul(258).and_then(|c| n.checked_add(c)))
        .ok_or(E::Overflow)
}
