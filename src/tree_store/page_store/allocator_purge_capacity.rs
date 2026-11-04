//! Prices only unchanged native purge vectors/paths, not a complete write graph.
use super::PageNumber;
use super::container_bounds::{ContainerBoundError, vector_amortized_growth, vector_storage};
use core::mem::size_of;

type BoundResult<T> = core::result::Result<T, ContainerBoundError>;
#[derive(Debug)]
pub(crate) struct NativePurgeVectorQuote {
    pub(crate) pages_retained: u64,
    pub(crate) pages_growth_peak: u64,
    pub(crate) recursive_paths_retained: u64,
    pub(crate) recursive_paths_peak: u64,
    pub(crate) pages_and_freed_growth_peak: u64,
}
fn sum(a: u64, b: u64) -> BoundResult<u64> {
    a.checked_add(b).ok_or(ContainerBoundError::Overflow)
}
fn next_capacity(capacity: usize, required: usize) -> BoundResult<usize> {
    // SAME pinned RawVec::grow_amortized profile as vector_amortized_growth;
    // PageNumber is native non-ZST, min_non_zero_cap is derived from its size.
    let minimum = if size_of::<PageNumber>() == 1 {
        8
    } else if size_of::<PageNumber>() <= 1024 {
        4
    } else {
        1
    };
    Ok(capacity
        .checked_mul(2)
        .ok_or(ContainerBoundError::Overflow)?
        .max(required)
        .max(minimum))
}
fn pushes(mut cap: usize, mut len: usize, additional: usize) -> BoundResult<(u64, u64)> {
    if len > cap {
        return Err(ContainerBoundError::InvalidLayout);
    }
    let target = len
        .checked_add(additional)
        .ok_or(ContainerBoundError::Overflow)?;
    let mut peak = vector_storage::<PageNumber>(cap)?.allocator_bytes;
    // Jump only over pushes that cannot allocate. At most usize::BITS growths.
    while target > cap {
        len = cap;
        let growth = vector_amortized_growth::<PageNumber>(cap, len, 1)?;
        peak = peak.max(growth.simultaneous_peak_bytes);
        cap = next_capacity(
            cap,
            len.checked_add(1).ok_or(ContainerBoundError::Overflow)?,
        )?;
    }
    Ok((vector_storage::<PageNumber>(cap)?.allocator_bytes, peak))
}
// Numeric inputs MUST come from the eventual exact native recipe/observed owner.
// This helper grants no allocation permission and cannot certify that provenance.
fn quote_vectors(
    pages: usize,
    depth: usize,
    freed_capacity_before_purge: usize,
    freed_len_before_purge: usize,
) -> BoundResult<NativePurgeVectorQuote> {
    if depth > 64 || (pages == 0) != (depth == 0) || depth > pages {
        return Err(ContainerBoundError::InvalidLayout);
    }
    let (pages_retained, pages_growth_peak) = pushes(0, 0, pages)?;
    let mut paths = 0;
    let mut paths_peak = 0;
    if depth != 0 {
        // PagePath::new_root uses vec![one]: exact one-element allocation.
        paths = vector_storage::<PageNumber>(1)?.allocator_bytes;
        paths_peak = paths;
        for child_depth in 2..=depth {
            // Vec::clone copies exactly parent length, not spare capacity. All
            // ancestor path Vecs remain live while the cloned child grows.
            let cloned = child_depth - 1;
            let growth = vector_amortized_growth::<PageNumber>(cloned, cloned, 1)?;
            paths_peak = paths_peak.max(sum(paths, growth.simultaneous_peak_bytes)?);
            paths = sum(paths, growth.retained_after_bytes)?;
        }
    }
    let (_, freed_peak) = pushes(freed_capacity_before_purge, freed_len_before_purge, pages)?;
    Ok(NativePurgeVectorQuote {
        pages_retained,
        pages_growth_peak,
        recursive_paths_retained: paths,
        recursive_paths_peak: paths_peak,
        // IntoIter retains the entire pages allocation as freed_pages grows.
        pages_and_freed_growth_peak: sum(pages_retained, freed_peak)?,
    })
}

// Keep the private layout error inside its owning module; callers receive the
// existing closed numerical native refusal family, never an admission token.
pub(crate) fn native_purge_vector_quote(
    pages: usize,
    depth: usize,
    freed_capacity_before_purge: usize,
    freed_len_before_purge: usize,
) -> core::result::Result<NativePurgeVectorQuote, crate::tree_store::NativeKeyPathError> {
    use crate::tree_store::NativeKeyPathError as E;
    quote_vectors(
        pages,
        depth,
        freed_capacity_before_purge,
        freed_len_before_purge,
    )
    .map_err(|error| match error {
        ContainerBoundError::Overflow => E::Overflow,
        ContainerBoundError::InvalidLayout => E::Malformed,
        ContainerBoundError::UnsupportedTarget
        | ContainerBoundError::UnsupportedBuild
        | ContainerBoundError::UnsupportedElementLayout => E::UnsupportedState,
    })
}
