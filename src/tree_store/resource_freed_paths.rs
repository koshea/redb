//! Exact bounded native freed-record successor reads under a genuine read pin.
//! No allocation/admission factory, mutation authority or complete write quote.
use super::resource_key_paths::{
    CompiledPathVisits, NativeKeyPathCost, NativeKeyPathError, SUPPORTED_PATH_DEPTH, check_layout,
};
use super::{
    BRANCH, BranchAccessor, Btree, Checksum, DEFERRED, LEAF, LeafAccessor, Page, PageImpl,
    PageNumber, branch_checksum, leaf_checksum,
};
use crate::Value;
use crate::transaction_tracker::NativeResourceEpoch;
use crate::transactions::{PageList, TransactionIdWithPagination};
use core::ops::Range;

type PathResult<T> = core::result::Result<T, NativeKeyPathError>;

// A checked native leaf entry, still physically owned and read-pinned. Only
// the closed native observation family receives its PageList borrow.
pub(crate) struct NativeFreedEntryRead<'tree> {
    actual: &'tree Btree<TransactionIdWithPagination, PageList<'static>>,
    page: PageImpl,
    value_range: Range<usize>,
    key: TransactionIdWithPagination,
    cost: NativeKeyPathCost,
}
impl NativeFreedEntryRead<'_> {
    pub(crate) fn key(&self) -> &TransactionIdWithPagination {
        &self.key
    }
    pub(crate) fn page_list(&self) -> PageList<'_> {
        PageList::from_bytes(&self.page.memory()[self.value_range.clone()])
    }
    pub(crate) fn cost(&self) -> NativeKeyPathCost {
        self.cost
    }
    pub(crate) fn require_current(&self) -> PathResult<()> {
        self.actual.require_current_native_resource_read()
    }
}
impl Drop for NativeFreedEntryRead<'_> {
    fn drop(&mut self) {}
}

#[derive(Clone, Copy)]
struct Sibling {
    page: PageNumber,
    checksum: Checksum,
}

impl Btree<TransactionIdWithPagination, PageList<'static>> {
    // Returns the exact least native key strictly after `after`. The captured
    // sibling stack is fixed, owns no page aliases and does not infer a global
    // tree height. The returned entry keeps its actual page/read owner alive.
    pub(crate) fn read_native_freed_after<'tree>(
        &'tree self,
        after: Option<&TransactionIdWithPagination>,
        epoch: &NativeResourceEpoch<'_>,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<(Option<NativeFreedEntryRead<'tree>>, NativeKeyPathCost)> {
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
        let result = self.read_native_freed_inner(after, false, visits)?;
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(tracker)
            .map_err(NativeKeyPathError::Epoch)?;
        Ok(result)
    }

    // Exact greatest native key strictly before `before`; shares the same
    // bounded parser/checksum/sibling stack as the existing successor read.
    pub(crate) fn read_native_freed_before<'tree>(
        &'tree self,
        before: &TransactionIdWithPagination,
        epoch: &NativeResourceEpoch<'_>,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<(Option<NativeFreedEntryRead<'tree>>, NativeKeyPathCost)> {
        let parts = self
            .transaction_guard
            .checked_pinned_read_parts()
            .map_err(NativeKeyPathError::Epoch)?
            .ok_or(NativeKeyPathError::Unpinned)?;
        epoch
            .require_tracker(parts.actual_tracker())
            .map_err(NativeKeyPathError::Epoch)?;
        self.require_current_native_resource_read()?;
        let result = self.read_native_freed_inner(Some(before), true, visits)?;
        self.require_current_native_resource_read()?;
        epoch
            .require_tracker(parts.actual_tracker())
            .map_err(NativeKeyPathError::Epoch)?;
        Ok(result)
    }

    fn read_native_freed_inner<'tree>(
        &'tree self,
        after: Option<&TransactionIdWithPagination>,
        reverse: bool,
        visits: &mut CompiledPathVisits,
    ) -> PathResult<(Option<NativeFreedEntryRead<'tree>>, NativeKeyPathCost)> {
        let mut cost = NativeKeyPathCost {
            depth: 0,
            observed_page_bytes: 0,
            observed_base_pages: 0,
        };
        let Some(root) = self.root else {
            return Ok((None, cost));
        };
        let query = after.map(TransactionIdWithPagination::as_bytes);
        let mut seeking = after.is_some();
        let mut next = root.root;
        let mut checksum = root.checksum;
        let mut depth = 0;
        let mut seen = [None::<PageNumber>; SUPPORTED_PATH_DEPTH];
        let mut siblings = [None::<Sibling>; SUPPORTED_PATH_DEPTH];
        loop {
            if depth >= SUPPORTED_PATH_DEPTH {
                return Err(NativeKeyPathError::UnsupportedDepth);
            }
            if checksum == DEFERRED {
                return Err(NativeKeyPathError::UnsupportedState);
            }
            if seen[..depth].contains(&Some(next)) {
                return Err(NativeKeyPathError::Malformed);
            }
            visits.visit()?;
            seen[depth] = Some(next);
            let page = self.mem.get_page(next, self.hint)?;
            check_layout::<TransactionIdWithPagination, PageList<'static>>(&page)?;
            cost.depth = cost.depth.max(depth + 1);
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
                BRANCH => {
                    if branch_checksum(&page, TransactionIdWithPagination::fixed_width())?
                        != checksum
                    {
                        return Err(NativeKeyPathError::Malformed);
                    }
                    let branch =
                        BranchAccessor::new(&page, TransactionIdWithPagination::fixed_width());
                    let index = if seeking {
                        branch
                            .child_for_key::<TransactionIdWithPagination>(
                                query.as_ref().ok_or(NativeKeyPathError::Malformed)?,
                            )
                            .0
                    } else if reverse {
                        branch
                            .count_children()
                            .checked_sub(1)
                            .ok_or(NativeKeyPathError::Malformed)?
                    } else {
                        0
                    };
                    let sibling_index = if reverse {
                        index.checked_sub(1)
                    } else if index + 1 < branch.count_children() {
                        Some(index + 1)
                    } else {
                        None
                    };
                    siblings[depth] = sibling_index
                        .map(|index| -> PathResult<Sibling> {
                            Ok(Sibling {
                                page: branch
                                    .child_page(index)
                                    .ok_or(NativeKeyPathError::Malformed)?,
                                checksum: branch
                                    .child_checksum(index)
                                    .ok_or(NativeKeyPathError::Malformed)?,
                            })
                        })
                        .transpose()?;
                    next = branch
                        .child_page(index)
                        .ok_or(NativeKeyPathError::Malformed)?;
                    checksum = branch
                        .child_checksum(index)
                        .ok_or(NativeKeyPathError::Malformed)?;
                    depth += 1;
                }
                LEAF => {
                    if leaf_checksum(&page, TransactionIdWithPagination::fixed_width(), None)?
                        != checksum
                    {
                        return Err(NativeKeyPathError::Malformed);
                    }
                    let leaf = LeafAccessor::new(
                        page.memory(),
                        TransactionIdWithPagination::fixed_width(),
                        None,
                    );
                    let index = if seeking {
                        let (index, equal) = leaf.position::<TransactionIdWithPagination>(
                            query.as_ref().ok_or(NativeKeyPathError::Malformed)?,
                        );
                        if reverse {
                            index.checked_sub(1)
                        } else {
                            Some(
                                index
                                    .checked_add(usize::from(equal))
                                    .ok_or(NativeKeyPathError::Overflow)?,
                            )
                        }
                    } else if reverse {
                        leaf.num_pairs().checked_sub(1)
                    } else {
                        Some(0)
                    };
                    if let Some(index) = index.filter(|index| *index < leaf.num_pairs()) {
                        let (key_range, value_range) = leaf
                            .entry_ranges(index)
                            .ok_or(NativeKeyPathError::Malformed)?;
                        let key =
                            TransactionIdWithPagination::from_bytes(&page.memory()[key_range]);
                        if after.is_some_and(|old| {
                            let observed = (key.transaction_id, key.pagination_id);
                            let bound = (old.transaction_id, old.pagination_id);
                            if reverse {
                                observed >= bound
                            } else {
                                observed <= bound
                            }
                        }) {
                            return Err(NativeKeyPathError::Malformed);
                        }
                        return Ok((
                            Some(NativeFreedEntryRead {
                                actual: self,
                                page,
                                value_range,
                                key,
                                cost,
                            }),
                            cost,
                        ));
                    }
                    // No qualifying row in this leaf: choose the nearest
                    // captured sibling in the selected direction, with no
                    // retained/re-read parent page or native range allocation.
                    let mut successor = None;
                    while depth != 0 {
                        depth -= 1;
                        if let Some(sibling) = siblings[depth].take() {
                            successor = Some(sibling);
                            depth += 1;
                            break;
                        }
                    }
                    let Some(sibling) = successor else {
                        return Ok((None, cost));
                    };
                    seen[depth..].fill(None);
                    siblings[depth..].fill(None);
                    next = sibling.page;
                    checksum = sibling.checksum;
                    seeking = false;
                }
                _ => return Err(NativeKeyPathError::Malformed),
            }
        }
    }
}
