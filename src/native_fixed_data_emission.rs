//! Exact DATA emission and original typed append positions for the closed recipe.
//! This is not a physical page peak or complete SYSTEM feedback theorem.
use super::{E, NativeFixedDataCensus, NativeNumericalEpoch, R};
use crate::transactions::{
    NativeDetachedProfiledSystemResources, NativeFixedCommitProfile, PageList,
    TransactionIdWithPagination,
};

// No scalar constructor/extraction. The enclosing owner retains the immutable
// recipe, captured namespace/windows and original native epoch inseparably.
pub(super) struct NativeFixedDataEmission {
    freed_pages: usize,
    append_rows: usize,
    encoded_key_value_bytes: usize,
    data_main: TransactionIdWithPagination,
    system_post: TransactionIdWithPagination,
}
impl NativeFixedDataEmission {
    pub(super) fn capture(
        data: &NativeFixedDataCensus<'_>,
        system: &NativeDetachedProfiledSystemResources<'_>,
        numerical: &NativeNumericalEpoch,
    ) -> R<Self> {
        data.require_fixed_cleanup_pool(numerical)?;
        let freed_pages = data.old_data_page_numbers_upper()?;
        // Committed-only actual deferred DATA pushes are covered by the original
        // table/namespace windows. Successors are uncommitted and freed directly;
        // the closed executor drops each old-value guard before the next point.
        let append_rows = freed_pages.div_ceil(PageList::allocated_capacity_count());
        let key_bytes = <TransactionIdWithPagination as crate::Value>::fixed_width()
            .ok_or(E::UnsupportedState)?;
        let row_bytes = key_bytes
            .checked_add(PageList::allocated_capacity_bytes())
            .ok_or(E::Overflow)?;
        let encoded_key_value_bytes = append_rows.checked_mul(row_bytes).ok_or(E::Overflow)?;
        let append = system.append_positions()?;
        // Copy only the genuine native typed keys, under their actual detached
        // root/read/epoch/serial seal. No caller ID or decoded DTO is adopted.
        let copy_key = |key: &TransactionIdWithPagination| TransactionIdWithPagination {
            transaction_id: key.transaction_id,
            pagination_id: key.pagination_id,
        };
        let data_main = copy_key(append.data_main());
        let system_post = copy_key(append.system_post());
        if data_main.pagination_id != 0
            || data_main.transaction_id.checked_add(1) != Some(system_post.transaction_id)
        {
            return Err(E::Malformed);
        }
        data.require_fixed_cleanup_pool(numerical)?;
        system.require_current()?;
        Ok(Self {
            freed_pages,
            append_rows,
            encoded_key_value_bytes,
            data_main,
            system_post,
        })
    }
    pub(super) fn append_rows_upper(&self) -> usize {
        self.append_rows
    }
    pub(super) fn freed_pages_upper(&self) -> usize {
        self.freed_pages
    }
    pub(super) fn require_for(
        &self,
        data: &NativeFixedDataCensus<'_>,
        system: &NativeDetachedProfiledSystemResources<'_>,
        numerical: &NativeNumericalEpoch,
    ) -> R<()> {
        let actual = Self::capture(data, system, numerical)?;
        if self.freed_pages != actual.freed_pages
            || self.append_rows != actual.append_rows
            || self.encoded_key_value_bytes != actual.encoded_key_value_bytes
            || <TransactionIdWithPagination as crate::Value>::as_bytes(&self.data_main)
                != <TransactionIdWithPagination as crate::Value>::as_bytes(&actual.data_main)
            || <TransactionIdWithPagination as crate::Value>::as_bytes(&self.system_post)
                != <TransactionIdWithPagination as crate::Value>::as_bytes(&actual.system_post)
        {
            return Err(E::ForeignOwner);
        }
        Ok(())
    }
    pub(super) fn require_for_writer(&self, actual: &NativeFixedCommitProfile<'_>) -> R<()> {
        actual.require_append_positions(&self.data_main, &self.system_post)?;
        // Actual Immediate/ExclusiveWriter/quick_repair=false permits ZERO main
        // SYSTEM_FREED appends: that sole call is inside the quick-repair loop.
        // Main SYS COW/freed-vector/local/checksum ownership is still required.
        // Post-only 200-PN padded emission/union/feedback remains unjoined.
        Ok(())
    }
}
