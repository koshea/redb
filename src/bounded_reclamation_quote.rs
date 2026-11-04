//! Numerical component only. The owning registration is cfg(test): production
//! construction awaits the complete native preallocation/resource owner.
use super::{
    DATA_ALLOCATED_TABLE, DATA_FREED_TABLE, InternalDurability, PageList, PostCommitFree,
    SAVEPOINT_TABLE, SYSTEM_FREED_TABLE, TransactionId, WriteTransaction,
};
use crate::StorageError;
use crate::db::ConcurrencyMode;
use crate::tree_store::{
    Btree, BtreeHeader, InternalTableDefinition, PageHint, PageResolver, RawBtree,
};
use core::sync::atomic::Ordering;

#[derive(Debug)]
enum Refusal {
    Unsupported,
    Overflow,
    Stale,
    Native(StorageError),
}
impl From<StorageError> for Refusal {
    fn from(value: StorageError) -> Self {
        Self::Native(value)
    }
}
type QuoteResult<T> = core::result::Result<T, Refusal>;

#[derive(Clone, Copy)]
struct NormalOperations {
    name: &'static str,
    puts: usize,
    deletes: usize,
    max_key_bytes: usize,
    max_value_bytes: usize,
}
struct CompiledEnvelope<const N: usize> {
    tables: [NormalOperations; N],
}

#[derive(Clone, Copy, Debug)]
struct Limits {
    retired_data_pages: usize,
    produced_data_records: usize,
    removed: usize,
    examined: usize,
    page_numbers: usize,
    encoded_bytes: usize,
    system_tail_records: usize,
    steady_record_stock: usize,
}

// Actual held native write owner/root, never independently supplied scalars.
// The TEST factory below traverses native stats/support under that real owner;
// its traversal is not a bounded production census or an admission mechanism.
struct SupportedView<'txn, const N: usize> {
    actual: &'txn WriteTransaction,
    user_root: Option<BtreeHeader>,
    system_root: Option<BtreeHeader>,
    predecessor: TransactionId,
    table_heights: [usize; N],
    table_names: [&'static str; N],
    namespace_height: usize,
    data_freed_height: usize,
    system_freed_height: usize,
    system_namespace_height: usize,
    adopted_pages: usize,
    adopted_weighted_pages: u64,
    adopted_capacity_slots: usize,
    prior_tracker_ids: usize,
}
const INSTALL_DERIVED_BUDGET: bool = true;

struct Quote<'txn, const N: usize> {
    view: SupportedView<'txn, N>,
    limits: Limits,
}

fn add(a: usize, b: usize) -> QuoteResult<usize> {
    a.checked_add(b).ok_or(Refusal::Overflow)
}
fn mul(a: usize, b: usize) -> QuoteResult<usize> {
    a.checked_mul(b).ok_or(Refusal::Overflow)
}
fn ceil(value: usize, divisor: usize) -> QuoteResult<usize> {
    add(value / divisor, usize::from(value % divisor != 0))
}

// No allocation, cloning, native read, callback or detached support factory.
fn derive<const N: usize>(
    view: &SupportedView<'_, N>,
    envelope: &CompiledEnvelope<N>,
) -> QuoteResult<Limits> {
    let mut retired = 0;
    let mut changed_tables = 0;
    let mut operations_total = 0;
    for (index, operations) in envelope.tables.iter().enumerate() {
        if view.table_names[index] != operations.name
            || envelope.tables[..index]
                .iter()
                .any(|prior| prior.name == operations.name)
        {
            return Err(Refusal::Unsupported);
        }
        if operations.max_value_bytes > crate::tree_store::MAX_VALUE_LENGTH
            || add(operations.max_key_bytes, operations.max_value_bytes)?
                > crate::tree_store::MAX_PAIR_LENGTH
        {
            return Err(Refusal::Unsupported);
        }
        let count = add(operations.puts, operations.deletes)?;
        operations_total = add(operations_total, count)?;
        if count != 0 {
            changed_tables = add(changed_tables, 1)?;
            retired = add(retired, mul(mul(2, count)?, view.table_heights[index])?)?;
        }
    }
    retired = add(retired, mul(changed_tables, view.namespace_height)?)?;
    let produced = ceil(retired, 400)?;
    let removed = add(produced, 1)?;
    let examined = add(mul(5, removed)?, 2)?;
    // The current DATA batch can grow its tree before the epilogue. Each
    // single insert raises height by at most one; three native automatic
    // table definitions can be created lazily in the system namespace.
    let data_height_after_main = add(view.data_freed_height, produced)?;
    let namespace_height_after_main = add(view.system_namespace_height, 3)?;
    let system_pages = add(
        add(
            mul(mul(2, removed)?, data_height_after_main)?,
            namespace_height_after_main,
        )?,
        view.system_freed_height,
    )?;
    let tail = ceil(system_pages, 200)?;
    // Current-view tail bounds need not be stationary. This finite normal-
    // shape class caps actual heights at64; derive its global stock ceiling
    // from the same fixed compiled operation envelope, never observed maxima.
    let global_produced = ceil(
        add(mul(128, operations_total)?, mul(64, changed_tables)?)?,
        400,
    )?;
    let global_k = add(global_produced, 1)?;
    // The prospective DATA tree includes all current producer records, and
    // the system namespace can create three automatic table definitions.
    // These are the same growth terms as the current-view bound above.
    let global_data_height_after_main = add(64, global_produced)?;
    let global_system_pages = add(
        add(
            mul(mul(2, global_k)?, global_data_height_after_main)?,
            add(64, 3)?,
        )?,
        64,
    )?;
    let global_tail = ceil(global_system_pages, 200)?;
    Ok(Limits {
        retired_data_pages: retired,
        produced_data_records: produced,
        removed,
        examined,
        page_numbers: mul(400, removed)?,
        encoded_bytes: mul(16 + PageList::required_bytes(400), examined)?,
        system_tail_records: tail,
        steady_record_stock: add(global_produced, mul(2, global_tail)?)?,
    })
}

fn namespace(
    actual: &WriteTransaction,
    root: Option<BtreeHeader>,
) -> QuoteResult<Btree<&'static str, InternalTableDefinition>> {
    Ok(Btree::new(
        root,
        PageHint::None,
        actual.transaction_guard.clone(),
        PageResolver::new(actual.mem.clone()),
    )?)
}
fn checked_height(height: u32) -> QuoteResult<usize> {
    if height > 64 {
        return Err(Refusal::Unsupported);
    }
    usize::try_from(height).map_err(|_| Refusal::Overflow)
}

impl<'txn, const N: usize> SupportedView<'txn, N> {
    fn inspect_for_test(
        actual: &'txn WriteTransaction,
        envelope: &CompiledEnvelope<N>,
    ) -> QuoteResult<Self> {
        if actual.mem.concurrency_mode() != ConcurrencyMode::ExclusiveWriter
            || actual.durability != InternalDurability::Immediate
            || actual.quick_repair
            || actual.post_commit_free != PostCommitFree::Enabled
            || actual.restored_transaction.is_some()
            || actual.dirty.load(Ordering::Acquire)
        {
            return Err(Refusal::Unsupported);
        }
        let tracker = actual
            .transaction_tracker
            .reclamation_quote_snapshot_for_test();
        if tracker.savepoints != 0 || tracker.persistent_savepoints != 0 || tracker.pending > 1 {
            return Err(Refusal::Unsupported);
        }
        let pending = actual.mem.reclamation_quote_snapshot_for_test();
        if !pending.post_only || pending.pages != pending.post_pages {
            return Err(Refusal::Unsupported);
        }
        let predecessor = actual.mem.get_last_committed_transaction_id()?;
        match (tracker.pending, tracker.first_pending) {
            (0, None) if pending.pages == 0 => {}
            (1, Some((id, ancestor)))
                if id == predecessor
                    && ancestor == actual.mem.get_last_durable_transaction_id()?
                    && ancestor.raw_id().checked_add(1) == Some(id.raw_id()) => {}
            _ => return Err(Refusal::Unsupported),
        }
        let user_root = actual.mem.get_data_root();
        let system_root = actual.mem.get_system_root();
        let user = namespace(actual, user_root)?;
        let namespace_height = checked_height(user.stats()?.tree_height)?;
        let mut table_heights = [0; N];
        for (index, operations) in envelope.tables.iter().enumerate() {
            let entry = user.get(&operations.name)?.ok_or(Refusal::Unsupported)?;
            let InternalTableDefinition::Normal {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } = entry.value()
            else {
                return Err(Refusal::Unsupported);
            };
            table_heights[index] = checked_height(
                RawBtree::new(
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    PageResolver::new(actual.mem.clone()),
                    PageHint::None,
                )
                .stats()?
                .tree_height,
            )?;
        }
        let system_namespace_height =
            checked_height(namespace(actual, system_root)?.stats()?.tree_height)?;
        let (data_freed_height, system_freed_height) = {
            let mut tables = actual.system_tables.lock().unwrap();
            if tables
                .get_system_table_root(DATA_ALLOCATED_TABLE)?
                .is_some()
                || tables.get_system_table_root(SAVEPOINT_TABLE)?.is_some()
            {
                return Err(Refusal::Unsupported);
            }
            let mut heights = [0; 2];
            for (index, definition) in [DATA_FREED_TABLE, SYSTEM_FREED_TABLE]
                .into_iter()
                .enumerate()
            {
                let root = tables.get_system_table_root(definition)?;
                heights[index] = checked_height(
                    RawBtree::new(
                        root,
                        Some(16),
                        None,
                        PageResolver::new(actual.mem.clone()),
                        PageHint::None,
                    )
                    .stats()?
                    .tree_height,
                )?;
                if root.is_none() {
                    continue;
                }
                let table = tables.open_system_table(definition)?;
                for row in table.range::<super::TransactionIdWithPagination>(..)? {
                    let (_, pages) = row?;
                    super::bounded_reclamation::ReclamationBudget::inspect_entry_for_quote_test(
                        &pages.value(),
                    )?;
                }
            }
            (heights[0], heights[1])
        };
        Ok(Self {
            actual,
            user_root,
            system_root,
            predecessor,
            table_heights,
            table_names: envelope.tables.map(|operations| operations.name),
            namespace_height,
            data_freed_height,
            system_freed_height,
            system_namespace_height,
            adopted_pages: pending.pages,
            adopted_weighted_pages: pending.weighted_pages.ok_or(Refusal::Overflow)?,
            adopted_capacity_slots: add(
                add(pending.pages_capacity, pending.post_capacity)?,
                pending.allocation_index_capacity,
            )?,
            prior_tracker_ids: tracker.unprocessed,
        })
    }
    fn quote(self, envelope: &CompiledEnvelope<N>) -> QuoteResult<Quote<'txn, N>> {
        let limits = derive(&self, envelope)?;
        Ok(Quote { view: self, limits })
    }
}
impl<const N: usize> Quote<'_, N> {
    fn install_for_test(self) -> QuoteResult<Limits> {
        let actual = self.view.actual;
        if actual.dirty.load(Ordering::Acquire)
            || actual.mem.get_data_root() != self.view.user_root
            || actual.mem.get_system_root() != self.view.system_root
            || actual.mem.get_last_committed_transaction_id()? != self.view.predecessor
        {
            return Err(Refusal::Stale);
        }
        let mut current = actual.reclamation.lock().unwrap();
        if current.is_some() {
            return Err(Refusal::Stale);
        }
        if INSTALL_DERIVED_BUDGET {
            *current = Some(
                super::bounded_reclamation::ReclamationBudget::for_emitted_entries(
                    self.limits.removed,
                ),
            );
        }
        Ok(self.limits)
    }
}

#[path = "bounded_reclamation_quote_tests.rs"]
mod tests;
