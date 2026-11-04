//! Closed readonly file handoff and numerical resource plan.
//!
//! The plan owns native file exclusion, not semantic validation or Source
//! authority. First-party callers retain their already-admitted numerical
//! reservation until this facade and every exclusively borrowed child drop.
#[path = "read_only_cold_prepaid.rs"]
mod cold_prepaid;
use super::read_only_reopen_custody::{
    ReadOnlyFileBinding, ReadOnlyReopenBackend, ReadOnlyReopenFileCustody,
};
use super::{
    Builder, ConcurrencyMode, ReadOnlyBackend, ReadOnlyDatabase, ReadableDatabase, TransactionGuard,
};
use crate::transaction_tracker::TransactionTracker;
use crate::tree_store::{
    AllocatorResourceError, DB_HEADER_SIZE, NativePageResources, ReadOnlyResourceProfile,
    ScopedBorrow, TransactionalMemory, readonly_cursor_capacity_bytes,
    readonly_lock_entry_capacity_bytes,
};
use crate::{
    DatabaseError, OwnedAccessGuard, OwnedRange, ReadOnlyTable, ReadTransaction, StorageError,
    TableDefinition, TableError, TransactionError,
};
use alloc::boxed::Box;
use alloc::sync::Arc;
use core::fmt;
use core::mem::size_of;
use core::ops::Bound;
use std::fs::File;

/// A typed refusal or an unchanged native error from the closed readonly path.
#[derive(Debug)]
pub enum ReadOnlyReopenError {
    /// This native profile cannot be bounded by the compiled plan.
    Unsupported,
    /// The actual native owner, root or retained file changed.
    Stale,
    /// Checked resource arithmetic exceeded its representation.
    Overflow,
    /// A permitted read exceeds the previously admitted finite profile.
    Capacity,
    /// Existing native database open/repair error.
    Native(DatabaseError),
    /// Existing native table/type validation error.
    Table(TableError),
    /// Existing native read transaction error.
    Transaction(TransactionError),
    /// Existing native storage error.
    Storage(StorageError),
}
impl fmt::Display for ReadOnlyReopenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unsupported => "unsupported native readonly resource profile",
            Self::Stale => "stale native readonly resource plan",
            Self::Overflow => "native readonly resource overflow",
            Self::Capacity => "native readonly resource capacity",
            Self::Native(_) => "native readonly database error",
            Self::Table(_) => "native readonly table error",
            Self::Transaction(_) => "native readonly transaction error",
            Self::Storage(_) => "native readonly storage error",
        })
    }
}
impl std::error::Error for ReadOnlyReopenError {}
impl From<DatabaseError> for ReadOnlyReopenError {
    fn from(e: DatabaseError) -> Self {
        Self::Native(e)
    }
}
impl From<TableError> for ReadOnlyReopenError {
    fn from(e: TableError) -> Self {
        Self::Table(e)
    }
}
impl From<TransactionError> for ReadOnlyReopenError {
    fn from(e: TransactionError) -> Self {
        Self::Transaction(e)
    }
}
impl From<StorageError> for ReadOnlyReopenError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}
impl From<AllocatorResourceError> for ReadOnlyReopenError {
    fn from(e: AllocatorResourceError) -> Self {
        match e {
            AllocatorResourceError::Overflow => Self::Overflow,
            AllocatorResourceError::Stale => Self::Stale,
            _ => Self::Unsupported,
        }
    }
}
type ReopenResult<T> = core::result::Result<T, ReadOnlyReopenError>;
const HEADER_BYTES: usize = DB_HEADER_SIZE;
// Existing first-party source key/control ceiling, not a new application knob.
pub(crate) const KEY_BYTES: usize = 4096;
fn sum(a: u64, b: u64) -> ReopenResult<u64> {
    a.checked_add(b).ok_or(ReadOnlyReopenError::Overflow)
}
fn count(a: u64, b: usize) -> ReopenResult<u64> {
    a.checked_mul(u64::try_from(b).map_err(|_| ReadOnlyReopenError::Overflow)?)
        .ok_or(ReadOnlyReopenError::Overflow)
}

/// Native-owned immutable numerical plan. Fields cannot be caller fabricated.
pub struct ReadOnlyReopenResourcePlan {
    custody: ReadOnlyReopenFileCustody,
    binding: ReadOnlyFileBinding,
    header: [u8; HEADER_BYTES],
    original_owner: usize,
    page_size: usize,
    profile: ReadOnlyResourceProfile,
    peak: u64,
    retained: u64,
}
impl ReadOnlyReopenResourcePlan {
    /// Complete conservative native construction/read peak to admit beforehand.
    pub fn peak_bytes(&self) -> u64 {
        self.peak
    }
    /// Retained native envelope; callers may conservatively retain the full peak.
    pub fn retained_bytes(&self) -> u64 {
        self.retained
    }
    fn verify_exclusive_handoff(&self) -> ReopenResult<()> {
        if !self
            .custody
            .is_exclusive_handoff_owner()
            .map_err(StorageError::from)?
        {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        self.verify_file()
    }
    fn verify_file(&self) -> ReopenResult<()> {
        if self
            .custody
            .actual_file_binding()
            .map_err(StorageError::from)?
            != self.binding
        {
            return Err(ReadOnlyReopenError::Stale);
        }
        let mut header = [0; HEADER_BYTES];
        self.custody
            .read_header(&mut header)
            .map_err(StorageError::from)?;
        if header != self.header {
            return Err(ReadOnlyReopenError::Stale);
        }
        Ok(())
    }
}
/// File-only handoff after the old native engine, caches and page aliases drain.
pub struct ReadOnlyReopenHandoff {
    plan: ReadOnlyReopenResourcePlan,
}
impl ReadOnlyReopenHandoff {
    /// Peak which must already be admitted before opening the planned owner.
    pub fn peak_bytes(&self) -> u64 {
        self.plan.peak_bytes()
    }
    /// Retained native envelope.
    pub fn retained_bytes(&self) -> u64 {
        self.plan.retained_bytes()
    }
}
impl ReadOnlyDatabase {
    /// Capture native resource facts from this actual maintenance file owner.
    /// This is not a full corruption scrub or application Source authorization.
    pub fn plan_read_only_reopen(&self) -> ReopenResult<ReadOnlyReopenResourcePlan> {
        if cfg!(feature = "logging") {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let custody = self
            .reopen_custody
            .as_ref()
            .ok_or(ReadOnlyReopenError::Unsupported)?;
        let component = self.mem.allocator_reopen_component()?;
        component.verify_for(&self.mem)?;
        let profile = self.mem.readonly_resource_profile()?;
        let binding = custody.actual_file_binding().map_err(StorageError::from)?;
        let mut header = [0; HEADER_BYTES];
        custody
            .read_header(&mut header)
            .map_err(StorageError::from)?;
        // The normal sole parser has already checked the clean exact header.
        // Noncanonical/reserved-byte variants are conservatively unsupported.
        if &header != component.native_header_binding()
            || binding != custody.actual_file_binding().map_err(StorageError::from)?
        {
            return Err(ReadOnlyReopenError::Stale);
        }
        let page_size = self.mem.get_page_size();
        let (peak, retained) = quote(custody, profile)?;
        let plan = ReadOnlyReopenResourcePlan {
            custody: custody.clone_file_custody(),
            binding,
            header,
            original_owner: Arc::as_ptr(&self.mem) as usize,
            page_size,
            profile,
            peak,
            retained,
        };
        component.verify_for(&self.mem)?;
        plan.verify_file()?;
        Ok(plan)
    }
    fn validate_read_only_reopen_handoff(
        &self,
        plan: &ReadOnlyReopenResourcePlan,
    ) -> ReopenResult<()> {
        let custody = self
            .reopen_custody
            .as_ref()
            .ok_or(ReadOnlyReopenError::Unsupported)?;
        if !custody.same_owner(&plan.custody)
            || Arc::as_ptr(&self.mem) as usize != plan.original_owner
        {
            return Err(ReadOnlyReopenError::Stale);
        }
        self.mem
            .allocator_reopen_component()?
            .verify_for(&self.mem)?;
        plan.verify_file()?;
        if Arc::strong_count(&self.mem) != 1 || Arc::strong_count(&self.transaction_tracker) != 1 {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        Ok(())
    }
    /// Consume the exact planned maintenance owner while keeping native file
    /// exclusion. Any surviving transaction/table/page alias refuses handoff.
    pub fn into_read_only_reopen_handoff(
        self,
        plan: ReadOnlyReopenResourcePlan,
    ) -> ReopenResult<ReadOnlyReopenHandoff> {
        self.validate_read_only_reopen_handoff(&plan)?;
        let aliases = self.mem.readonly_alias_drain();
        let Self {
            mem,
            transaction_tracker,
            reopen_custody,
            page_resources,
        } = self;
        let witness = page_resources.ok_or(ReadOnlyReopenError::Unsupported)?;
        // No native engine tasks exist; these synchronous destructors join the
        // actual caches, allocator graph, page/control owners and engine lease.
        drop(transaction_tracker);
        drop(mem);
        witness.try_into_drained()?;
        aliases.try_into_drained()?;
        drop(reopen_custody);
        plan.verify_exclusive_handoff()?;
        Ok(ReadOnlyReopenHandoff { plan })
    }
}
impl Builder {
    /// Open the closed native maintenance owner whose file exclusion can be
    /// retained through complete memory teardown. Ordinary open is unchanged.
    pub fn open_read_only_maintenance_file(&self, file: File) -> ReopenResult<ReadOnlyDatabase> {
        if self.concurrency_mode != ConcurrencyMode::ExclusiveWriter {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let custody = ReadOnlyReopenFileCustody::acquire(file)?;
        let resources = NativePageResources::numerical(usize::MAX);
        let mut db = ReadOnlyDatabase::new_read_resources(
            Box::new(custody.lease().map_err(StorageError::from)?),
            self.page_size,
            self.region_size,
            self.cache_size,
            ConcurrencyMode::ExclusiveWriter,
            Some(resources),
            None,
        )?;
        db.reopen_custody = Some(custody);
        Ok(db)
    }
    /// Reopen under the native-owned finite profile after external numerical
    /// admission. Builder cache settings cannot enlarge this compiled profile.
    pub fn open_read_only_planned_handoff(
        &self,
        handoff: ReadOnlyReopenHandoff,
    ) -> ReopenResult<PlannedReadOnlyDatabase> {
        let ReadOnlyReopenHandoff { plan } = handoff;
        plan.verify_file()?;
        if !plan
            .custody
            .is_exclusive_handoff_owner()
            .map_err(StorageError::from)?
        {
            return Err(ReadOnlyReopenError::Unsupported);
        }
        let resources = NativePageResources::numerical(plan.profile.page_bytes());
        let mut db = ReadOnlyDatabase::new_read_resources(
            Box::new(plan.custody.lease().map_err(StorageError::from)?),
            plan.page_size,
            None,
            0,
            ConcurrencyMode::ExclusiveWriter,
            Some(resources),
            Some(plan.profile),
        )?;
        // Same immutable artifact + pinned native build/config uses the same
        // sole parser schedule, whose complete PRE-resize peak was admitted.
        // This comparison rejects drift; it is not post-allocation admission.
        db.mem.readonly_policy_is_empty()?;
        if db.mem.allocator_reopen_component()?.facts() != plan.profile.allocator_facts() {
            return Err(ReadOnlyReopenError::Stale);
        }
        plan.verify_file()?;
        db.reopen_custody = Some(plan.custody.clone_file_custody());
        Ok(PlannedReadOnlyDatabase { inner: db, plan })
    }
}
fn quote(
    custody: &ReadOnlyReopenFileCustody,
    profile: ReadOnlyResourceProfile,
) -> ReopenResult<(u64, u64)> {
    let mut fixed = custody.control_capacity_bytes()?;
    for value in [
        readonly_lock_entry_capacity_bytes()?,
        ReadOnlyResourceProfile::typed_allocation::<ReadOnlyReopenBackend>()?,
        ReadOnlyResourceProfile::typed_allocation::<ReadOnlyBackend>()?,
        TransactionalMemory::readonly_fixed_capacity_bytes(profile)?,
        TransactionTracker::readonly_capacity_bytes()?,
        NativePageResources::control_capacity_bytes()?,
        count(
            ReadOnlyResourceProfile::arc_allocation::<TransactionGuard>()?,
            3,
        )?,
        readonly_cursor_capacity_bytes()?,
        ReadOnlyResourceProfile::allocation(KEY_BYTES, 1)?,
        ReadOnlyResourceProfile::allocation(HEADER_BYTES, 1)?,
        ReadOnlyResourceProfile::allocation(9, 1)?,
    ] {
        fixed = sum(fixed, value)?;
    }
    // The two decoded type-name buffers and their two check_match clones,
    // up to three fixed expected type names on an error path, one table name
    // and one bounded native error diagnostic can coexist.
    fixed = sum(
        fixed,
        count(
            ReadOnlyResourceProfile::allocation(profile.metadata_bytes(), 1)?,
            4,
        )?,
    )?;
    fixed = sum(
        fixed,
        count(ReadOnlyResourceProfile::allocation(KEY_BYTES, 1)?, 2)?,
    )?;
    fixed = sum(
        fixed,
        count(ReadOnlyResourceProfile::allocation(64, 1)?, 3)?,
    )?;
    // Inline plans/facade/transaction/table/cursor/row and one fixed stack
    // header are charged too, despite not requesting heap allocation.
    let inline = size_of::<ReadOnlyReopenResourcePlan>()
        + size_of::<ReadOnlyPostHandoffCustody>()
        + size_of::<ReadOnlyReopenHandoff>()
        + size_of::<PlannedReadOnlyDatabase>()
        + size_of::<PlannedReadTransaction<'static>>()
        + size_of::<PlannedBytesTable<'static>>()
        + size_of::<PlannedStrTable<'static>>()
        + size_of::<PlannedByteRange<'static>>()
        + size_of::<PlannedKeyValue<'static>>()
        + size_of::<PlannedValue<'static>>()
        + HEADER_BYTES
        + 2 * size_of::<TransactionalMemory>()
        + 2 * size_of::<TransactionTracker>()
        + size_of::<ReadOnlyReopenError>();
    fixed = sum(
        fixed,
        u64::try_from(inline).map_err(|_| ReadOnlyReopenError::Overflow)?,
    )?;
    let pages = u64::try_from(profile.page_bytes()).map_err(|_| ReadOnlyReopenError::Overflow)?;
    let retained = sum(sum(fixed, profile.allocator_bound(false)?)?, pages)?;
    let peak = sum(sum(fixed, profile.allocator_bound(true)?)?, pages)?;
    Ok((peak, retained))
}
/// Closed lifetime-only retention of an already opened planned file.
///
/// It exposes no file, backend, read, lock operation or resource-plan authority.
/// Its sole effect is to keep the same native and legacy exclusions alive after
/// a planned owner is destroyed. It cannot be created before the handoff joins.
pub struct ReadOnlyPostHandoffCustody {
    _custody: ReadOnlyReopenFileCustody,
}

/// Closed finite native reader. Ordinary native handles never escape it.
pub struct PlannedReadOnlyDatabase {
    inner: ReadOnlyDatabase,
    plan: ReadOnlyReopenResourcePlan,
}
impl PlannedReadOnlyDatabase {
    /// Retains the actual exclusions only after the checked planned open.
    /// No allocation occurs: this shares the existing closed native file core.
    pub fn retain_post_handoff_custody(&self) -> ReopenResult<ReadOnlyPostHandoffCustody> {
        self.plan.verify_file()?;
        Ok(ReadOnlyPostHandoffCustody {
            _custody: self.plan.custody.clone_file_custody(),
        })
    }

    /// One exclusive read transaction at a time.
    pub fn begin_read(&mut self) -> ReopenResult<PlannedReadTransaction<'_>> {
        self.plan.verify_file()?;
        let inner = self.inner.begin_read()?;
        Ok(PlannedReadTransaction {
            inner,
            _owner: ScopedBorrow::default(),
            _handle_gate: None,
        })
    }
}
// Separate from page/numerical ownership: physical page clones never clone
// this control. A forgotten child blocks the parent's next allocation/advance.
#[derive(Clone)]
pub(crate) struct PlannedHandleGate {
    owner: Option<Arc<()>>,
    expected: usize,
}
impl PlannedHandleGate {
    pub(crate) fn new() -> Self {
        Self {
            owner: Some(Arc::new(())),
            expected: 1,
        }
    }
    pub(crate) fn capacity_bytes() -> ReopenResult<u64> {
        Ok(ReadOnlyResourceProfile::arc_allocation::<()>()?)
    }
    fn check(&self) -> ReopenResult<()> {
        if Arc::strong_count(self.owner.as_ref().unwrap()) != self.expected {
            return Err(ReadOnlyReopenError::Capacity);
        }
        Ok(())
    }
    fn child(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            expected: self.expected + 1,
        }
    }
}
impl Drop for PlannedHandleGate {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            let _ = Arc::into_inner(owner);
        }
    }
}
fn check_handle(gate: &Option<PlannedHandleGate>) -> ReopenResult<()> {
    if let Some(gate) = gate {
        gate.check()?;
    }
    Ok(())
}
fn child_handle(gate: &Option<PlannedHandleGate>) -> Option<PlannedHandleGate> {
    gate.as_ref().map(PlannedHandleGate::child)
}

/// Exclusive read borrow, physically retained through all child destruction.
pub struct PlannedReadTransaction<'owner> {
    inner: ReadTransaction,
    _owner: ScopedBorrow<'owner>,
    _handle_gate: Option<PlannedHandleGate>,
}
impl PlannedReadTransaction<'_> {
    #[cfg(test)]
    pub(crate) fn actual_read_for_system_test(&self) -> &ReadTransaction {
        &self.inner
    }
    #[cfg(test)]
    pub(crate) fn context_current_for_test(&self) -> usize {
        self.inner.tree_context_current_for_test()
    }
    pub(crate) fn from_context(inner: ReadTransaction, handle: PlannedHandleGate) -> Self {
        Self {
            inner,
            _owner: ScopedBorrow::default(),
            _handle_gate: Some(handle),
        }
    }
    /// Open a native byte-key/byte-value table with unchanged type checks.
    pub fn open_bytes_table(&mut self, name: &str) -> ReopenResult<PlannedBytesTable<'_>> {
        check_handle(&self._handle_gate)?;
        check_key(name.as_bytes())?;
        Ok(PlannedBytesTable {
            inner: self
                .inner
                .open_table(TableDefinition::<&[u8], &[u8]>::new(name))?,
            _owner: ScopedBorrow::default(),
            _handle_gate: child_handle(&self._handle_gate),
        })
    }
    /// Open a native string-key/byte-value table with unchanged type checks.
    pub fn open_str_table(&mut self, name: &str) -> ReopenResult<PlannedStrTable<'_>> {
        check_handle(&self._handle_gate)?;
        check_key(name.as_bytes())?;
        Ok(PlannedStrTable {
            inner: self
                .inner
                .open_table(TableDefinition::<&str, &[u8]>::new(name))?,
            _owner: ScopedBorrow::default(),
            _handle_gate: child_handle(&self._handle_gate),
        })
    }
}
fn check_key(key: &[u8]) -> ReopenResult<()> {
    if key.len() > KEY_BYTES {
        Err(ReadOnlyReopenError::Capacity)
    } else {
        Ok(())
    }
}
/// One exclusively borrowed native byte table.
pub struct PlannedBytesTable<'owner> {
    inner: ReadOnlyTable<&'static [u8], &'static [u8]>,
    _owner: ScopedBorrow<'owner>,
    _handle_gate: Option<PlannedHandleGate>,
}
/// One exclusively borrowed native string table.
pub struct PlannedStrTable<'owner> {
    inner: ReadOnlyTable<&'static str, &'static [u8]>,
    _owner: ScopedBorrow<'owner>,
    _handle_gate: Option<PlannedHandleGate>,
}
impl PlannedBytesTable<'_> {
    #[cfg(test)]
    pub(crate) fn context_current_for_test(&self) -> usize {
        self.inner.context_current_for_test()
    }
    /// Borrow one value without copying it or detaching its native page.
    pub fn get(&mut self, key: &[u8]) -> ReopenResult<Option<PlannedValue<'_>>> {
        check_handle(&self._handle_gate)?;
        check_key(key)?;
        Ok(self.inner.get_owned(key)?.map(|inner| PlannedValue {
            inner,
            _owner: ScopedBorrow::default(),
            _handle_gate: child_handle(&self._handle_gate),
        }))
    }
    /// One forward range; no second cursor or reverse iterator can be obtained.
    pub fn range_from(&mut self, start: Option<&[u8]>) -> ReopenResult<PlannedByteRange<'_>> {
        check_handle(&self._handle_gate)?;
        if let Some(key) = start {
            check_key(key)?;
        }
        let lower = start.map_or(Bound::Unbounded, Bound::Included);
        #[cfg(not(feature = "experimental-api-5"))]
        let inner = self.inner.range_owned::<&[u8]>((lower, Bound::Unbounded))?;
        #[cfg(feature = "experimental-api-5")]
        let inner = self.inner.range_owned((lower, Bound::Unbounded))?;
        Ok(PlannedByteRange {
            inner,
            _owner: ScopedBorrow::default(),
            _handle_gate: child_handle(&self._handle_gate),
        })
    }
}
impl PlannedStrTable<'_> {
    /// Borrow one value under the unchanged native string-key comparison.
    pub fn get(&mut self, key: &str) -> ReopenResult<Option<PlannedValue<'_>>> {
        check_handle(&self._handle_gate)?;
        check_key(key.as_bytes())?;
        Ok(self.inner.get_owned(key)?.map(|inner| PlannedValue {
            inner,
            _owner: ScopedBorrow::default(),
            _handle_gate: child_handle(&self._handle_gate),
        }))
    }
}
/// Value bytes remain borrowed from the actual admitted native graph.
pub struct PlannedValue<'owner> {
    inner: OwnedAccessGuard<&'static [u8]>,
    _owner: ScopedBorrow<'owner>,
    _handle_gate: Option<PlannedHandleGate>,
}
impl PlannedValue<'_> {
    /// Scoped canonical stored bytes.
    pub fn value(&self) -> &[u8] {
        self.inner.value()
    }
}
/// Exclusive forward range with no detachable native iterator.
pub struct PlannedByteRange<'owner> {
    inner: OwnedRange<&'static [u8], &'static [u8]>,
    _owner: ScopedBorrow<'owner>,
    _handle_gate: Option<PlannedHandleGate>,
}
impl PlannedByteRange<'_> {
    /// Borrow one row until its physical guard drops.
    pub fn next(&mut self) -> ReopenResult<Option<PlannedKeyValue<'_>>> {
        check_handle(&self._handle_gate)?;
        self.inner
            .next()
            .transpose()
            .map(|row| {
                row.map(|(key, value)| PlannedKeyValue {
                    key,
                    value,
                    _owner: ScopedBorrow::default(),
                    _handle_gate: child_handle(&self._handle_gate),
                })
            })
            .map_err(ReadOnlyReopenError::Storage)
    }
}
/// One scoped key/value pair from the actual native range.
pub struct PlannedKeyValue<'owner> {
    key: OwnedAccessGuard<&'static [u8]>,
    value: OwnedAccessGuard<&'static [u8]>,
    _owner: ScopedBorrow<'owner>,
    _handle_gate: Option<PlannedHandleGate>,
}
impl PlannedKeyValue<'_> {
    /// Scoped native key bytes.
    pub fn key(&self) -> &[u8] {
        self.key.value()
    }
    /// Scoped native value bytes.
    pub fn value(&self) -> &[u8] {
        self.value.value()
    }
}

#[cfg(all(test, not(feature = "logging")))]
#[path = "read_only_reopen_tests.rs"]
mod tests;

#[cfg(all(test, feature = "logging"))]
mod logging_tests {
    use super::*;
    // req: STO-020, PRJ-004
    #[test]
    fn native_readonly_resource_plan_refuses_unbounded_global_logging_profile() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let writer = crate::Database::create(file.path()).unwrap();
        let write = writer.begin_write().unwrap();
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("rows"))
            .unwrap()
            .insert(b"key".as_slice(), b"value".as_slice())
            .unwrap();
        write.commit().unwrap();
        drop(writer);
        let descriptor = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        let owner = Builder::new()
            .open_read_only_maintenance_file(descriptor)
            .unwrap();
        assert!(matches!(
            owner.plan_read_only_reopen(),
            Err(ReadOnlyReopenError::Unsupported)
        ));
    }
}
