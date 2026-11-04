//! File-only exclusion across destruction of an explicitly owned maintenance reader.
//! Ordinary native open/close paths do not use these engine subleases.
use crate::tree_store::file_backend::FileBackend;
use crate::tree_store::{NativeOwnedShared, NativePageResources};
use crate::{BackendError, DatabaseError, ReadOnlyReopenError, StorageBackend, StorageError};
use std::fs::File;
use std::io;
use std::ops::Bound;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

type NativeRange = (Bound<u64>, Bound<u64>);
const ENGINE_RANGES: usize = 64;

pub(super) struct ReadOnlyReopenFileCustody {
    shared: NativeOwnedShared<ReadOnlyFileCore>,
}

struct ReadOnlyFileCore {
    backend: FileBackend,
    leases: Mutex<EngineLeases>,
    invalid: AtomicBool,
    binding: ReadOnlyFileBinding,
    // Last: an explicit flock release follows the native backend close.
    file: LegacyExclusion,
    #[cfg(test)]
    _retirement: Option<CustodyRetirement>,
}

struct LegacyExclusion(File);
impl Drop for LegacyExclusion {
    fn drop(&mut self) {
        // A fork may retain this open-file description after our descriptors.
        let _ = self.0.unlock();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReadOnlyFileBinding {
    device: u64,
    inode: u64,
    length: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl ReadOnlyFileBinding {
    #[cfg(target_os = "linux")]
    fn observe(file: &File) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "regular file required",
            ));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
    #[cfg(not(target_os = "linux"))]
    fn observe(_: &File) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported readonly custody target",
        ))
    }
}

struct EngineLeases {
    generation: u64,
    active: Option<u64>,
    ranges: [Option<NativeRange>; ENGINE_RANGES],
}
impl EngineLeases {
    fn new() -> Self {
        Self {
            generation: 0,
            active: None,
            ranges: [None; ENGINE_RANGES],
        }
    }
}

impl std::fmt::Debug for ReadOnlyReopenFileCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadOnlyReopenFileCustody([REDACTED])")
    }
}

impl ReadOnlyReopenFileCustody {
    pub(super) fn acquire(file: File) -> Result<Self, DatabaseError> {
        Self::acquire_kernel(
            file,
            None,
            #[cfg(test)]
            None,
        )
    }

    // Successful custody graph only: incoming File/pool bootstrap, later lease
    // Boxes/header/parser/cache and existing owned error work remain unadmitted.
    // This private entry mints file exclusion, never engine/Source authority.
    pub(super) fn prepaid_capacity_bytes() -> Result<usize, ReadOnlyReopenError> {
        let tracker = usize::try_from(FileBackend::fresh_readonly_full_range_capacity_bytes()?)
            .map_err(|_| ReadOnlyReopenError::Overflow)?;
        NativeOwnedShared::<ReadOnlyFileCore>::source_capacity_bytes()?
            .checked_add(tracker)
            .ok_or(ReadOnlyReopenError::Overflow)
    }
    pub(super) fn acquire_prepaid(
        file: File,
        resources: &NativePageResources,
    ) -> Result<Self, ReadOnlyReopenError> {
        Self::acquire_prepaid_observed(
            file,
            resources,
            #[cfg(test)]
            None,
        )
    }
    fn acquire_prepaid_observed(
        file: File,
        resources: &NativePageResources,
        #[cfg(test)] trace: Option<Arc<CustodyConstructionTrace>>,
    ) -> Result<Self, ReadOnlyReopenError> {
        // Map profile/overflow using the existing readonly error vocabulary.
        // No construction happens before this pure prospective quote.
        Self::prepaid_capacity_bytes()?;
        Self::acquire_kernel(
            file,
            Some(resources),
            #[cfg(test)]
            trace,
        )
        .map_err(Into::into)
    }
    fn acquire_kernel(
        file: File,
        resources: Option<&NativePageResources>,
        #[cfg(test)] trace: Option<Arc<CustodyConstructionTrace>>,
    ) -> Result<Self, DatabaseError> {
        let prepared = if let Some(resources) = resources {
            // The private Source entry already checked the typed profile. This
            // is the SAME sole prospective tracker calculator, no caller quote.
            let extra = FileBackend::fresh_readonly_full_range_capacity_bytes()
                .map_err(|_| StorageError::Unsupported)?;
            let extra = usize::try_from(extra).map_err(|_| StorageError::Unsupported)?;
            Some(
                NativeOwnedShared::<ReadOnlyFileCore>::prepare_source_retained_current(
                    resources, extra,
                )?,
            )
        } else {
            None
        };
        let mut parts = CustodyConstruction {
            core: None,
            backend: None,
            legacy: None,
            file: Some(file),
            #[cfg(test)]
            trace,
            prepared,
        };
        // Ordinary and prepaid custody share this original successful grammar
        // and first-failure ordering. Every partial data owner precedes guard.
        parts
            .file
            .as_ref()
            .unwrap()
            .try_lock()
            .map_err(|_| DatabaseError::DatabaseAlreadyOpen)?;
        parts.legacy = Some(LegacyExclusion(parts.file.take().unwrap()));
        #[cfg(test)]
        parts.allocation_stage()?;
        parts.backend = Some(FileBackend::new(
            parts.legacy.as_ref().unwrap().0.try_clone()?,
        )?);
        let binding = ReadOnlyFileBinding::observe(&parts.legacy.as_ref().unwrap().0)?;
        parts.core = Some(ReadOnlyFileCore {
            backend: parts.backend.take().unwrap(),
            leases: Mutex::new(EngineLeases::new()),
            invalid: AtomicBool::new(false),
            binding,
            file: parts.legacy.take().unwrap(),
            #[cfg(test)]
            _retirement: match (resources, parts.trace.as_ref()) {
                (Some(resources), Some(trace)) => Some(CustodyRetirement {
                    resources: resources.clone(),
                    trace: trace.clone(),
                }),
                _ => None,
            },
        });
        // Native shared full range + exclusive legacy flock are unchanged.
        // No Unsupported fallback may downgrade that legacy exclusion.
        #[cfg(test)]
        parts.allocation_stage()?;
        if !parts
            .core
            .as_ref()
            .unwrap()
            .backend
            .try_lock_shared_native_full_range()?
        {
            return Err(DatabaseError::DatabaseAlreadyOpen);
        }
        let core = parts.core.as_mut().unwrap();
        core.binding = ReadOnlyFileBinding::observe(&core.file.0)?;
        if let Some(prepared) = parts.prepared.as_ref() {
            prepared.require_current()?;
        }
        #[cfg(test)]
        parts.allocation_stage()?;
        // Final transfer is infallible once the first native data field moves.
        let core = parts.core.take().unwrap();
        let shared = match parts.prepared.take() {
            Some(prepared) => prepared.finish(core),
            None => NativeOwnedShared::ordinary(core),
        };
        Ok(Self { shared })
    }

    pub(super) fn clone_file_custody(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }

    pub(super) fn same_owner(&self, other: &Self) -> bool {
        self.shared.same_allocation(&other.shared)
    }

    pub(super) fn actual_file_binding(&self) -> io::Result<ReadOnlyFileBinding> {
        self.shared.available()?;
        let observed = ReadOnlyFileBinding::observe(&self.shared.file.0)?;
        if observed != self.shared.binding {
            self.shared.invalid.store(true, Ordering::Release);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "readonly artifact changed",
            ));
        }
        Ok(observed)
    }

    // The sole native header parser supplies its fixed stack buffer. No pathname,
    // new engine, allocation, format decoder or engine sublease is introduced.
    pub(super) fn read_header<const N: usize>(&self, out: &mut [u8; N]) -> io::Result<()> {
        if N == 0 || N > 4096 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded native header required",
            ));
        }
        self.actual_file_binding()?;
        self.shared.observe_io(self.shared.backend.read(0, out))?;
        self.actual_file_binding()?;
        Ok(())
    }

    pub(super) fn is_drained(&self) -> io::Result<bool> {
        self.shared.available()?;
        let leases = self.shared.lock()?;
        Ok(leases.active.is_none() && leases.ranges.iter().all(Option::is_none))
    }

    pub(super) fn is_exclusive_handoff_owner(&self) -> io::Result<bool> {
        self.actual_file_binding()?;
        // After the maintenance owner is consumed, only pre-existing plans or
        // engine subleases could retain another strong core. No raw core/weak
        // or clone authority is public. A second plan therefore must refuse.
        Ok(self.is_drained()? && self.shared.strong_count() == 1)
    }

    pub(super) fn lease(&self) -> io::Result<ReadOnlyReopenBackend> {
        self.actual_file_binding()?;
        let mut leases = self.shared.lock()?;
        if leases.active.is_some() || leases.ranges.iter().any(Option::is_some) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "readonly engine still live",
            ));
        }
        let generation = leases
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("readonly lease generation exhausted"))?;
        leases.generation = generation;
        leases.active = Some(generation);
        Ok(ReadOnlyReopenBackend {
            shared: self.shared.clone(),
            generation,
        })
    }

    // Actual ordinary Arc<T> or Source Arc<OwnedValue<T>> header. The native
    // owner computes its own typed allocation; no raw layout reconstruction.
    pub(super) fn control_capacity_bytes(&self) -> Result<u64, ReadOnlyReopenError> {
        self.shared.available().map_err(StorageError::from)?;
        Ok(self.shared.control_capacity_bytes()?)
    }
}

// Generic only over a private inferred prepared DATA control. This is an
// intact transition container, not an implementable authority/callback port.
struct CustodyConstruction<P> {
    core: Option<ReadOnlyFileCore>,
    backend: Option<FileBackend>,
    legacy: Option<LegacyExclusion>,
    file: Option<File>,
    #[cfg(test)]
    trace: Option<Arc<CustodyConstructionTrace>>,
    prepared: Option<P>,
}
#[cfg(test)]
impl<P> CustodyConstruction<P> {
    fn allocation_stage(&self) -> Result<(), DatabaseError> {
        if let Some(trace) = &self.trace {
            let n = trace.stages.load(Ordering::Acquire);
            if trace.fail_after == Some(n) {
                return Err(DatabaseError::Storage(StorageError::Io(io::Error::from(
                    io::ErrorKind::OutOfMemory,
                ))));
            }
            trace.stages.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }
}
#[cfg(test)]
struct CustodyConstructionTrace {
    fail_after: Option<usize>,
    stages: AtomicUsize,
    retired_at: AtomicUsize,
}
#[cfg(test)]
impl CustodyConstructionTrace {
    fn new(fail_after: Option<usize>) -> Self {
        Self {
            fail_after,
            stages: AtomicUsize::new(0),
            retired_at: AtomicUsize::new(usize::MAX),
        }
    }
}
#[cfg(test)]
struct CustodyRetirement {
    resources: NativePageResources,
    trace: Arc<CustodyConstructionTrace>,
}
#[cfg(test)]
impl Drop for CustodyRetirement {
    fn drop(&mut self) {
        // Core backend/actual tracker allocation and LegacyExclusion have
        // physically retired; enclosing OwnedValue still retains fixed charge.
        self.trace
            .retired_at
            .store(self.resources.current(), Ordering::Release);
    }
}

impl ReadOnlyFileCore {
    fn available(&self) -> io::Result<()> {
        if self.invalid.load(Ordering::Acquire) {
            Err(closed())
        } else {
            Ok(())
        }
    }
    fn lock(&self) -> io::Result<MutexGuard<'_, EngineLeases>> {
        self.leases.lock().map_err(|_| {
            self.invalid.store(true, Ordering::Release);
            closed()
        })
    }
    fn observe_io<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.invalid.store(true, Ordering::Release);
        }
        result
    }
}
impl Drop for ReadOnlyFileCore {
    fn drop(&mut self) {
        // Closing descriptors alone cannot release locks on an inherited OFD.
        let _ = self.backend.close();
    }
}

pub(super) struct ReadOnlyReopenBackend {
    shared: NativeOwnedShared<ReadOnlyFileCore>,
    generation: u64,
}
impl std::fmt::Debug for ReadOnlyReopenBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadOnlyReopenBackend([REDACTED])")
    }
}
impl ReadOnlyReopenBackend {
    fn access(&self) -> io::Result<MutexGuard<'_, EngineLeases>> {
        let leases = self.shared.lock()?;
        self.shared.available()?;
        if leases.active != Some(self.generation) {
            return Err(closed());
        }
        Ok(leases)
    }
    fn lock_range(&self, range: NativeRange) -> Result<bool, BackendError> {
        interval(range)?;
        let mut leases = self.access()?;
        if leases.ranges.contains(&Some(range)) {
            return Ok(true);
        }
        let slot = leases
            .ranges
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::StorageFull,
                    "readonly lease capacity exhausted",
                )
            })?;
        *slot = Some(range);
        Ok(true)
    }
}
impl StorageBackend for ReadOnlyReopenBackend {
    fn len(&self) -> io::Result<u64> {
        let _access = self.access()?;
        self.shared.observe_io(self.shared.backend.len())
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let _access = self.access()?;
        self.shared
            .observe_io(self.shared.backend.read(offset, out))
    }
    fn set_len(&self, _: u64) -> io::Result<()> {
        Err(readonly())
    }
    fn sync_data(&self) -> io::Result<()> {
        Err(readonly())
    }
    fn write(&self, _: u64, _: &[u8]) -> io::Result<()> {
        Err(readonly())
    }
    fn close(&self) -> io::Result<()> {
        let mut leases = match self.shared.leases.lock() {
            Ok(leases) => leases,
            Err(poisoned) => {
                self.shared.invalid.store(true, Ordering::Release);
                poisoned.into_inner()
            }
        };
        if leases.active == Some(self.generation) {
            leases.ranges.fill(None);
            leases.active = None;
        }
        // Exactly this sublease is revoked. The concrete native exclusion stays.
        Ok(())
    }
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.lock_range((start, end))
    }
    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.lock_range((start, end))
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.lock_range((start, end)).map(|_| ())
    }
    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.lock_range((start, end)).map(|_| ())
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        let mut leases = self.access()?;
        let slot = leases
            .ranges
            .iter_mut()
            .find(|slot| **slot == Some((start, end)))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "readonly lease not held")
            })?;
        *slot = None;
        Ok(())
    }
    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        let query = interval((start, end))?;
        let leases = self.access()?;
        for held in leases.ranges.iter().flatten() {
            let held = interval(*held)?;
            if query.0 < held.1 && held.0 < query.1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "readonly query overlaps own lease",
                )
                .into());
            }
        }
        Ok(false)
    }
}
impl Drop for ReadOnlyReopenBackend {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
fn interval(range: NativeRange) -> io::Result<(u128, u128)> {
    let start = match range.0 {
        Bound::Unbounded => 0,
        Bound::Included(n) => u128::from(n),
        Bound::Excluded(n) => u128::from(n) + 1,
    };
    let end = match range.1 {
        Bound::Unbounded => u128::from(u64::MAX) + 1,
        Bound::Included(n) => u128::from(n) + 1,
        Bound::Excluded(n) => u128::from(n),
    };
    if start >= end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty readonly lease",
        ));
    }
    Ok((start, end))
}
fn closed() -> io::Error {
    io::Error::other("readonly native custody unavailable")
}
fn readonly() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "readonly native custody")
}

#[cfg(test)]
#[path = "read_only_reopen_custody_tests.rs"]
mod tests;
