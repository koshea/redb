pub use crate::tree_store::InMemoryBackend;
#[cfg(not(redb_no_std))]
pub use crate::tree_store::file_backend::FileBackend;

// This closed owner records native lock/resource custody only. First-party
// artifact/path identity and operational authority remain outside this type.
#[cfg(not(redb_no_std))]
mod full_range_custody {
    use super::FileBackend;
    use crate::{ReadOnlyReopenError, StorageBackend};
    use std::{fs::File, ops::Bound};

    /// Opaque full-range lock ownership with a fresh, immutable tracker profile.
    /// This is numerical custody; it grants no database or Source authority.
    pub struct FullRangeFileCustody {
        backend: FileBackend,
    }
    impl FullRangeFileCustody {
        /// Preserves ordinary FileBackend construction/lock behavior. The
        /// resource build profile is checked only by the separate quote.
        pub fn acquire(file: File) -> Result<Option<Self>, ReadOnlyReopenError> {
            let backend = FileBackend::new(file).map_err(ReadOnlyReopenError::Native)?;
            let owner = Self { backend };
            let acquired = owner
                .backend
                .try_lock_range(Bound::Unbounded, Bound::Unbounded)
                .map_err(|error| ReadOnlyReopenError::Storage(error.into()))?;
            // False and error both destroy this actual partial owner, closing
            // any acquired native/fallback lock before its descriptor drops.
            Ok(acquired.then_some(owner))
        }
        /// Pure native fixed-control quote; no actual owner/I/O is required.
        pub fn quote_capture_bytes() -> Result<u64, ReadOnlyReopenError> {
            FileBackend::full_range_custody_quote_capture_bytes()
        }
        /// The constructor is the only lock mutation before Drop. No clone,
        /// raw backend/FD, StorageBackend impl, or caller freshness stamp exists.
        pub fn retained_resource_bytes(&self) -> Result<u64, ReadOnlyReopenError> {
            self.backend.full_range_custody_retained_bytes()
        }
    }
    impl Drop for FullRangeFileCustody {
        fn drop(&mut self) {
            let _ = self.backend.close();
        }
    }
}
#[cfg(not(redb_no_std))]
pub use full_range_custody::FullRangeFileCustody;
