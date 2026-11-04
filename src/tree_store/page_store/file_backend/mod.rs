#[cfg(any(windows, unix, target_os = "wasi"))]
mod optimized;
#[cfg(any(windows, unix, target_os = "wasi"))]
pub use optimized::FileBackend;

// Beside the backend that uses it: the fallback backend below has no file to lock
#[cfg(any(windows, unix, target_os = "wasi"))]
pub(crate) mod range_lock;

#[cfg(not(any(windows, unix, target_os = "wasi")))]
mod fallback;
#[cfg(not(any(windows, unix, target_os = "wasi")))]
pub use fallback::FileBackend;

#[cfg(all(test, target_os = "linux"))]
#[path = "fallback.rs"]
mod no_native_range_backend;

#[cfg(all(test, target_os = "linux"))]
mod readonly_no_fallback_tests {
    use super::no_native_range_backend::FileBackend;
    use crate::{BackendError, StorageBackend};
    use std::fs::{File, TryLockError};

    // req: STO-020, REC-001, REC-003, REC-005, PRJ-004
    #[test]
    fn readonly_native_unsupported_never_downgrades_retained_exclusive_legacy_lock() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let legacy = File::open(file.path()).unwrap();
        legacy.try_lock().unwrap();
        let backend = FileBackend::new(legacy.try_clone().unwrap()).unwrap();
        assert!(matches!(
            backend.try_lock_shared_native_full_range(),
            Err(BackendError::Unsupported)
        ));
        for _ in 0..2 {
            let observer = File::open(file.path()).unwrap();
            assert!(matches!(
                observer.try_lock_shared(),
                Err(TryLockError::WouldBlock)
            ));
            assert!(matches!(observer.try_lock(), Err(TryLockError::WouldBlock)));
            backend.close().unwrap();
        }
        drop(backend);
        let observer = File::open(file.path()).unwrap();
        assert!(matches!(
            observer.try_lock_shared(),
            Err(TryLockError::WouldBlock)
        ));
        assert!(matches!(observer.try_lock(), Err(TryLockError::WouldBlock)));
        legacy.unlock().unwrap();
        observer.try_lock_shared().unwrap();
        observer.unlock().unwrap();
        observer.try_lock().unwrap();
        observer.unlock().unwrap();
    }
}
