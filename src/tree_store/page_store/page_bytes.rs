//! All immutable native page aliases retain their actual allocation owner.
use super::page_resources::{NativePageAllocation, NativePageResources};
use crate::Result;
use alloc::sync::Arc;
use core::{fmt, ops::Deref};

#[derive(Clone)]
pub(crate) struct SharedPageBytes {
    inner: PageBytesKind,
    no_global_cache: bool,
}
#[derive(Clone)]
enum PageBytesKind {
    Legacy(Arc<[u8]>),
    // The actual data Arc (including its allocation header) drops before the
    // separately shared numerical owner. No bare Arc escapes this closed value.
    Admitted {
        bytes: Arc<[u8]>,
        _allocation: SharedPageAllocation,
    },
}

// No weak owner or naked Arc escapes this module. Every strong owner uses
// into_inner, so exactly one concurrent final drop obtains the numerical guard
// after the Arc control allocation has been freed. Ordinary Arc<T>::drop would
// run T::drop before freeing that control allocation.
#[derive(Clone)]
struct SharedPageAllocation(Option<Arc<NativePageAllocation>>);

impl Drop for SharedPageAllocation {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            drop(Arc::into_inner(owner));
        }
    }
}
impl SharedPageBytes {
    pub(super) fn zeroed(len: usize, resources: Option<&NativePageResources>) -> Result<Self> {
        let Some(resources) = resources else {
            return Ok(Self {
                inner: PageBytesKind::Legacy(core::iter::repeat_n(0_u8, len).collect()),
                no_global_cache: false,
            });
        };
        let allocation = resources.admit_page(len)?;
        Self::zeroed_admitted(len, allocation)
    }

    pub(super) fn zeroed_admitted(len: usize, allocation: NativePageAllocation) -> Result<Self> {
        allocation.validate_payload_length(len)?;
        let no_global_cache = allocation.forbids_global_cache();
        let bytes = core::iter::repeat_n(0_u8, len).collect();
        #[cfg(all(test, not(redb_no_std)))]
        let mut allocation = allocation;
        #[cfg(all(test, not(redb_no_std)))]
        allocation.record_materialized_for_test();
        Ok(Self {
            inner: PageBytesKind::Admitted {
                bytes,
                _allocation: SharedPageAllocation(Some(Arc::new(allocation))),
            },
            no_global_cache,
        })
    }

    pub(super) fn may_reuse_for_admission(&self, admitted: &NativePageAllocation) -> bool {
        match &self.inner {
            // Legacy only identifies the ordinary allocation representation.
            // The complete fixed-write factory must additionally prove that
            // the actual existing buffer is in its pinned baseline graph.
            PageBytesKind::Legacy(_) => true,
            PageBytesKind::Admitted { _allocation, .. } => _allocation
                .0
                .as_ref()
                .is_some_and(|owner| owner.same_pool(admitted)),
        }
    }

    pub(super) fn allows_global_cache(&self) -> bool {
        !self.no_global_cache
    }
    pub(super) fn forbid_global_cache(&mut self) {
        self.no_global_cache = true;
    }

    pub(super) fn get_mut(&mut self) -> Option<&mut [u8]> {
        match &mut self.inner {
            PageBytesKind::Legacy(bytes) | PageBytesKind::Admitted { bytes, .. } => {
                Arc::get_mut(bytes)
            }
        }
    }
}
impl Deref for SharedPageBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.inner {
            PageBytesKind::Legacy(bytes) | PageBytesKind::Admitted { bytes, .. } => bytes,
        }
    }
}
impl AsRef<[u8]> for SharedPageBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl From<Arc<[u8]>> for SharedPageBytes {
    fn from(bytes: Arc<[u8]>) -> Self {
        Self {
            inner: PageBytesKind::Legacy(bytes),
            no_global_cache: false,
        }
    }
}
impl fmt::Debug for SharedPageBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedPageBytes")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}
