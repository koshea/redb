//! Actual copied return-value/key buffers, not native page aliases.
//! Admission precedes Vec construction, and the byte buffer physically drops
//! before its control charge. No value/Arc/raw numerical owner escapes here.
#[cfg(not(redb_no_std))]
use super::page_resources::{NativeControlAllocation, NativePageResources};
use alloc::vec::Vec;
use core::ops::Deref;

pub(crate) struct NativeOwnedValueBytes {
    bytes: Vec<u8>,
    #[cfg(not(redb_no_std))]
    _allocation: Option<NativeControlAllocation>,
}
impl NativeOwnedValueBytes {
    pub(crate) fn ordinary(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            #[cfg(not(redb_no_std))]
            _allocation: None,
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn copy_source(
        bytes: &[u8],
        resources: &NativePageResources,
    ) -> crate::Result<Self> {
        use super::{container_bounds, page_resources::refused};
        let bound = container_bounds::vector_storage::<u8>(bytes.len()).map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(bytes.len())
            .map_err(|_| refused())?;
        copied.extend_from_slice(bytes);
        Ok(Self {
            bytes: copied,
            _allocation: Some(allocation),
        })
    }
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}
impl Deref for NativeOwnedValueBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
