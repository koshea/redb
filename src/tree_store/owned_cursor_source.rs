//! Named backing for one closed exact-key SYSTEM extraction.
use super::{Branch, FixedCursorKey};
use crate::Result;
use crate::tree_store::PageResolver;
use crate::tree_store::fixed_scratch::{PreparedVec, ScratchVec};
use crate::tree_store::page_store::{NativeNumericalEpoch, NativePageResources, PageImpl};

#[derive(Clone)]
pub(crate) struct NativeFixedCursorPool {
    path: PreparedVec<Branch>,
    parts: PreparedVec<(PageImpl, usize)>,
    removed: PreparedVec<usize>,
    keys: [PreparedVec<u8>; 5],
}
impl NativeFixedCursorPool {
    pub(crate) fn capacity_bytes(depth: usize) -> Result<usize> {
        PreparedVec::<Branch>::capacity_bytes(depth)?
            .checked_add(PreparedVec::<(PageImpl, usize)>::capacity_bytes(depth)?)
            .and_then(|n| n.checked_add(PreparedVec::<usize>::capacity_bytes(1).ok()?))
            .and_then(|n| {
                n.checked_add(PreparedVec::<u8>::capacity_bytes(16).ok()?.checked_mul(5)?)
            })
            .ok_or_else(crate::tree_store::source_owner_refused)
    }
    pub(crate) fn prepare(
        resolver: &PageResolver,
        numerical: &NativeNumericalEpoch,
        depth: usize,
    ) -> Result<Self> {
        let result = Self {
            path: PreparedVec::prepare(resolver, numerical, depth)?,
            parts: PreparedVec::prepare(resolver, numerical, depth)?,
            // The admitted closed extractor yields/removes ONCE then closes.
            removed: PreparedVec::prepare(resolver, numerical, 1)?,
            keys: [
                PreparedVec::prepare(resolver, numerical, 16)?,
                PreparedVec::prepare(resolver, numerical, 16)?,
                PreparedVec::prepare(resolver, numerical, 16)?,
                PreparedVec::prepare(resolver, numerical, 16)?,
                PreparedVec::prepare(resolver, numerical, 16)?,
            ],
        };
        result.require_available(numerical.resources())?;
        Ok(result)
    }
    pub(crate) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
        self.path.require_available(resources)?;
        self.parts.require_available(resources)?;
        self.removed.require_available(resources)?;
        for key in &self.keys {
            key.require_available(resources)?;
        }
        Ok(())
    }
    pub(in crate::tree_store) fn path(
        &self,
        resources: &NativePageResources,
    ) -> Result<ScratchVec<Branch>> {
        self.path.checkout(resources)
    }
    pub(in crate::tree_store) fn parts(
        &self,
        resources: &NativePageResources,
    ) -> Result<ScratchVec<(PageImpl, usize)>> {
        self.parts.checkout(resources)
    }
    pub(in crate::tree_store) fn removed(
        &self,
        resources: &NativePageResources,
    ) -> Result<ScratchVec<usize>> {
        self.removed.checkout(resources)
    }
    pub(in crate::tree_store) fn key(
        &self,
        resources: &NativePageResources,
        role: FixedCursorKey,
        bytes: &[u8],
    ) -> Result<ScratchVec<u8>> {
        if bytes.len() != 16 {
            return Err(crate::tree_store::source_owner_refused());
        }
        let index = match role {
            FixedCursorKey::Lower => 0,
            FixedCursorKey::Upper => 1,
            FixedCursorKey::Seek => 2,
            FixedCursorKey::Scan => 3,
            FixedCursorKey::Park => 4,
        };
        let mut key = self.keys[index].checkout(resources)?;
        key.try_extend_from_slice(bytes)?;
        Ok(key)
    }
}
