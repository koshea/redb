//! Actual preclaim backing; this partial member does not certify all15/peak.
use super::*;
use crate::tree_store::{
    NativeBranchChildrenPool, NativeBranchKeyPool, NativeChecksumPool, NativeEntryBufferPool,
    NativeFixedCursorPool, NativeFixedPackingPool,
};

pub(crate) struct NativeFixedExecutionBufferDemand {
    checksum_depth: usize,
    checksum_children: usize,
    branch_children: usize,
    maximum_branch_key: usize,
    cursor_depth: usize,
    sparse_pairs: usize,
    sparse_payload: usize,
}
impl NativeFixedExecutionBufferDemand {
    pub(super) fn from_bound_owners(
        data: &NativeFixedDataCensus<'_>,
        phase: &NativeFixedSystemPhasePlan,
    ) -> R<Self> {
        let (data_depth, data_children) = data.checksum_shape()?;
        let (depth, children, pairs, payload) = phase.execution_buffers_shape();
        Ok(Self {
            checksum_depth: data_depth.max(depth),
            cursor_depth: depth,
            checksum_children: data_children.max(children),
            // DATA already carries max(terminals+1,2). The SYSTEM phase
            // tuple carries terminals, so include the actual new-root child.
            branch_children: data_children
                .max(children.checked_add(1).ok_or(E::Overflow)?)
                .max(2),
            maximum_branch_key: data.maximum_branch_key()?.max(
                <crate::transactions::TransactionIdWithPagination as crate::Value>::fixed_width().ok_or(E::UnsupportedState)?
            ).max(crate::transactions::NativeSystemFamily::all().into_iter().map(|f| f.name().len()).max().unwrap_or(0)),
            sparse_pairs: pairs,
            sparse_payload: payload,
        })
    }
    pub(crate) fn prepare_on_actual_resolver(
        &self,
        resolver: &PageResolver,
        numerical: &NativeNumericalEpoch,
    ) -> R<NativeFixedExecutionBuffers> {
        let checksum = NativeChecksumPool::prepare(
            resolver,
            numerical,
            self.checksum_depth,
            self.checksum_children,
        )?;
        let sparse = NativeEntryBufferPool::prepare(
            resolver,
            numerical,
            self.sparse_pairs,
            self.sparse_payload,
        )?;
        let cursor = NativeFixedCursorPool::prepare(resolver, numerical, self.cursor_depth)?;
        let packing = NativeFixedPackingPool::prepare(resolver, numerical, self.sparse_pairs)?;
        let branch_children =
            NativeBranchChildrenPool::prepare(resolver, numerical, self.branch_children)?;
        let branch_keys = NativeBranchKeyPool::prepare(
            resolver,
            numerical,
            self.branch_children.checked_sub(1).ok_or(E::Overflow)?,
            self.maximum_branch_key,
        )?;
        #[cfg(test)]
        eprintln!(
            "native_branch_key_preclaim_quote C={} K={} M={} Qadded={} Qbuffers={}",
            self.branch_children,
            self.branch_children - 1,
            self.maximum_branch_key,
            NativeBranchKeyPool::capacity_bytes(self.branch_children - 1, self.maximum_branch_key)?,
            self.capacity_bytes()?
        );
        let result = NativeFixedExecutionBuffers {
            checksum,
            sparse,
            cursor,
            packing,
            branch_children,
            branch_keys,
        };
        result.require_available(numerical.resources())?;
        Ok(result)
    }
    #[cfg(test)]
    pub(super) fn branch_key_quote_for_fixture(&self) -> R<(usize, usize, usize, usize)> {
        let keys = self.branch_children.checked_sub(1).ok_or(E::Overflow)?;
        Ok((
            self.branch_children,
            keys,
            self.maximum_branch_key,
            NativeBranchKeyPool::capacity_bytes(keys, self.maximum_branch_key)?,
        ))
    }
    pub(super) fn capacity_bytes(&self) -> R<usize> {
        NativeChecksumPool::capacity_bytes(self.checksum_depth, self.checksum_children)?
            .checked_add(NativeEntryBufferPool::capacity_bytes(
                self.sparse_pairs,
                self.sparse_payload,
            )?)
            .and_then(|n| {
                n.checked_add(NativeFixedCursorPool::capacity_bytes(self.cursor_depth).ok()?)
            })
            .and_then(|n| {
                n.checked_add(NativeFixedPackingPool::capacity_bytes(self.sparse_pairs).ok()?)
            })
            .and_then(|n| {
                n.checked_add(NativeBranchChildrenPool::capacity_bytes(self.branch_children).ok()?)
            })
            .and_then(|n| {
                NativeBranchKeyPool::capacity_bytes(
                    self.branch_children.checked_sub(1)?,
                    self.maximum_branch_key,
                )
                .ok()?
                .checked_add(n)
            })
            .ok_or(E::Overflow)
    }
}
#[derive(Clone)]
pub(crate) struct NativeFixedExecutionBuffers {
    pub(crate) checksum: NativeChecksumPool,
    pub(crate) sparse: NativeEntryBufferPool,
    pub(crate) cursor: NativeFixedCursorPool,
    pub(crate) packing: NativeFixedPackingPool,
    pub(crate) branch_children: NativeBranchChildrenPool,
    pub(crate) branch_keys: NativeBranchKeyPool,
}
impl NativeFixedExecutionBuffers {
    pub(crate) fn require_available(
        &self,
        resources: &crate::tree_store::NativePageResources,
    ) -> crate::Result<()> {
        self.checksum.require_available(resources)?;
        self.cursor.require_available(resources)?;
        self.packing.require_available(resources)?;
        self.sparse.require_available(resources)?;
        self.branch_children.require_available(resources)?;
        self.branch_keys.require_available(resources)?;
        Ok(())
    }
}
