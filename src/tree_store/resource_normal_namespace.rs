//! Native-owned ordered normal-namespace versions for the fixed physical recipe.
//! Numerical only: no assigned root, fabricated definition bytes or writer API.
use super::*;
use crate::transaction_tracker::NativeResourceEpoch;
use crate::tree_store::{
    NativeControlAllocation, NativeDefinitionResourceError, NativeFixedPointCells,
    NativeKeyPathError as E, NativeOwnedFixedRecipe,
};
use crate::types::NativeResourceTypeName;

// An internal observation label, not a persisted table name or input authority.
pub(crate) const NORMAL_NAMESPACE_OBSERVATION: &str = "[native DATA namespace]";
type R<T> = core::result::Result<T, NativeNormalNamespaceError>;
pub(crate) enum NativeNormalNamespaceError {
    Path(E),
    Table(TableError),
}
impl From<E> for NativeNormalNamespaceError {
    fn from(value: E) -> Self {
        Self::Path(value)
    }
}
impl From<TableError> for NativeNormalNamespaceError {
    fn from(value: TableError) -> Self {
        Self::Table(value)
    }
}
impl From<crate::StorageError> for NativeNormalNamespaceError {
    fn from(value: crate::StorageError) -> Self {
        Self::Path(value.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeNamespacePutPhase {
    Create,
    FinalFlush,
}
struct NamespacePut {
    table: usize,
    phase: NativeNamespacePutPhase,
    definition_bytes: usize,
}
struct NativeNamespaceOperations {
    ordered: Vec<NamespacePut>,
    creations: usize,
    incoming_bytes: usize,
    maximum_key: usize,
    maximum_value: usize,
    // Whole real vector capacity dies before SAME-pool guard retirement.
    _allocation: NativeControlAllocation,
}
impl NativeNamespaceOperations {
    fn push(
        &mut self,
        recipe: &NativeOwnedFixedRecipe,
        table: usize,
        phase: NativeNamespacePutPhase,
        definition_bytes: usize,
    ) -> core::result::Result<(), E> {
        if self.ordered.len() == self.ordered.capacity() {
            return Err(E::VisitsExceeded);
        }
        let name = recipe.table_name(table).ok_or(E::Malformed)?;
        let incoming = name
            .len()
            .checked_add(definition_bytes)
            .ok_or(E::Overflow)?;
        self.incoming_bytes = self
            .incoming_bytes
            .checked_add(incoming)
            .ok_or(E::Overflow)?;
        self.maximum_key = self.maximum_key.max(name.len());
        self.maximum_value = self.maximum_value.max(definition_bytes);
        if phase == NativeNamespacePutPhase::Create {
            self.creations = self.creations.checked_add(1).ok_or(E::Overflow)?;
        }
        self.ordered.push(NamespacePut {
            table,
            phase,
            definition_bytes,
        });
        Ok(())
    }
}

// The recipe view borrows the ACTUAL typed namespace and original immutable
// physical input. Repeated keys remain ordered create/final-flush versions.
pub(crate) struct NativeNormalNamespaceRecipe<'tree, 'recipe, 'epoch, 'tracker> {
    actual: &'tree Btree<&'static str, InternalTableDefinition>,
    original: &'recipe NativeOwnedFixedRecipe,
    operations: &'recipe NativeNamespaceOperations,
    epoch: &'epoch NativeResourceEpoch<'tracker>,
}
impl NativeNormalNamespaceRecipe<'_, '_, '_, '_> {
    pub(crate) fn points(&self) -> usize {
        self.operations.ordered.len()
    }
    pub(crate) fn key_at(&self, index: usize) -> core::result::Result<&[u8], E> {
        let point = self.operations.ordered.get(index).ok_or(E::Malformed)?;
        Ok(self
            .original
            .table_name(point.table)
            .ok_or(E::Malformed)?
            .as_bytes())
    }
    pub(crate) fn incoming_bytes(&self) -> usize {
        self.operations.incoming_bytes
    }
    pub(crate) fn maximum_key(&self) -> usize {
        self.operations.maximum_key
    }
    pub(crate) fn maximum_value(&self) -> usize {
        self.operations.maximum_value
    }
    pub(crate) fn require_for(
        &self,
        actual: &Btree<&'static str, InternalTableDefinition>,
        epoch: &NativeResourceEpoch<'_>,
    ) -> core::result::Result<(), E> {
        if !core::ptr::eq(self.actual, actual) || !core::ptr::eq(self.epoch, epoch) {
            return Err(E::ForeignOwner);
        }
        actual.require_normal_namespace_epoch(epoch)?;
        let current = actual.fixed_namespace_affinity()?;
        if !self.original.matches_observation_allocation(&current)
            || !self
                .original
                .matches_observation_allocation(&self.operations._allocation)
        {
            return Err(E::ForeignOwner);
        }
        Ok(())
    }
}

// Inseparable member of NativeFixedDataCensus. No arbitrary operation-list or
// detached scalar facts constructor is exposed to the complete native factory.
pub(crate) struct NativeFixedNamespaceCensus {
    cells: NativeFixedPointCells,
    operations: NativeNamespaceOperations,
}
impl NativeFixedNamespaceCensus {
    pub(crate) fn maximum_branch_key(&self) -> usize {
        self.cells.maximum_branch_key()
    }

    pub(crate) fn checksum_shape(&self) -> (usize, usize) {
        self.cells.checksum_shape()
    }

    pub(crate) fn operation_count(&self) -> usize {
        self.operations.ordered.len()
    }
    pub(crate) fn creation_count(&self) -> usize {
        self.operations.creations
    }
    pub(crate) fn operation(
        &self,
        index: usize,
    ) -> Option<(usize, NativeNamespacePutPhase, usize)> {
        self.operations
            .ordered
            .get(index)
            .map(|op| (op.table, op.phase, op.definition_bytes))
    }
    pub(crate) fn old_pages(&self) -> usize {
        self.cells.old_pages()
    }
    pub(crate) fn old_physical_bytes(&self) -> u64 {
        self.cells.old_physical_bytes()
    }
    pub(crate) fn final_live_physical_upper(&self) -> u64 {
        self.cells.final_live_physical_upper()
    }
    pub(crate) fn point_scratch_heap_upper(&self) -> u64 {
        self.cells.point_scratch_heap_upper()
    }
    pub(crate) fn capture_control_bytes() -> core::result::Result<u64, E> {
        u64::try_from(size_of::<(
            Self,
            NativeNormalNamespaceRecipe<'static, 'static, 'static, 'static>,
            NativeNormalNamespaceError,
            core::result::Result<Self, NativeNormalNamespaceError>,
            Option<InternalTableDefinition>,
            NamespacePut,
            [usize; 6],
        )>())
        .map_err(|_| E::Overflow)
    }
}

impl TableTree {
    pub(crate) fn collect_fixed_namespace(
        &self,
        original: &NativeOwnedFixedRecipe,
        epoch: &NativeResourceEpoch<'_>,
    ) -> R<NativeFixedNamespaceCensus> {
        self.tree.require_normal_namespace_epoch(epoch)?;
        let maximum = original.tables_len().checked_mul(2).ok_or(E::Overflow)?;
        let bytes = crate::tree_store::page_store::point_vector_bytes::<NamespacePut>(maximum)?
            .checked_add(NativeFixedNamespaceCensus::capture_control_bytes()?)
            .ok_or(E::Overflow)?;
        let allocation = self
            .mem
            .admit_fixed_observation(usize::try_from(bytes).map_err(|_| E::Overflow)?)?;
        if !original.matches_observation_allocation(&allocation) {
            return Err(E::ForeignOwner.into());
        }
        let mut operations = NativeNamespaceOperations {
            ordered: Vec::new(),
            creations: 0,
            incoming_bytes: 0,
            maximum_key: 0,
            maximum_value: 0,
            _allocation: allocation,
        };
        operations
            .ordered
            .try_reserve_exact(maximum)
            .map_err(|_| E::from(crate::tree_store::source_owner_refused()))?;
        // Creation follows ORIGINAL first-open order, never lexical/key order.
        // No set/map/extra vector is allocated to derive that order.
        let mut first_opens = 0usize;
        for point in 0..original.points_len() {
            let Some(table) = original.table_first_open_at(point) else {
                continue;
            };
            first_opens = first_opens.checked_add(1).ok_or(E::Overflow)?;
            let name = original.table_name(table).ok_or(E::Malformed)?;
            let absent = match original.table_uses_str_keys(table) {
                Some(true) => self
                    .get_table::<&'static str, &'static [u8]>(name, TableType::Normal)?
                    .is_none(),
                Some(false) => self
                    .get_table::<&'static [u8], &'static [u8]>(name, TableType::Normal)?
                    .is_none(),
                None => return Err(E::Malformed.into()),
            };
            if absent {
                operations.push(
                    original,
                    table,
                    NativeNamespacePutPhase::Create,
                    definition_bytes(original, table)?,
                )?;
            }
        }
        if first_opens != original.tables_len() {
            return Err(E::Malformed.into());
        }
        // Existing pending-name map drains lexically and contains at most one
        // FINAL root update per touched table. Include even a skipped no-op.
        for table in 0..original.tables_len() {
            operations.push(
                original,
                table,
                NativeNamespacePutPhase::FinalFlush,
                definition_bytes(original, table)?,
            )?;
        }
        let view = NativeNormalNamespaceRecipe {
            actual: &self.tree,
            original,
            operations: &operations,
            epoch,
        };
        let cells = self.tree.collect_normal_namespace_cells(&view, epoch)?;
        view.require_for(&self.tree, epoch)?;
        self.tree.require_normal_namespace_epoch(epoch)?;
        Ok(NativeFixedNamespaceCensus { cells, operations })
    }
}
fn definition_bytes(
    original: &NativeOwnedFixedRecipe,
    table: usize,
) -> core::result::Result<usize, E> {
    fn encoded<K: Key + NativeResourceTypeName>() -> core::result::Result<usize, E> {
        InternalTableDefinition::native_resource_encoded_len::<K, &'static [u8]>().map_err(
            |error| match error {
                NativeDefinitionResourceError::Malformed => E::Malformed,
                NativeDefinitionResourceError::Overflow => E::Overflow,
            },
        )
    }
    match original.table_uses_str_keys(table) {
        Some(true) => encoded::<&'static str>(),
        Some(false) => encoded::<&'static [u8]>(),
        None => Err(E::Malformed),
    }
}
