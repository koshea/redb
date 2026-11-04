//! Inseparable DATA recipe/epoch observations for the closed complete planner.
//! This is not the complete physical peak or a native writer constructor.
use super::*;
use crate::transaction_tracker::NativeResourceEpoch;
use crate::tree_store::{
    Btree, InternalTableDefinition, NativeControlAllocation, NativeFixedNamespaceCensus,
    NativeFixedPoint, NativeFixedPointCells, NativeKeyPathError, NativeNormalNamespaceError,
    NativeNumericalEpoch, NativeOwnedFixedRecipe, NativePointRecipe, ReadOnlyResourceProfile,
};
use core::mem::size_of;

pub(crate) enum NativeFixedDataError {
    Path(NativeKeyPathError),
    Table(TableError),
    Quote(crate::ReadOnlyReopenError),
}
impl From<NativeKeyPathError> for NativeFixedDataError {
    fn from(value: NativeKeyPathError) -> Self {
        Self::Path(value)
    }
}
impl From<StorageError> for NativeFixedDataError {
    fn from(value: StorageError) -> Self {
        Self::Path(value.into())
    }
}
impl From<TableError> for NativeFixedDataError {
    fn from(value: TableError) -> Self {
        Self::Table(value)
    }
}
impl From<crate::ReadOnlyReopenError> for NativeFixedDataError {
    fn from(value: crate::ReadOnlyReopenError) -> Self {
        Self::Quote(value)
    }
}
impl From<NativeNormalNamespaceError> for NativeFixedDataError {
    fn from(error: NativeNormalNamespaceError) -> Self {
        match error {
            NativeNormalNamespaceError::Path(error) => Self::Path(error),
            NativeNormalNamespaceError::Table(error) => Self::Table(error),
        }
    }
}
// Native error values can own names/type strings. They physically die BEFORE
// the admitted definition/error workspace can retire. Borrow-only inspection
// supports first-party redaction; there is no take-error-without-owner method.
pub(crate) struct NativeFixedDataFailure {
    error: NativeFixedDataError,
    _workspace: Option<NativeControlAllocation>,
}
impl NativeFixedDataFailure {
    pub(crate) fn error(&self) -> &NativeFixedDataError {
        &self.error
    }
}

pub(crate) struct NativeFixedDataCensus<'work> {
    // Immutable physical execution envelope and canonical observation groups.
    recipe: NativeOwnedFixedRecipe,
    cells: Vec<NativeFixedPointCells>,
    namespace: NativeFixedNamespaceCensus,
    epoch: NativeResourceEpoch<'work>,
    admission: crate::tree_store::NativeFixedObservationAdmission,
    _cells_storage: NativeControlAllocation,
    // Keep the complete captured inline/result/header allowance with the
    // long-lived prepared owner. Temporary parser heap allowance may safely
    // remain conservative here; no refund while this census still exists.
    _capture_workspace: NativeControlAllocation,
}
impl<'work> NativeFixedDataCensus<'work> {
    // Pure native control quote to include in the enclosing preadmission
    // BEFORE calling capture. No native pin, I/O, heap or caller root is used.
    pub(crate) fn capture_control_bytes() -> core::result::Result<u64, NativeKeyPathError> {
        let bytes = size_of::<(
            NativeFixedDataCensus<'static>,
            core::result::Result<NativeFixedDataCensus<'static>, NativeFixedDataError>,
            NativeFixedDataFailure,
            NativeFixedDataError,
            NativeFixedPointCells,
            Btree<&'static str, &'static [u8]>,
            Option<InternalTableDefinition>,
            crate::tree_store::NativeFixedTableViews<'static>,
            NativePointRecipe<'static>,
            [usize; 6],
            Option<NativeControlAllocation>,
        )>();
        let control = u64::try_from(bytes)
            .map_err(|_| NativeKeyPathError::Overflow)?
            .checked_add(crate::tree_store::point_frontier_capture_bytes()?)
            .ok_or(NativeKeyPathError::Overflow)?;
        control
            .checked_add(NativeFixedNamespaceCensus::capture_control_bytes()?)
            .ok_or(NativeKeyPathError::Overflow)
    }
    // This crate-private constructor consumes the original owned recipe and
    // creates EVERY table fact itself. A caller cannot adopt a small-recipe
    // scalar result for a different same-root operation envelope.
    pub(crate) fn capture(
        read: &ReadTransaction,
        recipe: NativeOwnedFixedRecipe,
        epoch: NativeResourceEpoch<'work>,
        numerical: &NativeNumericalEpoch,
    ) -> core::result::Result<Self, NativeFixedDataFailure> {
        let resolver = read.tree.read_page_resolver();
        let mut workspace = None;
        let result = (|| -> core::result::Result<Self, NativeFixedDataError> {
            numerical.require_open()?;
            let affinity = resolver.admit_fixed_observation(0)?;
            if !affinity.belongs_to(numerical.resources())
                || !recipe.belongs_to(numerical.resources())
            {
                return Err(NativeKeyPathError::ForeignOwner.into());
            }
            let parts = read
                .tree
                .transaction_guard()
                .checked_pinned_read_parts()
                .map_err(NativeKeyPathError::Epoch)?
                .ok_or(NativeKeyPathError::Unpinned)?;
            epoch
                .require_tracker(parts.actual_tracker())
                .map_err(NativeKeyPathError::Epoch)?;
            epoch.require_current().map_err(NativeKeyPathError::Epoch)?;
            let admission = resolver.seal_fixed_observation_admission()?;
            let definition = resolver.fixed_observation_definition_limit()?;
            let workspace_bytes =
                super::native_read_context::read_context_definition_workspace(definition)?
                    .checked_add(Self::capture_control_bytes()?)
                    .ok_or(NativeKeyPathError::Overflow)?;
            workspace = Some(resolver.admit_fixed_observation(
                usize::try_from(workspace_bytes).map_err(|_| NativeKeyPathError::Overflow)?,
            )?);
            let storage_bytes = recipe
                .tables_len()
                .checked_mul(size_of::<NativeFixedPointCells>())
                .ok_or(NativeKeyPathError::Overflow)?;
            let bound = ReadOnlyResourceProfile::allocation(
                storage_bytes,
                core::mem::align_of::<NativeFixedPointCells>(),
            )
            .map_err(crate::ReadOnlyReopenError::from)?;
            let cells_storage = resolver.admit_fixed_observation(
                usize::try_from(bound).map_err(|_| NativeKeyPathError::Overflow)?,
            )?;
            let mut cells = Vec::new();
            cells
                .try_reserve_exact(recipe.tables_len())
                .map_err(|_| crate::tree_store::source_owner_refused())?;
            for table in 0..recipe.tables_len() {
                let views = recipe.prepare_table_views(table, numerical.resources())?;
                let compiled = views.recipe()?;
                let name = recipe
                    .table_name(table)
                    .ok_or(NativeKeyPathError::Malformed)?;
                let cell = match recipe.table_uses_str_keys(table) {
                    Some(true) => observe::<&'static str>(read, name, &compiled, &epoch)?,
                    Some(false) => observe::<&'static [u8]>(read, name, &compiled, &epoch)?,
                    None => return Err(NativeKeyPathError::Malformed.into()),
                };
                cells.push(cell);
                // Temporary typed tree, native pages, views/recipe and parser
                // graphs are physically gone before the next table opens.
            }
            let namespace = read.tree.collect_fixed_namespace(&recipe, &epoch)?;
            epoch.require_current().map_err(NativeKeyPathError::Epoch)?;
            numerical.require_open()?;
            let capture_workspace = workspace
                .take()
                .ok_or(NativeKeyPathError::UnsupportedState)?;
            Ok(Self {
                recipe,
                cells,
                namespace,
                epoch,
                admission,
                _cells_storage: cells_storage,
                _capture_workspace: capture_workspace,
            })
        })();
        match result {
            Ok(census) => {
                drop(workspace);
                Ok(census)
            }
            Err(error) => Err(NativeFixedDataFailure {
                error,
                _workspace: workspace,
            }),
        }
    }
    pub(crate) fn require_current(&self) -> core::result::Result<(), NativeKeyPathError> {
        self.admission.require_current()?;
        self.epoch
            .require_current()
            .map_err(NativeKeyPathError::Epoch)?;
        self.admission.require_current()?;
        Ok(())
    }
    pub(crate) fn require_fixed_cleanup_pool(
        &self,
        numerical: &NativeNumericalEpoch,
    ) -> core::result::Result<(), NativeKeyPathError> {
        self.require_current()?;
        self.require_fixed_cleanup_serial(numerical)
    }
    pub(crate) fn require_fixed_cleanup_serial(
        &self,
        numerical: &NativeNumericalEpoch,
    ) -> core::result::Result<(), NativeKeyPathError> {
        self.admission.require_current()?;
        numerical.require_open()?;
        if !self.recipe.belongs_to(numerical.resources())
            || !self._cells_storage.belongs_to(numerical.resources())
            || !self._capture_workspace.belongs_to(numerical.resources())
        {
            return Err(NativeKeyPathError::ForeignOwner);
        }
        Ok(())
    }
    pub(crate) fn matches_exact_input(&self, points: &[NativeFixedPoint<'_>]) -> bool {
        self.recipe.matches_exact_input(points)
    }
    pub(crate) fn checksum_shape(
        &self,
    ) -> core::result::Result<(usize, usize), NativeKeyPathError> {
        self.require_current()?;
        let (mut depth, mut children) = self.namespace.checksum_shape();
        for cell in &self.cells {
            let (d, c) = cell.checksum_shape();
            depth = depth.max(d);
            children = children.max(c);
        }
        Ok((depth, children))
    }
    pub(crate) fn maximum_branch_key(&self) -> core::result::Result<usize, NativeKeyPathError> {
        self.require_current()?;
        Ok(self
            .cells
            .iter()
            .fold(self.namespace.maximum_branch_key(), |m, cell| {
                m.max(cell.maximum_branch_key())
            }))
    }
    pub(crate) fn fixed_cleanup_parts(
        &self,
    ) -> (&NativeOwnedFixedRecipe, &NativeResourceEpoch<'work>) {
        (&self.recipe, &self.epoch)
    }
    pub(crate) fn namespace(
        &self,
    ) -> core::result::Result<&NativeFixedNamespaceCensus, NativeKeyPathError> {
        self.require_current()?;
        Ok(&self.namespace)
    }
    pub(crate) fn old_data_page_numbers_upper(
        &self,
    ) -> core::result::Result<usize, NativeKeyPathError> {
        self.cells
            .iter()
            .try_fold(self.namespace.old_pages(), |n, cell| {
                n.checked_add(cell.old_pages())
                    .ok_or(NativeKeyPathError::Overflow)
            })
    }
    pub(crate) fn old_data_physical_bytes(&self) -> core::result::Result<u64, NativeKeyPathError> {
        self.cells
            .iter()
            .try_fold(self.namespace.old_physical_bytes(), |n, cell| {
                n.checked_add(cell.old_physical_bytes())
                    .ok_or(NativeKeyPathError::Overflow)
            })
    }
    // The closed executor is sequential; these are only normal per-point heap
    // builders (including namespace point geometry), not native definition
    // codec temporaries, final checksum recursion, SYSTEM scratch/pages,
    // errors from the mutable writer, or a complete physical peak.
    pub(crate) fn point_heap_upper(&self) -> u64 {
        self.cells
            .iter()
            .map(NativeFixedPointCells::point_scratch_heap_upper)
            .max()
            .unwrap_or(0)
            .max(self.namespace.point_scratch_heap_upper())
    }
    pub(crate) fn final_data_live_bytes_upper(
        &self,
    ) -> core::result::Result<u64, NativeKeyPathError> {
        self.cells
            .iter()
            .try_fold(self.namespace.final_live_physical_upper(), |n, cell| {
                n.checked_add(cell.final_live_physical_upper())
                    .ok_or(NativeKeyPathError::Overflow)
            })
    }
}

// Actual namespace lookup/type checking proves table presence OR absence.
// Constructing an empty typed Btree here is derived from that real lookup,
// never from a caller root/guard tuple or a fabricated application catalog.
fn observe<K: Key + 'static>(
    read: &ReadTransaction,
    name: &str,
    recipe: &NativePointRecipe<'_>,
    epoch: &NativeResourceEpoch<'_>,
) -> core::result::Result<NativeFixedPointCells, NativeFixedDataError> {
    let definition = read
        .tree
        .get_table::<K, &'static [u8]>(name, TableType::Normal)?;
    let root = match definition {
        Some(InternalTableDefinition::Normal { table_root, .. }) => table_root,
        None => None,
        Some(InternalTableDefinition::Multimap { .. }) => {
            return Err(NativeKeyPathError::UnsupportedKey.into());
        }
    };
    let tree = Btree::<K, &'static [u8]>::new(
        root,
        PageHint::Clean,
        read.tree.transaction_guard().clone(),
        read.tree.read_page_resolver(),
    )?;
    let prepared = tree.prepare_point_frontier(name, recipe, epoch)?;
    Ok(prepared.collect_fixed_cells()?)
}
