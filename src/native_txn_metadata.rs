//! Transaction buffers owned by the native complete fixed-recipe preparation.
//! There is no public constructor, Source capability or write permission here.
//! Capacity inputs must come from the actual complete recipe theorem; the
//! closed production factory remains inactive until that theorem is joined.
use super::*;
use crate::tree_store::{NativePageResources, NativeTxnNameMap};

pub(super) struct NativePreparedNamespace {
    pub(super) open: NativeTxnNameMap<&'static panic::Location<'static>>,
    pub(super) pending: NativeTxnNameMap<(Option<BtreeHeader>, u64, bool)>,
    pub(super) freed: NativeOwnedShared<Mutex<NativeOwnedPageVec>>,
}
impl NativePreparedNamespace {
    fn prepare(
        resources: &NativePageResources,
        names: &[&str],
        freed_capacity: usize,
        system: bool,
    ) -> Result<Self> {
        let open = NativeTxnNameMap::prepare_source(names, resources)?;
        let pending = NativeTxnNameMap::prepare_source(names, resources)?;
        let mut freed = if system {
            NativeOwnedPageVec::prepare_system_freed(resources, freed_capacity)?
        } else {
            NativeOwnedPageVec::prepare_data_freed(resources, freed_capacity)?
        };
        // All local scratch backing/control is admitted before capture/writer
        // claim. Operation entries may only transfer/recycle these slots.
        freed.prepare_fixed_local_storage(resources)?;
        let freed = NativeOwnedShared::source(Mutex::new(freed), resources)?;
        Ok(Self {
            open,
            pending,
            freed,
        })
    }
}

pub(crate) struct NativePreparedTxnMetadata<'db> {
    // The actual parsed engine borrow binds preparation. No adopted ID/root
    // tuple, global registry or strong engine backreference is introduced.
    memory: &'db TransactionalMemory,
    pub(super) data: NativePreparedNamespace,
    pub(super) system: NativePreparedNamespace,
    completed: Option<[crate::tree_store::NativeOwnedTxnIds; 2]>,
    data_freed_capacity: usize,
    system_freed_capacity: usize,
    maximum_removed_entries: usize,
}
impl<'db> NativePreparedTxnMetadata<'db> {
    pub(crate) fn prepare(
        memory: &'db TransactionalMemory,
        resources: &NativePageResources,
        data_names: &[&str],
        system_names: &[&str],
        data_freed_capacity: usize,
        system_freed_capacity: usize,
        maximum_removed_entries: usize,
    ) -> Result<Self> {
        if !resources.is_source_write() {
            return Err(crate::tree_store::source_owner_refused());
        }
        let data =
            NativePreparedNamespace::prepare(resources, data_names, data_freed_capacity, false)?;
        let system =
            NativePreparedNamespace::prepare(resources, system_names, system_freed_capacity, true)?;
        let completed = [
            crate::tree_store::NativeOwnedTxnIds::prepare_source(
                maximum_removed_entries,
                resources,
            )?,
            crate::tree_store::NativeOwnedTxnIds::prepare_source(
                maximum_removed_entries,
                resources,
            )?,
        ];
        Ok(Self {
            memory,
            data,
            system,
            completed: Some(completed),
            data_freed_capacity,
            system_freed_capacity,
            maximum_removed_entries,
        })
    }
    // The complete immutable native recipe supplies its own union/counts;
    // no caller subset or detached numeric observation can replace them.
    pub(crate) fn require_for_recipe(
        &self,
        memory: &TransactionalMemory,
        resources: &NativePageResources,
        data_names: &[&str],
        system_names: &[&str],
        data_freed_capacity: usize,
        system_freed_capacity: usize,
        maximum_removed_entries: usize,
    ) -> Result<()> {
        self.require_for(memory, resources)?;
        if self.data_freed_capacity != data_freed_capacity
            || self.system_freed_capacity != system_freed_capacity
            || self.maximum_removed_entries != maximum_removed_entries
            || !self.data.open.matches_prepared_names(data_names)
            || !self.data.pending.matches_prepared_names(data_names)
            || !self.system.open.matches_prepared_names(system_names)
            || !self.system.pending.matches_prepared_names(system_names)
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(())
    }
    pub(crate) fn require_system_freed_upper(&self, upper: usize) -> Result<()> {
        if self.system_freed_capacity < upper {
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(())
    }
    pub(crate) fn require_data_freed_upper(&self, upper: usize) -> Result<()> {
        if self.data_freed_capacity < upper {
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(())
    }

    pub(crate) fn require_fixed_recipe_names(
        &self,
        recipe: &crate::tree_store::NativeOwnedFixedRecipe,
    ) -> Result<()> {
        let mut system = NativeSystemFamily::all().map(NativeSystemFamily::name);
        system.sort_unstable();
        if !self.data.open.matches_fixed_recipe_names(recipe)
            || !self.data.pending.matches_fixed_recipe_names(recipe)
            || !self.system.open.matches_prepared_names(&system)
            || !self.system.pending.matches_prepared_names(&system)
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        Ok(())
    }
    fn require_namespace_owners(
        &self,
        memory: &TransactionalMemory,
        resources: &NativePageResources,
    ) -> Result<()> {
        if !core::ptr::eq(self.memory, memory)
            || !self.data.open.belongs_to(resources)
            || !self.data.pending.belongs_to(resources)
            || !self.system.open.belongs_to(resources)
            || !self.system.pending.belongs_to(resources)
        {
            return Err(crate::tree_store::source_owner_refused());
        }
        // Check actual buffer controls/families, not merely the maps' pool.
        // Independent locks die before any namespace/table mutation.
        {
            let data = self.data.freed.lock()?;
            data.require_prepared_data_freed(resources, self.data_freed_capacity)?;
            data.require_fixed_local_storage(resources)?;
        }
        {
            let system = self.system.freed.lock()?;
            system.require_prepared_system_freed(resources, self.system_freed_capacity)?;
            system.require_fixed_local_storage(resources)?;
        }
        Ok(())
    }
    pub(crate) fn require_for(
        &self,
        memory: &TransactionalMemory,
        resources: &NativePageResources,
    ) -> Result<()> {
        self.require_namespace_owners(memory, resources)?;
        let completed = self
            .completed
            .as_ref()
            .ok_or_else(crate::tree_store::source_owner_refused)?;
        for buffer in completed {
            buffer.require_for(self.maximum_removed_entries, resources)?;
        }
        Ok(())
    }
    pub(crate) fn require_with_reclamation(
        &self,
        memory: &TransactionalMemory,
        resources: &NativePageResources,
        budget: &bounded_reclamation::ReclamationBudget,
    ) -> Result<()> {
        self.require_namespace_owners(memory, resources)?;
        if let Some(completed) = &self.completed {
            for buffer in completed {
                buffer.require_for(self.maximum_removed_entries, resources)?;
            }
        } else {
            // Absence is never evidence: inspect the actual SAME pair in its
            // sole receiving owner, including capacity and exact pool affinity.
            budget.require_completed_storage(resources, self.maximum_removed_entries)?;
        }
        Ok(())
    }
    pub(crate) fn move_completed_into(
        &mut self,
        memory: &TransactionalMemory,
        resources: &NativePageResources,
        budget: &mut bounded_reclamation::ReclamationBudget,
    ) -> Result<()> {
        self.require_namespace_owners(memory, resources)?;
        if self.completed.is_some() {
            budget.adopt_completed_storage(
                &mut self.completed,
                resources,
                self.maximum_removed_entries,
            )?;
        } else {
            budget.require_completed_storage(resources, self.maximum_removed_entries)?;
        }
        Ok(())
    }
}
