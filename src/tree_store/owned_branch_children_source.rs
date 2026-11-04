//! Three actual scalar child arrays for the sequential fixed mutator unwind.
use super::btree_base::Checksum;
use super::fixed_scratch::{PreparedVec, ScratchVec};
use crate::Result;
use crate::tree_store::page_store::{NativeNumericalEpoch, NativePageResources};
use crate::tree_store::{PageNumber, PageResolver};

#[derive(Clone)]
pub(crate) struct NativeBranchChildrenPool {
    slots: [PreparedVec<(PageNumber, Checksum)>; 3],
    capacity: usize,
}
impl NativeBranchChildrenPool {
    pub(crate) fn capacity_bytes(capacity: usize) -> Result<usize> {
        PreparedVec::<(PageNumber, Checksum)>::capacity_bytes(capacity)?
            .checked_mul(3)
            .ok_or_else(super::source_owner_refused)
    }
    pub(crate) fn prepare(
        resolver: &PageResolver,
        numerical: &NativeNumericalEpoch,
        capacity: usize,
    ) -> Result<Self> {
        if capacity < 2 {
            return Err(super::source_owner_refused());
        }
        let result = Self {
            slots: [
                PreparedVec::prepare(resolver, numerical, capacity)?,
                PreparedVec::prepare(resolver, numerical, capacity)?,
                PreparedVec::prepare(resolver, numerical, capacity)?,
            ],
            capacity,
        };
        result.require_available(numerical.resources())?;
        Ok(result)
    }
    pub(crate) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
        for slot in &self.slots {
            slot.require_available(resources)?;
        }
        Ok(())
    }
    pub(in crate::tree_store) fn checkout(
        &self,
        resources: &NativePageResources,
        requested: usize,
    ) -> Result<ScratchVec<(PageNumber, Checksum)>> {
        if requested > self.capacity {
            return Err(super::source_owner_refused());
        }
        // Parent + incoming PartialBranch + sibling child builder coexist.
        // No fourth slot or replacement allocation exists on this closed path.
        for slot in &self.slots {
            if let Some(storage) = slot.try_checkout(resources)? {
                return Ok(storage);
            }
        }
        Err(super::source_owner_refused())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree_store::{NativeChecksumPool, NativeEntryBufferPool, NativeReadContext};
    use crate::{NativeNumericalPool, ReadableDatabase, TableDefinition};
    use std::sync::{Arc, Barrier};

    const CEILING: usize = 64 * 1024 * 1024;
    fn actual_fixture() -> (tempfile::NamedTempFile, crate::Database) {
        const TABLE: TableDefinition<u128, &[u8]> = TableDefinition::new("branch_child_backing");
        let file = crate::create_tempfile();
        let db = crate::Builder::new()
            .set_cache_size(0)
            .create(file.path())
            .unwrap();
        let write = db.begin_write().unwrap();
        drop(
            write
                .open_table(TABLE)
                .unwrap()
                .insert(1, b"real".as_slice())
                .unwrap(),
        );
        write.commit().unwrap();
        (file, db)
    }

    // req: STO-020, REC-001
    #[test]
    fn native_branch_children_three_slots_concurrent_hold_no_replacement_and_final_retirement() {
        let (_file, db) = actual_fixture();
        let plan = db.plan_current_read_context().unwrap();
        let numerical_pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let numerical = numerical_pool.begin_managed_epoch(CEILING).unwrap();
        let resolver = PageResolver::with_read_context(
            db.get_memory(),
            NativeReadContext::for_fixed_recipe(
                plan.fixed_cleanup_limits_for_fixture(),
                &numerical,
            )
            .unwrap(),
        );
        let resources = numerical.resources();
        let baseline = resources.current();
        let needed = NativeBranchChildrenPool::capacity_bytes(4).unwrap();
        let pool = NativeBranchChildrenPool::prepare(&resolver, &numerical, 4).unwrap();
        assert_eq!(resources.current(), baseline + needed);
        let arrived = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let pool = pool.clone();
                let resources = resources.clone();
                let arrived = arrived.clone();
                let release = release.clone();
                std::thread::spawn(move || {
                    let held = pool.checkout(&resources, 4).unwrap();
                    assert!(held.is_prepared());
                    assert_eq!(held.capacity(), 4);
                    arrived.wait();
                    release.wait();
                    drop(held);
                })
            })
            .collect();
        arrived.wait();
        let third = pool.checkout(resources, 4).unwrap();
        assert!(
            pool.checkout(resources, 4).is_err(),
            "no fourth/replacement child backing"
        );
        assert!(
            pool.checkout(resources, 5).is_err(),
            "actual demand bound is checked before checkout"
        );
        assert_eq!(
            resources.current(),
            baseline + needed,
            "no growth while all three actual slots are held"
        );
        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        drop(third);
        pool.require_available(resources).unwrap();
        let retained = pool.checkout(resources, 4).unwrap();
        drop(pool);
        assert!(
            resources.current() > baseline,
            "actual checked-out backing outlives original pool wrapper"
        );
        drop(retained);
        assert_eq!(resources.current(), baseline);
    }

    // req: STO-020, REC-001
    #[test]
    fn native_branch_entry_checksum_same_counter_stale_passed_serial_refuses_without_new_backing() {
        let (_file, db) = actual_fixture();
        let plan = db.plan_current_read_context().unwrap();
        let numerical_pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let original = numerical_pool.begin_managed_epoch(CEILING).unwrap();
        let old_resources = original.resources().clone();
        let old_resolver = PageResolver::with_read_context(
            db.get_memory(),
            NativeReadContext::for_fixed_recipe(plan.fixed_cleanup_limits_for_fixture(), &original)
                .unwrap(),
        );
        let old_branch = NativeBranchChildrenPool::prepare(&old_resolver, &original, 4).unwrap();
        let old_entry = NativeEntryBufferPool::prepare(&old_resolver, &original, 2, 64).unwrap();
        drop(original); // Genuine close; old charged arrays/context remain alive.
        let reopened = numerical_pool.begin_managed_epoch(CEILING).unwrap();
        let new_resolver = PageResolver::with_read_context(
            db.get_memory(),
            NativeReadContext::for_fixed_recipe(plan.fixed_cleanup_limits_for_fixture(), &reopened)
                .unwrap(),
        );
        let branch = NativeBranchChildrenPool::prepare(&new_resolver, &reopened, 4).unwrap();
        let entry = NativeEntryBufferPool::prepare(&new_resolver, &reopened, 2, 64).unwrap();
        let checksum = NativeChecksumPool::prepare(&new_resolver, &reopened, 1, 4).unwrap();
        let before = reopened.resources().current();
        assert!(
            branch.checkout(&old_resources, 4).is_err(),
            "new slot must refuse stale passed serial on SAME counter"
        );
        assert!(branch.require_available(&old_resources).is_err());
        assert!(entry.require_for(&old_resources).is_err());
        assert!(entry.checkout(&old_resources).is_err());
        assert!(checksum.require_available(&old_resources).is_err());
        assert!(
            old_branch.checkout(reopened.resources(), 4).is_err(),
            "old slot cannot adopt reopened epoch"
        );
        assert!(old_entry.require_for(reopened.resources()).is_err());
        let foreign_pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let foreign = foreign_pool.begin_managed_epoch(CEILING).unwrap();
        assert!(branch.checkout(foreign.resources(), 4).is_err());
        assert_eq!(
            reopened.resources().current(),
            before,
            "every refusal precedes storage removal/allocation"
        );
        branch.require_available(reopened.resources()).unwrap();
        let actual = branch.checkout(reopened.resources(), 4).unwrap();
        drop(actual);
        let actual = entry.checkout(reopened.resources()).unwrap();
        drop(actual);
        drop(branch);
        drop(entry);
        drop(checksum);
        drop(old_branch);
        drop(old_entry);
        drop(new_resolver);
        drop(old_resolver);
        assert_eq!(
            reopened.resources().current(),
            0,
            "all actual backing/control retired on original same counter"
        );
    }
}
