//! One preadmitted sparse-entry slot; numerical custody grants no tree authority.
use super::{LeafAccessor, OwnedEntryBuffer};
use crate::Result;
use crate::sync::Mutex;
use crate::tree_store::page_store::{
    NativeControlAllocation, NativeFixedObservationAdmission, NativeNumericalEpoch,
    NativePageResources,
};
use crate::tree_store::{NativeOwnedShared, PageResolver};
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::ops::Range;

type Pair = (Range<usize>, Range<usize>);

// Bare storage never owns a pool/checkout. Returning it cannot create a cycle.
pub(super) struct EntryBuffers {
    pub(super) pairs: VecDeque<Pair>,
    pub(super) data: Vec<u8>,
    pub(super) retained: Vec<Pair>,
}
struct EntrySlot {
    buffers: Option<EntryBuffers>,
    admission: NativeFixedObservationAdmission,
    // ALL three buffers, including storage moved into a checkout, retire first.
    allocation: NativeControlAllocation,
}
#[derive(Clone)]
pub(crate) struct NativeEntryBufferPool {
    owner: NativeOwnedShared<Mutex<EntrySlot>>,
}
impl NativeEntryBufferPool {
    fn buffer_bytes(pairs: usize, payload: usize) -> Result<usize> {
        use crate::tree_store::page_store::point_vector_bytes;
        let pair = point_vector_bytes::<Pair>(pairs).map_err(|_| refused())?;
        let data = point_vector_bytes::<u8>(payload).map_err(|_| refused())?;
        usize::try_from(
            pair.checked_mul(2)
                .and_then(|n| n.checked_add(data))
                .ok_or_else(refused)?,
        )
        .map_err(|_| refused())
    }
    pub(crate) fn capacity_bytes(pairs: usize, payload: usize) -> Result<usize> {
        Self::buffer_bytes(pairs, payload)?
            .checked_add(NativeOwnedShared::<Mutex<EntrySlot>>::source_capacity_bytes()?)
            .ok_or_else(refused)
    }
    pub(crate) fn prepare(
        actual: &PageResolver,
        numerical: &NativeNumericalEpoch,
        pairs: usize,
        payload: usize,
    ) -> Result<Self> {
        numerical.require_open()?;
        // Only this actual Source resolver can preadmit the storage. An equal
        // limit from another counter or an earlier managed serial cannot bind it.
        let allocation = actual.admit_fixed_observation(Self::buffer_bytes(pairs, payload)?)?;
        if !allocation.belongs_to(numerical.resources()) {
            return Err(refused());
        }
        let admission = actual.seal_fixed_observation_admission()?;
        admission.require_for(numerical.resources())?;
        // source() admits the actual Arc/OwnedValue BEFORE its Arc allocation.
        // No deque/Vec allocation has happened; all three are already admitted.
        let owner = NativeOwnedShared::source(
            Mutex::new(EntrySlot {
                buffers: None,
                admission,
                allocation,
            }),
            numerical.resources(),
        )?;
        let result = Self { owner };
        {
            let mut slot = result.owner.lock()?;
            let mut buffers = EntryBuffers {
                pairs: VecDeque::new(),
                data: Vec::new(),
                retained: Vec::new(),
            };
            // Pinned VecDeque/RawVec exact-reserve backing, same sole native
            // vector calculator. Partial construction dies before slot charge.
            buffers
                .pairs
                .try_reserve_exact(pairs)
                .map_err(|_| refused())?;
            buffers
                .data
                .try_reserve_exact(payload)
                .map_err(|_| refused())?;
            buffers
                .retained
                .try_reserve_exact(pairs)
                .map_err(|_| refused())?;
            slot.buffers = Some(buffers);
        }
        result.require_for(numerical.resources())?;
        Ok(result)
    }
    pub(crate) fn require_for(&self, resources: &NativePageResources) -> Result<()> {
        let slot = self.owner.lock()?;
        slot.admission.require_for(resources)?;
        if !resources.is_source_write() || !slot.allocation.belongs_to(resources) {
            return Err(refused());
        }
        slot.admission.require_for(resources)
    }
    // Numerical availability only: no checkout, buffer, or mutation capability
    // escapes the owning tree module. The same serial/pool checks apply.
    pub(crate) fn require_available(&self, resources: &NativePageResources) -> Result<()> {
        self.require_for(resources)?;
        let slot = self.owner.lock()?;
        slot.admission.require_for(resources)?;
        if slot.buffers.is_none() {
            return Err(refused());
        }
        Ok(())
    }
    pub(in crate::tree_store) fn checkout(
        &self,
        resources: &NativePageResources,
    ) -> Result<OwnedEntryBuffer> {
        self.require_for(resources)?;
        let buffers = {
            let mut slot = self.owner.lock()?;
            slot.admission.require_for(resources)?;
            slot.buffers.take().ok_or_else(refused)?
        };
        // An unavailable slot is never replenished. Forgotten checkout retains
        // the SAME actual backing allowance and prevents a replacement checkout.
        let checkout = SourceEntryCheckout {
            buffers: Some(buffers),
            owner: self.owner.clone(),
        };
        Ok(OwnedEntryBuffer::from_source(checkout))
    }
}

pub(super) struct SourceEntryCheckout {
    buffers: Option<EntryBuffers>,
    owner: NativeOwnedShared<Mutex<EntrySlot>>,
}
impl SourceEntryCheckout {
    pub(super) fn buffers(&self) -> &EntryBuffers {
        self.buffers.as_ref().expect("live closed entry checkout")
    }
    pub(super) fn buffers_mut(&mut self) -> &mut EntryBuffers {
        self.buffers.as_mut().expect("live closed entry checkout")
    }
    pub(super) fn require_extend(
        &self,
        accessor: &LeafAccessor<'_>,
        removed_indexes: &[usize],
    ) -> Result<()> {
        self.owner.lock()?.admission.require_current()?;
        // All failures precede copying, reserve growth, or removing temporary
        // storage. Full original payload counts even for removed values because
        // the ordinary loop reserves that complete source-leaf payload first.
        let mut previous = None;
        for &index in removed_indexes {
            if index >= accessor.num_pairs() || previous.is_some_and(|prior| prior >= index) {
                return Err(refused());
            }
            previous = Some(index);
        }
        let retained = accessor
            .num_pairs()
            .checked_sub(removed_indexes.len())
            .ok_or_else(refused)?;
        let buffers = self.buffers();
        let pairs = buffers
            .pairs
            .len()
            .checked_add(retained)
            .ok_or_else(refused)?;
        let payload = buffers
            .data
            .len()
            .checked_add(accessor.length_of_pairs(0, accessor.num_pairs()))
            .ok_or_else(refused)?;
        if pairs > buffers.pairs.capacity()
            || payload > buffers.data.capacity()
            || retained > buffers.retained.capacity()
            || !buffers.retained.is_empty()
        {
            return Err(refused());
        }
        Ok(())
    }
}
impl Drop for SourceEntryCheckout {
    fn drop(&mut self) {
        let Some(mut buffers) = self.buffers.take() else {
            return;
        };
        // Only inert ranges/bytes live here; page/value guards are owned outside
        // this buffer. Clearing retains the actual capacities and their charge.
        buffers.pairs.clear();
        buffers.data.clear();
        buffers.retained.clear();
        if let Ok(mut slot) = self.owner.lock() {
            if slot.buffers.is_none() {
                slot.buffers = Some(buffers);
                return;
            }
        }
        // A poisoned/unavailable return never allocates replacement storage.
        // Local buffers physically retire while owner still keeps their charge.
        drop(buffers);
    }
}
fn refused() -> crate::StorageError {
    crate::tree_store::source_owner_refused()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NativeNumericalPool;
    use crate::tree_store::NativeReadContext;
    use crate::{ReadableDatabase, ReadableTable, TableDefinition};
    use core::mem;

    // Numerical buffer component only: a genuine actual engine supplies the
    // Source resolver. Neither a full factory nor SYSTEM authority is minted.
    const CEILING: usize = 64 * 1024 * 1024;
    struct Fixture {
        resolver: PageResolver,
        numerical: NativeNumericalEpoch,
        _db: crate::Database,
        _file: tempfile::NamedTempFile,
    }
    fn fixture() -> Fixture {
        const TABLE: TableDefinition<u128, &[u8]> = TableDefinition::new("entry_storage_unit");
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
        {
            let read = db.begin_read().unwrap();
            drop(read.open_table(TABLE).unwrap().get(1).unwrap());
        }
        let plan = db.plan_current_read_context().unwrap();
        let limits = plan.fixed_cleanup_limits_for_fixture();
        drop(plan);
        let pool = NativeNumericalPool::from_retained_data(CEILING, ()).unwrap();
        let numerical = pool.begin_managed_epoch(CEILING).unwrap();
        let context = NativeReadContext::for_fixed_recipe(limits, &numerical).unwrap();
        let resolver = PageResolver::with_read_context(db.get_memory(), context);
        Fixture {
            resolver,
            numerical,
            _db: db,
            _file: file,
        }
    }
    fn leaf(bytes: &mut [u8; 128]) -> LeafAccessor<'_> {
        let mut builder = super::super::RawLeafBuilder::new(bytes, 2, Some(16), None, 32);
        builder.append(&1_u128.to_le_bytes(), b"ab");
        builder.append(&2_u128.to_le_bytes(), b"cde");
        drop(builder);
        LeafAccessor::new(bytes, Some(16), None)
    }

    #[test]
    // req: STO-020, REC-001
    fn native_source_entry_slot_n_minus_one_refuses_and_refunds_before_construction() {
        let fixture = fixture();
        let resources = fixture.numerical.resources();
        let needed = NativeEntryBufferPool::capacity_bytes(2, 37).unwrap();
        let baseline = resources.current();
        let pressure = fixture
            .resolver
            .admit_fixed_observation(
                CEILING
                    .checked_sub(baseline)
                    .unwrap()
                    .checked_sub(needed)
                    .unwrap()
                    + 1,
            )
            .unwrap();
        let before = resources.current();
        assert!(
            NativeEntryBufferPool::prepare(&fixture.resolver, &fixture.numerical, 2, 37).is_err()
        );
        assert_eq!(
            resources.current(),
            before,
            "partial preadmission completely refunded"
        );
        drop(pressure);
        assert_eq!(resources.current(), baseline);
        // No writer slot/transaction was claimed by numerical preparation.
        fixture._db.begin_write().unwrap().abort().unwrap();
    }

    #[test]
    // req: STO-020, REC-001
    fn native_source_entry_slot_uses_all_three_prepaid_buffers_and_refuses_before_copy() {
        let fixture = fixture();
        let resources = fixture.numerical.resources();
        let baseline = resources.current();
        let needed = NativeEntryBufferPool::capacity_bytes(2, 37).unwrap();
        let pool =
            NativeEntryBufferPool::prepare(&fixture.resolver, &fixture.numerical, 2, 37).unwrap();
        assert_eq!(resources.current(), baseline + needed);
        let mut buffer = pool.checkout(resources).unwrap();
        let capacities = {
            let storage = buffer.source.as_ref().unwrap().buffers();
            [
                storage.pairs.capacity(),
                storage.data.capacity(),
                storage.retained.capacity(),
            ]
        };
        assert_eq!(
            capacities,
            [2, 37, 2],
            "actual pinned exact-reserve backing"
        );
        let mut bytes = [0_u8; 128];
        let leaf = leaf(&mut bytes);
        buffer.extend_from_leaf(&leaf, &[0], true).unwrap();
        assert_eq!(buffer.num_pairs(), 1);
        assert_eq!(buffer.total_bytes(), 19);
        assert_eq!(
            buffer.entries().next().unwrap(),
            (2_u128.to_le_bytes().as_slice(), b"cde".as_slice())
        );
        assert!(buffer.extend_from_leaf(&leaf, &[], true).is_err());
        assert!(buffer.extend_from_leaf(&leaf, &[2], true).is_err());
        assert!(buffer.extend_from_leaf(&leaf, &[0, 0], true).is_err());
        assert!(buffer.extend_from_leaf(&leaf, &[1, 0], true).is_err());
        assert!(buffer.push_back(&[3; 16], b"x").is_err());
        assert!(buffer.push_front(&[0; 16], b"x").is_err());
        assert!(buffer.extend_from_leaf_range(&leaf, 0..1, true).is_err());
        assert_eq!(buffer.num_pairs(), 1, "all refusals occur before copying");
        assert_eq!(buffer.total_bytes(), 19);
        let storage = buffer.source.as_ref().unwrap().buffers();
        assert_eq!(
            [
                storage.pairs.capacity(),
                storage.data.capacity(),
                storage.retained.capacity()
            ],
            capacities
        );
        assert_eq!(resources.current(), baseline + needed);
        drop(buffer);
        assert_eq!(
            resources.current(),
            baseline + needed,
            "returned backing still physically owned"
        );
        let reused = pool.checkout(resources).unwrap();
        assert_eq!(reused.num_pairs(), 0);
        assert_eq!(reused.total_bytes(), 0);
        drop(reused);
        drop(pool);
        assert_eq!(resources.current(), baseline);

        // The same sparse path prepends in reverse while keeping canonical
        // order and the original deque/temporary backing; no Source InsertRun.
        let pool =
            NativeEntryBufferPool::prepare(&fixture.resolver, &fixture.numerical, 2, 74).unwrap();
        let mut buffer = pool.checkout(resources).unwrap();
        buffer.extend_from_leaf(&leaf, &[0], true).unwrap();
        buffer.extend_from_leaf(&leaf, &[1], false).unwrap();
        let mut entries = buffer.entries();
        assert_eq!(
            entries.next().unwrap(),
            (1_u128.to_le_bytes().as_slice(), b"ab".as_slice())
        );
        assert_eq!(
            entries.next().unwrap(),
            (2_u128.to_le_bytes().as_slice(), b"cde".as_slice())
        );
        assert!(entries.next().is_none());
        drop(entries);
        let storage = buffer.source.as_ref().unwrap().buffers();
        assert_eq!(
            [
                storage.pairs.capacity(),
                storage.data.capacity(),
                storage.retained.capacity()
            ],
            [2, 74, 2]
        );
        drop(buffer);
        drop(pool);
        assert_eq!(resources.current(), baseline);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_source_entry_checkout_outlives_pool_and_failed_checkout_never_replaces_it() {
        let fixture = fixture();
        let resources = fixture.numerical.resources();
        let baseline = resources.current();
        let needed = NativeEntryBufferPool::capacity_bytes(2, 37).unwrap();
        let pool =
            NativeEntryBufferPool::prepare(&fixture.resolver, &fixture.numerical, 2, 37).unwrap();
        let checkout = pool.checkout(resources).unwrap();
        assert!(pool.checkout(resources).is_err());
        assert_eq!(resources.current(), baseline + needed);
        drop(pool);
        assert_eq!(
            resources.current(),
            baseline + needed,
            "live checkout owns all backing and shell"
        );
        drop(checkout);
        assert_eq!(
            resources.current(),
            baseline,
            "final backing before final charge retirement"
        );
    }

    #[test]
    // req: STO-020, REC-001
    fn native_source_entry_concurrent_checkout_refuses_until_actual_owner_returns_storage() {
        let fixture = fixture();
        let resources = fixture.numerical.resources();
        let baseline = resources.current();
        let needed = NativeEntryBufferPool::capacity_bytes(2, 37).unwrap();
        let pool =
            NativeEntryBufferPool::prepare(&fixture.resolver, &fixture.numerical, 2, 37).unwrap();
        let arrived = std::sync::Barrier::new(2);
        let release = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let alias = pool.clone();
            let arrived = &arrived;
            let release = &release;
            let worker = scope.spawn(move || {
                let checkout = alias.checkout(resources).unwrap();
                arrived.wait();
                release.wait();
                drop(checkout);
            });
            arrived.wait();
            let refused_while_held = pool.checkout(resources).is_err();
            let charged_while_held = resources.current();
            release.wait();
            worker.join().unwrap();
            assert!(refused_while_held);
            assert_eq!(charged_while_held, baseline + needed);
        });
        let checkout = pool.checkout(resources).unwrap();
        drop(pool);
        assert_eq!(resources.current(), baseline + needed);
        drop(checkout);
        assert_eq!(resources.current(), baseline);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_source_entry_preparation_refuses_foreign_original_counter_without_allocation() {
        let actual = fixture();
        let foreign = fixture();
        let before_actual = actual.numerical.resources().current();
        let before_foreign = foreign.numerical.resources().current();
        assert!(
            NativeEntryBufferPool::prepare(&actual.resolver, &foreign.numerical, 2, 37).is_err()
        );
        assert_eq!(actual.numerical.resources().current(), before_actual);
        assert_eq!(foreign.numerical.resources().current(), before_foreign);
    }

    #[test]
    // req: STO-020, REC-001
    fn native_source_entry_forgotten_checkout_retains_charge_and_closed_epoch_refuses_new_use() {
        let fixture = fixture();
        let Fixture {
            resolver,
            numerical,
            _db: db,
            _file: file,
        } = fixture;
        let resources = numerical.resources().clone();
        let baseline = resources.current();
        let needed = NativeEntryBufferPool::capacity_bytes(2, 37).unwrap();
        let pool = NativeEntryBufferPool::prepare(&resolver, &numerical, 2, 37).unwrap();
        let checkout = pool.checkout(&resources).unwrap();
        mem::forget(checkout);
        assert!(pool.checkout(&resources).is_err());
        assert_eq!(resources.current(), baseline + needed);
        drop(numerical);
        assert!(
            pool.require_for(&resources).is_err(),
            "original serial cannot be reused after scope closes"
        );
        drop(pool);
        assert_eq!(
            resources.current(),
            baseline + needed,
            "forgotten storage cannot refund or replenish"
        );
        drop(resolver);
        assert_eq!(
            resources.current(),
            needed,
            "only intentional leaked backing/owner remains"
        );
        drop(db);
        drop(file);
    }
}
