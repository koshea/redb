//! Numerical native page custody. This component is not a complete resource plan.
//!
//! No Source/engine authority or caller-defined callback is stored here. A complete
//! native plan must admit the counter/control allocation and every other graph
//! before enabling this owner outside tests.
#[cfg(not(redb_no_std))]
use crate::io;
use crate::{Result, StorageError};
use alloc::{boxed::Box, sync::Arc};
use core::mem::{align_of, size_of};
use core::sync::atomic::{AtomicUsize, Ordering};

// Opaque, single-owner numerical custody. The only erased implementation is
// defined here: callers cannot provide a retirement callback or get a raw owner.
// The future Source constructor must supply an audited closed numerical owner,
// not a raw Arc, engine reference or application value. Send + Sync alone is not
// proof of that data-only condition, nor does this carrier mint Source admission.
pub(crate) struct NativeNumericalRetainer(Option<Box<dyn NativeRetirement>>);

trait NativeRetirement: Send + Sync {
    fn retire(self: Box<Self>);
}

struct NativeCarrier<T> {
    value: Option<T>,
}

impl<T: Send + Sync + 'static> NativeRetirement for NativeCarrier<T> {
    fn retire(mut self: Box<Self>) {
        let value = self.value.take();
        // Explicitly free this concrete erased allocation before the numerical
        // value can refund it. Moving value out of *self alone would not order
        // the partially-moved Box's deallocation before value destruction.
        drop(self);
        drop(value);
    }
}

impl NativeNumericalRetainer {
    pub(crate) fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self(Some(Box::new(NativeCarrier { value: Some(value) })))
    }

    fn capacity_bytes<T>()
    -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        super::container_bounds::allocation(
            size_of::<NativeCarrier<T>>(),
            align_of::<NativeCarrier<T>>(),
        )
        .map(|bound| bound.allocator_bytes)
        .map_err(|_| super::allocator_reopen_facts::AllocatorResourceError::Overflow)
    }
}

impl Drop for NativeNumericalRetainer {
    fn drop(&mut self) {
        if let Some(carrier) = self.0.take() {
            carrier.retire();
        }
    }
}

#[derive(Clone)]
pub(crate) struct NativePageResources(SharedCounter, Option<usize>);

// Serial state is inline and priced by the actual Counter control quote. It
// contains no Engine, Source ledger, caller callback or auxiliary allocation.
#[cfg(not(redb_no_std))]
struct ManagedAdmission {
    serial: usize,
    open: bool,
    ceiling: usize,
}
#[cfg(not(redb_no_std))]
impl ManagedAdmission {
    fn initial(ceiling: usize) -> Self {
        Self {
            serial: 0,
            open: false,
            ceiling,
        }
    }
}

// An internal admission owner, NOT a public writer or complete-plan proof.
// Its only production caller will be the consuming complete fixed executor.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeNumericalEpoch {
    resources: NativePageResources,
    closed: bool,
}
#[cfg(not(redb_no_std))]
pub(crate) struct NativeClosedNumericalEpoch {
    resources: NativePageResources,
    serial: usize,
    admitted_upper: usize,
}
#[cfg(not(redb_no_std))]
impl NativeNumericalEpoch {
    pub(crate) fn resources(&self) -> &NativePageResources {
        &self.resources
    }
    pub(crate) fn require_open(&self) -> Result<()> {
        let counter = self.resources.0.get();
        let serial = self.resources.1.ok_or_else(refused)?;
        let state = counter.admission.lock().map_err(|_| refused())?;
        if self.closed || !state.open || state.serial != serial {
            return Err(refused());
        }
        Ok(())
    }
    pub(crate) fn close(mut self) -> Result<NativeClosedNumericalEpoch> {
        let counter = self.resources.0.get();
        let serial = self.resources.1.ok_or_else(refused)?;
        let admitted_upper = {
            let mut state = counter.admission.lock().map_err(|_| refused())?;
            if !state.open || state.serial != serial {
                return Err(refused());
            }
            // The same mutex surrounds every successful admission CAS, so no
            // old context can increase current after this close linearizes.
            state.open = false;
            let upper = counter.current.load(Ordering::Acquire);
            state.ceiling = upper;
            upper
        };
        self.closed = true;
        Ok(NativeClosedNumericalEpoch {
            resources: self.resources.clone(),
            serial,
            admitted_upper,
        })
    }
}
#[cfg(not(redb_no_std))]
impl Drop for NativeNumericalEpoch {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        // Cancellation disables admissions but never mints a settlement/refund
        // proof. The real numerical T stays with this Counter and its guards.
        let counter = self.resources.0.get();
        let Ok(mut state) = counter.admission.lock() else {
            return;
        };
        if self.resources.1 == Some(state.serial) && state.open {
            state.open = false;
            state.ceiling = counter.current.load(Ordering::Acquire);
        }
    }
}
#[cfg(not(redb_no_std))]
impl NativeClosedNumericalEpoch {
    // This is only admission accounting. The final factory must JOIN the full
    // fifteen-family guarded-owner census, actual root/recipe/profile, all
    // prepared work/IO/error destruction, and exact pool identity BEFORE
    // turning it into any first-party trim token. A current snapshot alone is
    // deliberately not exposed as a public retained physical plan.
    pub(crate) fn require_still_closed_for(
        &self,
        resources: &NativePageResources,
    ) -> Result<usize> {
        if !self.resources.same_pool(resources) {
            return Err(refused());
        }
        let counter = self.resources.0.get();
        let state = counter.admission.lock().map_err(|_| refused())?;
        if state.open
            || state.serial != self.serial
            || counter.current.load(Ordering::Acquire) > self.admitted_upper
        {
            return Err(refused());
        }
        Ok(self.admitted_upper)
    }
}

// Every strong Counter reference uses this owner. into_inner deallocates the
// final Arc control before the returned Counter destroys its numerical retainer.
// There are no Weak references or raw Arc clones in the production graph.
#[derive(Clone)]
struct SharedCounter(Option<Arc<Counter>>);

impl SharedCounter {
    fn new(counter: Counter) -> Self {
        Self(Some(Arc::new(counter)))
    }

    fn get(&self) -> &Counter {
        self.0.as_deref().expect("live native numerical counter")
    }

    #[cfg(not(redb_no_std))]
    fn try_into_unique(mut self) -> core::result::Result<Counter, ()> {
        let counter = self.0.take().expect("live native numerical counter");
        match Arc::try_unwrap(counter) {
            Ok(counter) => Ok(counter),
            Err(counter) => {
                // A competing final drop must still use the into_inner protocol.
                drop(Arc::into_inner(counter));
                Err(())
            }
        }
    }
}

impl Drop for SharedCounter {
    fn drop(&mut self) {
        if let Some(counter) = self.0.take() {
            drop(Arc::into_inner(counter));
        }
    }
}
struct Counter {
    limit: usize,
    no_global_cache: bool,
    current: AtomicUsize,
    #[cfg(not(redb_no_std))]
    admission: crate::sync::Mutex<ManagedAdmission>,
    #[cfg(all(test, not(redb_no_std)))]
    admitted_high_water: AtomicUsize,
    // Single-threaded deterministic fixture observation, never admission.
    // [current payload bytes, peak payload bytes, current buffers, peak buffers]
    #[cfg(all(test, not(redb_no_std)))]
    materialized: [AtomicUsize; 4],
    // Deterministic single-owner diagnostic only. These independent atomics
    // are NOT a concurrently coherent snapshot and never change admission.
    // Actual control quotes use size_of::<Counter>, including cfgtest fields.
    #[cfg(all(test, not(redb_no_std)))]
    read_refusal: [AtomicUsize; 4],
    // Fixed, owning cfgtest evidence only; no admissions, callbacks or heap trace.
    // The private address is compared internally and never returned/logged.
    // Concrete Counter size/control quotes include this mutex and five rows.
    #[cfg(all(test, not(redb_no_std)))]
    fixed_phases: std::sync::Mutex<(Option<usize>, [[u64; 11]; 5], u8)>,
    // Actual checked-writer child backing: builds, unguarded, transfers, unguarded transfers.
    #[cfg(all(test, not(redb_no_std)))]
    branch_children_observation: [AtomicUsize; 4],
    // Actual checked-writer key backing: builds/unowned, into_parts/unowned,
    // owned split separators/unowned. Inline diagnostic only, never admission.
    // Every owning Counter quote includes these cfgtest atomics via sizeof.
    #[cfg(all(test, not(redb_no_std)))]
    branch_key_observation: [AtomicUsize; 6],
    // Already-admitted numerical ownership; only our private destruction runs.
    _retainer: NativeNumericalRetainer,
    #[cfg(all(
        test,
        not(redb_no_std),
        not(feature = "logging"),
        not(feature = "experimental-api-5")
    ))]
    final_release_pause: std::sync::Mutex<Option<FinalReleasePause>>,
}
#[cfg(all(
    test,
    not(redb_no_std),
    not(feature = "logging"),
    not(feature = "experimental-api-5")
))]
struct FinalReleasePause {
    arrived: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}
pub(crate) struct NativePageAllocation {
    counter: SharedCounter,
    bytes: usize,
    payload_bytes: usize,
    #[cfg(all(test, not(redb_no_std)))]
    materialized: bool,
}

// Closed numerical admission identity for one actual fixed observation.
// No resource getter, admission method, native owner or mutation capability.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeFixedObservationAdmission {
    resources: NativePageResources,
}
#[cfg(not(redb_no_std))]
impl NativeFixedObservationAdmission {
    pub(crate) fn require_for(&self, resources: &NativePageResources) -> Result<()> {
        if !self.resources.same_pool(resources) || self.resources.1 != resources.1 {
            return Err(refused());
        }
        self.require_current()
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        let counter = self.resources.0.get();
        let serial = self.resources.1.ok_or_else(refused)?;
        let state = counter.admission.lock().map_err(|_| refused())?;
        if !state.open || state.serial != serial {
            return Err(refused());
        }
        Ok(())
    }
}

// Numerical control ownership on the EXISTING counter, not a second family.
#[cfg(not(redb_no_std))]
pub(crate) struct NativeControlAllocation {
    counter: SharedCounter,
    bytes: usize,
}
#[cfg(all(test, debug_assertions, not(redb_no_std)))]
impl NativeControlAllocation {
    pub(super) fn reserved_bytes(&self) -> usize {
        self.bytes
    }
}
#[cfg(not(redb_no_std))]
impl NativeControlAllocation {
    pub(super) fn same_pool(&self, other: &Self) -> bool {
        match (&self.counter.0, &other.counter.0) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    pub(crate) fn belongs_to(&self, resources: &NativePageResources) -> bool {
        match (&self.counter.0, &resources.0.0) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}
#[cfg(not(redb_no_std))]
impl Drop for NativeControlAllocation {
    fn drop(&mut self) {
        let before = self
            .counter
            .get()
            .current
            .fetch_sub(self.bytes, Ordering::AcqRel);
        assert!(before >= self.bytes);
    }
}
impl NativePageResources {
    // Closed numerical-only wrapper support. This does not authorize native
    // allocation or mutation and does not acquire first-party admission.
    #[cfg(not(redb_no_std))]
    pub(crate) fn retained_pool_control_capacity_bytes<T: Send + Sync + 'static>()
    -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        super::resource_build_profile::NativeResourceBuildProfile::require()
            .map_err(|_| super::allocator_reopen_facts::AllocatorResourceError::Unsupported)?;
        Self::fixed_write_control_capacity_bytes::<T>()
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn from_retained_pool_data<T: Send + Sync + 'static>(
        limit: usize,
        value: T,
    ) -> core::result::Result<Self, super::allocator_reopen_facts::AllocatorResourceError> {
        let _ = Self::retained_pool_control_capacity_bytes::<T>()?;
        Ok(Self::for_fixed_write(
            limit,
            NativeNumericalRetainer::new(value),
        ))
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn record_read_refusal_for_test(
        &self,
        kind: usize,
        physical: usize,
        first: usize,
        second: usize,
    ) {
        // One deterministic synchronous fixture reads this only AFTER its own
        // operation returns. No caller may infer coherence under concurrency.
        let fields = &self.0.get().read_refusal;
        fields[1].store(physical, Ordering::Relaxed);
        fields[2].store(first, Ordering::Relaxed);
        fields[3].store(second, Ordering::Relaxed);
        fields[0].store(kind, Ordering::Release);
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn read_refusal_for_test(&self) -> (usize, usize, usize, usize) {
        let fields = &self.0.get().read_refusal;
        let kind = fields[0].load(Ordering::Acquire);
        (
            kind,
            fields[1].load(Ordering::Relaxed),
            fields[2].load(Ordering::Relaxed),
            fields[3].load(Ordering::Relaxed),
        )
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn numerical(limit: usize) -> Self {
        Self(
            SharedCounter::new(Counter {
                limit,
                no_global_cache: false,
                current: AtomicUsize::new(0),
                #[cfg(not(redb_no_std))]
                admission: crate::sync::Mutex::new(ManagedAdmission::initial(limit)),
                #[cfg(all(test, not(redb_no_std)))]
                admitted_high_water: AtomicUsize::new(0),
                #[cfg(all(test, not(redb_no_std)))]
                materialized: [const { AtomicUsize::new(0) }; 4],
                #[cfg(all(test, not(redb_no_std)))]
                read_refusal: [const { AtomicUsize::new(0) }; 4],
                #[cfg(all(test, not(redb_no_std)))]
                fixed_phases: std::sync::Mutex::new((None, [[0; 11]; 5], 0)),
                #[cfg(all(test, not(redb_no_std)))]
                branch_children_observation: core::array::from_fn(|_| AtomicUsize::new(0)),
                #[cfg(all(test, not(redb_no_std)))]
                branch_key_observation: core::array::from_fn(|_| AtomicUsize::new(0)),
                _retainer: NativeNumericalRetainer::new(()),
                #[cfg(all(
                    test,
                    not(redb_no_std),
                    not(feature = "logging"),
                    not(feature = "experimental-api-5")
                ))]
                final_release_pause: std::sync::Mutex::new(None),
            }),
            None,
        )
    }
    // Consumed only by the future complete fixed-write constructor after its
    // exact peak is admitted. The retainer is data-only, never engine authority.
    #[cfg(not(redb_no_std))]
    pub(crate) fn for_fixed_write(limit: usize, retainer: NativeNumericalRetainer) -> Self {
        Self(
            SharedCounter::new(Counter {
                limit,
                no_global_cache: true,
                current: AtomicUsize::new(0),
                #[cfg(not(redb_no_std))]
                admission: crate::sync::Mutex::new(ManagedAdmission::initial(limit)),
                #[cfg(all(test, not(redb_no_std)))]
                admitted_high_water: AtomicUsize::new(0),
                #[cfg(all(test, not(redb_no_std)))]
                materialized: [const { AtomicUsize::new(0) }; 4],
                #[cfg(all(test, not(redb_no_std)))]
                read_refusal: [const { AtomicUsize::new(0) }; 4],
                #[cfg(all(test, not(redb_no_std)))]
                fixed_phases: std::sync::Mutex::new((None, [[0; 11]; 5], 0)),
                #[cfg(all(test, not(redb_no_std)))]
                branch_children_observation: core::array::from_fn(|_| AtomicUsize::new(0)),
                #[cfg(all(test, not(redb_no_std)))]
                branch_key_observation: core::array::from_fn(|_| AtomicUsize::new(0)),
                _retainer: retainer,
                #[cfg(all(test, not(feature = "logging"), not(feature = "experimental-api-5")))]
                final_release_pause: std::sync::Mutex::new(None),
            }),
            None,
        )
    }
    pub(crate) fn is_source_write(&self) -> bool {
        self.0.get().no_global_cache
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn seal_fixed_observation_admission(
        &self,
    ) -> Result<NativeFixedObservationAdmission> {
        if !self.is_source_write() {
            return Err(refused());
        }
        let result = NativeFixedObservationAdmission {
            resources: self.clone(),
        };
        result.require_current()?;
        Ok(result)
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn current(&self) -> usize {
        self.0.get().current.load(Ordering::Acquire)
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn admitted_high_water_for_test(&self) -> usize {
        self.0.get().admitted_high_water.load(Ordering::Acquire)
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn observe_branch_children_for_test(&self, prepared: bool, transfer: bool) {
        let offset = if transfer { 2 } else { 0 };
        self.0.get().branch_children_observation[offset].fetch_add(1, Ordering::AcqRel);
        if !prepared {
            self.0.get().branch_children_observation[offset + 1].fetch_add(1, Ordering::AcqRel);
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn branch_children_observation_for_test(&self) -> [usize; 4] {
        core::array::from_fn(|i| {
            self.0.get().branch_children_observation[i].load(Ordering::Acquire)
        })
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn observe_branch_keys_for_test(&self, prepared: bool, transfer: bool) {
        let offset = if transfer { 2 } else { 0 };
        self.0.get().branch_key_observation[offset].fetch_add(1, Ordering::AcqRel);
        if !prepared {
            self.0.get().branch_key_observation[offset + 1].fetch_add(1, Ordering::AcqRel);
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn observe_split_key_for_test(&self, prepared: bool) {
        self.0.get().branch_key_observation[4].fetch_add(1, Ordering::AcqRel);
        if !prepared {
            self.0.get().branch_key_observation[5].fetch_add(1, Ordering::AcqRel);
        }
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn branch_key_observation_for_test(&self) -> [usize; 6] {
        core::array::from_fn(|i| self.0.get().branch_key_observation[i].load(Ordering::Acquire))
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn materialized_observation_for_test(&self) -> (usize, usize, usize, usize) {
        let fields = &self.0.get().materialized;
        (
            fields[0].load(Ordering::Acquire),
            fields[1].load(Ordering::Acquire),
            fields[2].load(Ordering::Acquire),
            fields[3].load(Ordering::Acquire),
        )
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn record_fixed_phase_for_test(
        &self,
        phase: usize,
        budget_address: usize,
        row: [u64; 10],
    ) {
        let mut trace = self.0.get().fixed_phases.lock().unwrap();
        let original = *trace.0.get_or_insert(budget_address);
        trace.1[phase][..10].copy_from_slice(&row);
        trace.1[phase][10] = u64::from(original == budget_address);
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn record_fixed_phase_fault_for_test(&self, phase: u8) {
        let mut trace = self.0.get().fixed_phases.lock().unwrap();
        assert_eq!(trace.2, 0);
        trace.2 = phase;
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn fixed_phase_fault_for_test(&self) -> u8 {
        self.0.get().fixed_phases.lock().unwrap().2
    }
    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn fixed_phases_for_test(&self) -> [[u64; 11]; 5] {
        self.0.get().fixed_phases.lock().unwrap().1
    }

    #[cfg(all(test, not(redb_no_std)))]
    pub(crate) fn page_charge_for_test(bytes: usize) -> Result<usize> {
        Self::page_allocation_bound(bytes)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn control_capacity_bytes()
    -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        Self::fixed_write_control_capacity_bytes::<()>()
    }
    // Quote the concrete incoming numerical value BEFORE either allocation.
    // The complete Source factory must use its actual type, never the unit
    // quote used by the non-Source numerical constructor.
    #[cfg(not(redb_no_std))]
    pub(crate) fn fixed_write_control_capacity_bytes<T>()
    -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        use super::read_only_resource::ReadOnlyResourceProfile;
        ReadOnlyResourceProfile::arc_allocation::<Counter>()?
            .checked_add(NativeNumericalRetainer::capacity_bytes::<T>()?)
            .ok_or(super::allocator_reopen_facts::AllocatorResourceError::Overflow)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn try_into_drained(
        self,
    ) -> core::result::Result<(), super::allocator_reopen_facts::AllocatorResourceError> {
        use super::allocator_reopen_facts::AllocatorResourceError as E;
        if self.current() != 0 {
            return Err(E::Unsupported);
        }
        let counter = self.0.try_into_unique().map_err(|_| E::Unsupported)?;
        if counter.current.load(Ordering::Acquire) != 0 {
            return Err(E::Unsupported);
        }
        // Arc::try_unwrap frees its actual Arc control first. The returned
        // counter and its unique numerical marker then physically drop here.
        drop(counter);
        Ok(())
    }
    #[cfg(all(
        test,
        not(redb_no_std),
        not(feature = "logging"),
        not(feature = "experimental-api-5")
    ))]
    pub(crate) fn pause_final_release_for_test(
        &self,
        arrived: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
    ) {
        *self.0.get().final_release_pause.lock().unwrap() =
            Some(FinalReleasePause { arrived, resume });
    }
    // Same conservative allocator-envelope convention used by the existing
    // first-party owned-capacity inspector. This is per physical allocation,
    // not a multiplier that substitutes for the native graph inventory.
    fn allocation_bound(bytes: usize, alignment: usize) -> Result<usize> {
        bytes
            .checked_mul(2)
            .and_then(|n| alignment.checked_mul(2).and_then(|a| n.checked_add(a)))
            .and_then(|n| n.checked_add(256))
            .ok_or_else(refused)
    }
    pub(super) fn page_allocation_bound(bytes: usize) -> Result<usize> {
        let payload = bytes
            .checked_add(2 * size_of::<usize>())
            .ok_or_else(refused)?;
        let payload = Self::allocation_bound(payload, align_of::<usize>())?;
        let owner = size_of::<NativePageAllocation>()
            .checked_add(2 * size_of::<usize>())
            .ok_or_else(refused)?;
        payload
            .checked_add(Self::allocation_bound(
                owner,
                align_of::<NativePageAllocation>(),
            )?)
            .ok_or_else(refused)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn begin_managed_epoch(
        &self,
        fully_admitted_ceiling: usize,
    ) -> Result<NativeNumericalEpoch> {
        if !self.is_source_write() {
            return Err(refused());
        }
        let counter = self.0.get();
        let serial = {
            let mut state = counter.admission.lock().map_err(|_| refused())?;
            if state.open || fully_admitted_ceiling < counter.current.load(Ordering::Acquire) {
                return Err(refused());
            }
            let serial = state.serial.checked_add(1).ok_or_else(refused)?;
            state.serial = serial;
            state.ceiling = fully_admitted_ceiling;
            state.open = true;
            serial
        };
        Ok(NativeNumericalEpoch {
            resources: Self(self.0.clone(), Some(serial)),
            closed: false,
        })
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn same_pool(&self, other: &Self) -> bool {
        match (&self.0.0, &other.0.0) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
    fn reserve_amount(&self, bytes: usize, _physical: usize) -> Result<()> {
        // Keep this bounded native mutex through the admission CAS. Checking
        // a serial and dropping its lock before CAS would allow a close race.
        #[cfg(not(redb_no_std))]
        let state = self.0.get().admission.lock().map_err(|_| refused())?;
        #[cfg(not(redb_no_std))]
        let limit = match self.1 {
            Some(serial) if state.open && state.serial == serial => state.ceiling,
            None if state.serial == 0 => self.0.get().limit,
            _ => return Err(refused()),
        };
        #[cfg(redb_no_std)]
        let limit = self.0.get().limit;
        let mut current = self.0.get().current.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .filter(|next| *next <= limit)
                .ok_or_else(|| {
                    #[cfg(all(test, not(redb_no_std)))]
                    self.record_read_refusal_for_test(
                        4,
                        _physical,
                        bytes,
                        limit.saturating_sub(current),
                    );
                    refused()
                })?;
            match self.0.get().current.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Includes admitted-but-unused tokens and metadata controls,
                    // not only materialized payloads. Never reset while live.
                    #[cfg(all(test, not(redb_no_std)))]
                    self.0
                        .get()
                        .admitted_high_water
                        .fetch_max(next, Ordering::AcqRel);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }
    #[cfg(not(redb_no_std))]
    pub(super) fn admit_control(&self, bytes: usize) -> Result<NativeControlAllocation> {
        self.reserve_amount(bytes, 0)?;
        Ok(NativeControlAllocation {
            counter: self.0.clone(),
            bytes,
        })
    }
    pub(super) fn admit_page(&self, bytes: usize) -> Result<NativePageAllocation> {
        let payload_bytes = bytes;
        let bytes = Self::page_allocation_bound(bytes)?;
        self.reserve_amount(bytes, payload_bytes)?;
        Ok(NativePageAllocation {
            counter: self.0.clone(),
            bytes,
            payload_bytes,
            #[cfg(all(test, not(redb_no_std)))]
            materialized: false,
        })
    }
    #[cfg(test)]
    pub(super) fn for_test<T: Send + Sync + 'static>(limit: usize, retainer: Arc<T>) -> Self {
        Self(
            SharedCounter::new(Counter {
                limit,
                no_global_cache: false,
                current: AtomicUsize::new(0),
                #[cfg(not(redb_no_std))]
                admission: crate::sync::Mutex::new(ManagedAdmission::initial(limit)),
                #[cfg(all(test, not(redb_no_std)))]
                admitted_high_water: AtomicUsize::new(0),
                #[cfg(all(test, not(redb_no_std)))]
                materialized: [const { AtomicUsize::new(0) }; 4],
                #[cfg(all(test, not(redb_no_std)))]
                read_refusal: [const { AtomicUsize::new(0) }; 4],
                #[cfg(all(test, not(redb_no_std)))]
                fixed_phases: std::sync::Mutex::new((None, [[0; 11]; 5], 0)),
                #[cfg(all(test, not(redb_no_std)))]
                branch_children_observation: core::array::from_fn(|_| AtomicUsize::new(0)),
                #[cfg(all(test, not(redb_no_std)))]
                branch_key_observation: core::array::from_fn(|_| AtomicUsize::new(0)),
                // Existing test-only Arc markers retain their original semantics.
                // They are not admissible first-party Source numerical owners.
                _retainer: NativeNumericalRetainer::new(retainer),
                #[cfg(all(
                    test,
                    not(redb_no_std),
                    not(feature = "logging"),
                    not(feature = "experimental-api-5")
                ))]
                final_release_pause: std::sync::Mutex::new(None),
            }),
            None,
        )
    }
    #[cfg(test)]
    pub(super) fn current_for_test(&self) -> usize {
        self.0.get().current.load(Ordering::Acquire)
    }
}
impl NativePageAllocation {
    // Clone only the existing closed numerical owner; no allocation or new pool.
    pub(super) fn resources(&self) -> NativePageResources {
        // Physical custody does not grant an admission serial. Once a managed
        // epoch exists, this unscoped context cannot admit any new allocation.
        NativePageResources(self.counter.clone(), None)
    }
    // Called once by the sole admitted byte-buffer constructor, after actual
    // bytes exist. Fresh unused admission tokens never reach this marker.
    // Only fixed-write no-global-cache buffers participate; legacy cached
    // ordinary allocations are not reattributed by a numerical observer.
    #[cfg(all(test, not(redb_no_std)))]
    pub(super) fn record_materialized_for_test(&mut self) {
        if !self.counter.get().no_global_cache {
            return;
        }
        assert!(!self.materialized);
        self.materialized = true;
        let fields = &self.counter.get().materialized;
        let before = fields[0].fetch_add(self.payload_bytes, Ordering::AcqRel);
        let bytes = before
            .checked_add(self.payload_bytes)
            .expect("admitted payload sum");
        fields[1].fetch_max(bytes, Ordering::AcqRel);
        let before = fields[2].fetch_add(1, Ordering::AcqRel);
        let buffers = before.checked_add(1).expect("admitted buffer count");
        fields[3].fetch_max(buffers, Ordering::AcqRel);
    }
    pub(super) fn forbids_global_cache(&self) -> bool {
        self.counter.get().no_global_cache
    }
    pub(super) fn same_pool(&self, other: &Self) -> bool {
        match (&self.counter.0, &other.counter.0) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
    pub(super) fn validate_payload_length(&self, bytes: usize) -> Result<()> {
        if self.payload_bytes != bytes
            || NativePageResources::page_allocation_bound(bytes)? != self.bytes
        {
            return Err(refused());
        }
        Ok(())
    }
}
impl Drop for NativePageAllocation {
    fn drop(&mut self) {
        // SharedPageBytes drops its actual bytes Arc first, then the closed
        // SharedPageAllocation into_inner frees that Arc control before this
        // returned allocation is destroyed. Never decrement on logical free.
        #[cfg(all(test, not(redb_no_std)))]
        if self.materialized {
            let fields = &self.counter.get().materialized;
            let before = fields[0].fetch_sub(self.payload_bytes, Ordering::AcqRel);
            assert!(before >= self.payload_bytes);
            let before = fields[2].fetch_sub(1, Ordering::AcqRel);
            assert!(before >= 1);
        }
        let before = self
            .counter
            .get()
            .current
            .fetch_sub(self.bytes, Ordering::AcqRel);
        #[cfg(all(
            test,
            not(redb_no_std),
            not(feature = "logging"),
            not(feature = "experimental-api-5")
        ))]
        if before == self.bytes {
            let pause = self
                .counter
                .get()
                .final_release_pause
                .lock()
                .unwrap()
                .take();
            if let Some(pause) = pause {
                pause.arrived.wait();
                pause.resume.wait();
            }
        }
        #[cfg(not(all(
            test,
            not(redb_no_std),
            not(feature = "logging"),
            not(feature = "experimental-api-5")
        )))]
        let _ = before;
    }
}
pub(super) fn refused() -> StorageError {
    #[cfg(not(redb_no_std))]
    {
        StorageError::Io(io::Error::from(std::io::ErrorKind::OutOfMemory))
    }
    #[cfg(redb_no_std)]
    {
        StorageError::Unsupported
    }
}
