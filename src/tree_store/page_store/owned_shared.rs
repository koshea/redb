//! Closed physical control custody for internal native aliases.
//!
//! Ordinary aliases keep their existing raw Arc allocation and destruction.
//! Source aliases price their actual value/control allocation before creation;
//! every strong alias participates in final shell-before-value retirement.
#[cfg(not(redb_no_std))]
use super::page_resources::{
    NativeControlAllocation, NativeFixedObservationAdmission, NativePageResources,
};
use alloc::sync::Arc;
use core::ops::Deref;

pub(crate) struct NativeOwnedShared<T> {
    inner: SharedKind<T>,
}
enum SharedKind<T> {
    Ordinary(Arc<T>),
    #[cfg(not(redb_no_std))]
    Source(Option<Arc<OwnedValue<T>>>),
}
#[cfg(not(redb_no_std))]
struct OwnedValue<T> {
    // Value, including its separately guarded native buffers, dies first.
    value: T,
    _allocation: NativeControlAllocation,
}
impl<T> NativeOwnedShared<T> {
    pub(crate) fn ordinary(value: T) -> Self {
        Self {
            inner: SharedKind::Ordinary(Arc::new(value)),
        }
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn source(value: T, resources: &NativePageResources) -> crate::Result<Self> {
        Ok(Self::prepare_source_retained(resources, 0)?.finish(value))
    }
    // The typed shared header and its entire enclosed extra retained graph are
    // admitted together, before that graph is allocated. No guard/raw Arc or
    // caller-defined retirement callback leaves this closed prepared owner.
    #[cfg(not(redb_no_std))]
    pub(super) fn prepare_source_retained(
        resources: &NativePageResources,
        extra_retained_bytes: usize,
    ) -> crate::Result<NativePreparedShared<T>> {
        let bytes = Self::source_capacity_bytes()?
            .checked_add(extra_retained_bytes)
            .ok_or_else(super::page_resources::refused)?;
        Ok(NativePreparedShared {
            allocation: resources.admit_control(bytes)?,
            admission: None,
            value: core::marker::PhantomData,
        })
    }
    // Crate-private native construction only. The saved serial is checked
    // again before final construction transfer; same Counter != same epoch.
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_source_retained_current(
        resources: &NativePageResources,
        extra_retained_bytes: usize,
    ) -> crate::Result<NativePreparedShared<T>> {
        let admission = resources.seal_fixed_observation_admission()?;
        let mut prepared = Self::prepare_source_retained(resources, extra_retained_bytes)?;
        prepared.admission = Some(admission);
        Ok(prepared)
    }
    #[cfg(not(redb_no_std))]
    fn source_control_capacity_bytes()
    -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        super::read_only_resource::ReadOnlyResourceProfile::arc_allocation::<OwnedValue<T>>()
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn control_capacity_bytes(
        &self,
    ) -> core::result::Result<u64, super::allocator_reopen_facts::AllocatorResourceError> {
        match &self.inner {
            SharedKind::Ordinary(_) => {
                super::read_only_resource::ReadOnlyResourceProfile::arc_allocation::<T>()
            }
            SharedKind::Source(_) => Self::source_control_capacity_bytes(),
        }
    }
    pub(crate) fn same_allocation(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (SharedKind::Ordinary(a), SharedKind::Ordinary(b)) => Arc::ptr_eq(a, b),
            #[cfg(not(redb_no_std))]
            (SharedKind::Source(a), SharedKind::Source(b)) => Arc::ptr_eq(
                a.as_ref().expect("live closed native alias"),
                b.as_ref().expect("live closed native alias"),
            ),
            #[cfg(not(redb_no_std))]
            _ => false,
        }
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn source_capacity_bytes() -> crate::Result<usize> {
        // Sole native pinned Arc layout calculator, actual OwnedValue<T>.
        let bytes =
            Self::source_control_capacity_bytes().map_err(|_| super::page_resources::refused())?;
        usize::try_from(bytes).map_err(|_| super::page_resources::refused())
    }
    pub(crate) fn strong_count(&self) -> usize {
        match &self.inner {
            SharedKind::Ordinary(value) => Arc::strong_count(value),
            #[cfg(not(redb_no_std))]
            SharedKind::Source(value) => {
                Arc::strong_count(value.as_ref().expect("live closed native alias"))
            }
        }
    }
}
// Source-only: ordinary/NoStd shared aliases keep their original representation.
#[cfg(not(redb_no_std))]
pub(crate) struct NativePreparedShared<T> {
    allocation: NativeControlAllocation,
    // Construction-only serial custody; no change to OwnedValue<T> layout.
    admission: Option<NativeFixedObservationAdmission>,
    value: core::marker::PhantomData<fn() -> T>,
}
#[cfg(not(redb_no_std))]
impl<T> NativePreparedShared<T> {
    pub(crate) fn require_current(&self) -> crate::Result<()> {
        self.admission
            .as_ref()
            .ok_or_else(super::page_resources::refused)?
            .require_current()
    }
    pub(crate) fn finish(self, value: T) -> NativeOwnedShared<T> {
        NativeOwnedShared {
            inner: SharedKind::Source(Some(Arc::new(OwnedValue {
                value,
                _allocation: self.allocation,
            }))),
        }
    }
}
impl<T> Clone for NativeOwnedShared<T> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            SharedKind::Ordinary(value) => SharedKind::Ordinary(value.clone()),
            #[cfg(not(redb_no_std))]
            SharedKind::Source(value) => SharedKind::Source(Some(
                value.as_ref().expect("live closed native alias").clone(),
            )),
        };
        Self { inner }
    }
}
impl<T> Deref for NativeOwnedShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match &self.inner {
            SharedKind::Ordinary(value) => value,
            #[cfg(not(redb_no_std))]
            SharedKind::Source(value) => &value.as_ref().expect("live closed native alias").value,
        }
    }
}
impl<T> Drop for NativeOwnedShared<T> {
    fn drop(&mut self) {
        #[cfg(not(redb_no_std))]
        if let SharedKind::Source(value) = &mut self.inner {
            if let Some(value) = value.take() {
                // Every Source strong reference uses this protocol. The one
                // winner gets the payload only after the Arc shell is freed;
                // competing final releases cannot default-drop the raw Arc.
                drop(Arc::into_inner(value));
            }
        }
    }
}

#[cfg(all(test, not(redb_no_std)))]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread;

    // Numerical unit marker only: these tests establish native control custody,
    // never actual SourceReservation attachment or a complete write quote.
    struct DropObservation {
        resources: NativePageResources,
        drops: Arc<AtomicUsize>,
        charge_at_drop: Arc<AtomicUsize>,
    }
    impl Drop for DropObservation {
        fn drop(&mut self) {
            self.charge_at_drop
                .store(self.resources.current(), Ordering::Release);
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn observation(
        resources: &NativePageResources,
    ) -> (DropObservation, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        let charge = Arc::new(AtomicUsize::new(0));
        (
            DropObservation {
                resources: resources.clone(),
                drops: drops.clone(),
                charge_at_drop: charge.clone(),
            },
            drops,
            charge,
        )
    }

    #[test]
    fn native_source_shared_controls_survive_concurrent_final_alias_release() {
        let resources = NativePageResources::from_retained_pool_data(1024 * 1024, ()).unwrap();
        let (value, drops, charge_at_drop) = observation(&resources);
        let owner = NativeOwnedShared::source(value, &resources).unwrap();
        let expected = NativeOwnedShared::<DropObservation>::source_capacity_bytes().unwrap();
        assert_eq!(resources.current(), expected);
        let a = owner.clone();
        let b = owner.clone();
        drop(owner);
        let arrived = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for alias in [a, b] {
            let arrived = arrived.clone();
            let release = release.clone();
            workers.push(thread::spawn(move || {
                arrived.wait();
                release.wait();
                drop(alias);
            }));
        }
        arrived.wait();
        assert_eq!(resources.current(), expected);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(charge_at_drop.load(Ordering::Acquire), expected);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    fn native_source_shared_forgotten_alias_retains_control_and_blocks_unadmitted_growth() {
        let needed = NativeOwnedShared::<u64>::source_capacity_bytes().unwrap();
        let resources = NativePageResources::from_retained_pool_data(needed, ()).unwrap();
        let parent = NativeOwnedShared::source(7_u64, &resources).unwrap();
        let forgotten = parent.clone();
        core::mem::forget(forgotten);
        drop(parent);
        assert_eq!(resources.current(), needed);
        for _ in 0..4 {
            assert!(NativeOwnedShared::source(8_u64, &resources).is_err());
            assert_eq!(resources.current(), needed);
        }
        // The intentionally leaked native control also retains its numerical
        // unit marker. This is not a Source-ledger refund assertion.
    }

    #[test]
    fn native_source_shared_admission_error_drops_value_without_constructing_control() {
        let resources = NativePageResources::from_retained_pool_data(0, ()).unwrap();
        let (value, drops, charge_at_drop) = observation(&resources);
        assert!(NativeOwnedShared::source(value, &resources).is_err());
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(charge_at_drop.load(Ordering::Acquire), 0);
        assert_eq!(resources.current(), 0);
    }

    #[test]
    fn native_ordinary_shared_controls_keep_raw_arc_counts_and_need_no_source_allowance() {
        let source = NativePageResources::from_retained_pool_data(0, ()).unwrap();
        let ordinary = NativeOwnedShared::ordinary(17_u64);
        let alias = ordinary.clone();
        assert_eq!(ordinary.strong_count(), 2);
        assert_eq!(*alias, 17);
        drop(alias);
        assert_eq!(ordinary.strong_count(), 1);
        assert_eq!(source.current(), 0);
    }
}
