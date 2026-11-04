//! Numerical data custody only; no native read/write/plan/engine capability.
use crate::tree_store::{AllocatorResourceError, NativePageResources};

/// A numerical-only shared owner for the closed native resource integration.
///
/// Construction requires preadmission of the concrete control quote and an
/// audited data-only numerical value. Send+Sync alone is not that audit. There
/// is no method to obtain a page, engine, writer or resource plan from this.
#[doc(hidden)]
#[derive(Clone)]
pub struct NativeNumericalPool(NativePageResources);
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeNumericalPoolError {
    Overflow,
    UnsupportedProfile,
}
impl core::fmt::Display for NativeNumericalPoolError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Overflow => "native numerical capacity overflow",
            Self::UnsupportedProfile => "unsupported native numerical profile",
        })
    }
}
impl std::error::Error for NativeNumericalPoolError {}
fn map_error(error: AllocatorResourceError) -> NativeNumericalPoolError {
    match error {
        AllocatorResourceError::Overflow => NativeNumericalPoolError::Overflow,
        _ => NativeNumericalPoolError::UnsupportedProfile,
    }
}
impl NativeNumericalPool {
    pub fn control_capacity_bytes<T: Send + Sync + 'static>()
    -> Result<u64, NativeNumericalPoolError> {
        NativePageResources::retained_pool_control_capacity_bytes::<T>().map_err(map_error)
    }
    pub fn from_retained_data<T: Send + Sync + 'static>(
        limit: usize,
        value: T,
    ) -> Result<Self, NativeNumericalPoolError> {
        NativePageResources::from_retained_pool_data(limit, value)
            .map(Self)
            .map_err(map_error)
    }
    pub fn reserved_bytes(&self) -> usize {
        self.0.current()
    }
    // Called only after the first-party actual numerical T has absorbed its
    // full incremental admission. It grants no native write/source authority.
    pub(crate) fn begin_managed_epoch(
        &self,
        fully_admitted_ceiling: usize,
    ) -> crate::Result<crate::tree_store::NativeNumericalEpoch> {
        self.0.begin_managed_epoch(fully_admitted_ceiling)
    }
    pub(crate) fn resources(&self) -> &NativePageResources {
        &self.0
    }
}
