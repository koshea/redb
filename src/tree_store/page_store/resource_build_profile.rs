//! This closed build profile grants layout interpretation only, never resources.
use super::container_bounds::ContainerBoundError;

pub(super) struct NativeResourceBuildProfile {
    _private: (),
}

impl NativeResourceBuildProfile {
    pub(super) fn require() -> Result<Self, ContainerBoundError> {
        if cfg!(all(
            redb_resource_layout_supported,
            not(redb_no_std),
            target_arch = "x86_64",
            target_pointer_width = "64",
            target_feature = "sse2",
            target_os = "linux",
            target_env = "gnu",
            not(miri)
        )) {
            Ok(Self { _private: () })
        } else {
            Err(ContainerBoundError::UnsupportedBuild)
        }
    }
}

// req: STO-020, PRJ-004, REC-001
#[cfg(all(test, not(redb_no_std)))]
mod tests {
    use super::NativeResourceBuildProfile;
    #[test]
    fn native_resource_profile_matches_actual_pinned_build() {
        assert!(
            cfg!(redb_resource_layout_supported),
            "this qualification requires the actual pinned compiler/target build"
        );
        assert!(NativeResourceBuildProfile::require().is_ok());
    }
}
