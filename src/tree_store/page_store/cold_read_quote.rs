//! Numerical heap requests for the one closed prepaid cold read chain.

// Native numerical-only quote for the FIXED backend's string custom errors.
// The exact pinned core/alloc sources are attested by the encompassing runner.
// No private std layout equality is assumed: sum each public constituent plus
// one full maximum-alignment padding allowance per actual Custom field.
#[cfg(not(redb_no_std))]
pub(crate) fn native_cold_string_error_bytes(message: usize) -> crate::Result<u64> {
    use super::ReadOnlyResourceProfile as P;
    use super::{container_bounds as b, page_resources::refused};
    use core::mem::{align_of, size_of};
    type Payload = core::ptr::NonNull<dyn core::error::Error + Send + Sync>;
    let alignment = align_of::<std::io::ErrorKind>()
        .max(align_of::<Payload>())
        .max(align_of::<fn()>())
        .max(4);
    let custom = [
        size_of::<std::io::ErrorKind>(),
        size_of::<Payload>(),
        size_of::<fn()>(),
        size_of::<fn()>(),
    ]
    .into_iter()
    .try_fold(0usize, |sum, field| {
        sum.checked_add(field)
            .and_then(|n| n.checked_add(alignment - 1))
            .ok_or_else(refused)
    })?;
    let custom = P::allocation(custom, alignment).map_err(|_| refused())?;
    let string_payload = size_of::<alloc::string::String>()
        .checked_add(align_of::<alloc::string::String>() - 1)
        .ok_or_else(refused)?;
    let string_error = P::allocation(string_payload, align_of::<alloc::string::String>())
        .map_err(|_| refused())?;
    let backing = b::vector_storage::<u8>(message)
        .map_err(|_| refused())?
        .allocator_bytes;
    custom
        .checked_add(string_error)
        .and_then(|n| n.checked_add(backing))
        .ok_or_else(refused)
}

// Only the closed native cold constructor uses this prospective formatter work.
// Pinned format::Arguments::estimated_capacity <= 2*output length; later
// RawVec growth has old capacity < required length <= output length. Hence
// final <= max(2*output, MIN_NON_ZERO_CAP), old <= output. This accounts for
// simultaneous old/new buffers rather than pricing only the final String.
#[cfg(not(redb_no_std))]
pub(crate) fn native_cold_format_work_bytes(output: usize) -> crate::Result<u64> {
    use super::{container_bounds as b, page_resources::refused};
    let final_capacity = output.checked_mul(2).ok_or_else(refused)?.max(8);
    b::vector_storage::<u8>(final_capacity)
        .map_err(|_| refused())?
        .allocator_bytes
        .checked_add(
            b::vector_storage::<u8>(output)
                .map_err(|_| refused())?
                .allocator_bytes,
        )
        .ok_or_else(refused)
}

#[cfg(not(redb_no_std))]
pub(crate) fn native_cold_fixed_error_work_bytes() -> crate::Result<usize> {
    use super::page_resources::refused;
    let u32_digits = u32::MAX.ilog10() as usize + 1;
    let u64_digits = u64::MAX.ilog10() as usize + 1;
    let u8_digits = u8::MAX.ilog10() as usize + 1;
    let pn = "r./"
        .len()
        .checked_add(u32_digits)
        .and_then(|n| n.checked_add(u32_digits))
        .and_then(|n| n.checked_add(u8_digits))
        .ok_or_else(refused)?;
    // A finite superset of actual cold header/lookup/depth/page errors. Templates
    // retain placeholder spelling, so adding real digit bounds overcounts it.
    let rendered = [
        "Invalid magic number".len(),
        "Database page size {page_size} does not match expected {expected_page_size}".len() + 2*u32_digits,
        "Invalid region data page count: {region_max_data_pages}".len() + u32_digits,
        "Invalid region header page count: {region_header_pages}".len() + u32_digits,
        "Trailing region data pages {trailing_data_pages} exceed region max {region_max_data_pages}".len() + 2*u32_digits,
        "Invalid region count: full_regions={full_regions}, trailing_data_pages={trailing_data_pages}".len() + 2*u32_digits,
        "File truncated below stored layout: file_len={file_len}, layout_len={stored_len}".len() + 2*u64_digits,
        "File length requires more regions than are addressable: file_len={file_len}".len() + u64_digits,
        "File truncated below the minimum layout: file_len={file_len}".len() + u64_digits,
        "File length does not correspond to a valid region layout: file_len={file_len}".len() + u64_digits,
        "Primary is corrupted despite 2-phase commit".len(),
        "Both commit slots are corrupted".len(),
        "Primary commit slot is corrupted".len(),
        "Expected file format version <= {FILE_FORMAT_VERSION3}, found {version}".len() + 2*u8_digits,
        "Btree exceeded maximum depth".len(),
        "Page {page:?} has order greater than the maximum of {MAX_MAX_PAGE_ORDER}".len() + pn + u8_digits,
        "Malformed native table definition".len(),
    ].into_iter().max().unwrap();
    let message = [
        "regular file required",
        "unsupported readonly custody target",
        "ReadOnlyReopenFileCustody([REDACTED])",
        "readonly artifact changed",
        "bounded native header required",
        "readonly engine still live",
        "readonly lease generation exhausted",
        "ReadOnlyReopenBackend([REDACTED])",
        "readonly lease capacity exhausted",
        "readonly lease not held",
        "readonly query overlaps own lease",
        "empty readonly lease",
        "readonly native custody unavailable",
        "readonly native custody",
        "read_only_reopen_custody_tests.rs",
        "experimental-api-5",
        "lock range overlaps backend-reserved bytes",
        "failed to fill whole buffer",
        "failed to write whole buffer",
        "the storage backend does not support this operation",
        "Not a redb database: magic number mismatch",
        "Database file is empty and creating a new database was not requested",
    ]
    .into_iter()
    .map(str::len)
    .max()
    .unwrap();
    let io = native_cold_string_error_bytes(message)?;
    // One returned original error and one ignored synchronous cleanup error may
    // coexist; native cleanup is sequential, not an unbounded error collection.
    let bytes = io
        .checked_add(io)
        .and_then(|n| {
            native_cold_format_work_bytes(rendered)
                .ok()
                .and_then(|f| n.checked_add(f))
        })
        .ok_or_else(refused)?;
    usize::try_from(bytes).map_err(|_| refused())
}

// Fixed-class owning admissions; no caller-selected byte count is accepted.
pub(crate) fn prepare_cold_read_error_work(
    resources: &super::NativePageResources,
) -> crate::Result<super::page_resources::NativeControlAllocation> {
    resources
        .seal_fixed_observation_admission()?
        .require_current()?;
    resources.admit_control(native_cold_fixed_error_work_bytes()?)
}
pub(crate) fn cold_read_fixed_capacity_bytes(
    profile: Option<super::ReadOnlyResourceProfile>,
) -> crate::Result<usize> {
    use super::{ReadOnlyResourceProfile as P, page_resources::refused};
    use crate::db::TransactionGuard;
    use crate::transaction_tracker::TransactionTracker;
    use crate::types::Value;
    let mut bytes =
        super::page_manager::TransactionalMemory::prepaid_read_memory_capacity_bytes(profile)
            .map_err(|_| refused())?;
    for extra in [
        crate::db::cold_read_backend_box_capacity_bytes().map_err(|_| refused())?,
        P::arc_allocation::<TransactionTracker>().map_err(|_| refused())?,
        P::arc_allocation::<TransactionGuard>().map_err(|_| refused())?,
        P::arc_allocation::<TransactionGuard>().map_err(|_| refused())?,
        crate::tree_store::readonly_cursor_capacity_bytes().map_err(|_| refused())?,
    ] {
        bytes = bytes.checked_add(extra).ok_or_else(refused)?;
    }
    let lower = crate::transactions::AllocatorStateKey::as_bytes(
        &crate::transactions::AllocatorStateKey::Region(0),
    );
    let upper = crate::transactions::AllocatorStateKey::as_bytes(
        &crate::transactions::AllocatorStateKey::Region(u32::MAX),
    );
    for len in [lower.len(), upper.len()] {
        bytes = bytes
            .checked_add(
                super::container_bounds::vector_storage::<u8>(len)
                    .map_err(|_| refused())?
                    .allocator_bytes,
            )
            .ok_or_else(refused)?;
    }
    usize::try_from(bytes).map_err(|_| refused())
}
pub(crate) fn prepare_cold_read_fixed(
    resources: &super::NativePageResources,
    profile: Option<super::ReadOnlyResourceProfile>,
) -> crate::Result<super::page_resources::NativeControlAllocation> {
    resources
        .seal_fixed_observation_admission()?
        .require_current()?;
    resources.admit_control(cold_read_fixed_capacity_bytes(profile)?)
}

pub(crate) fn prepare_cold_read_admission(
    epoch: &super::NativeNumericalEpoch,
) -> crate::Result<super::NativeFixedObservationAdmission> {
    epoch.require_open()?;
    epoch.resources().seal_fixed_observation_admission()
}
