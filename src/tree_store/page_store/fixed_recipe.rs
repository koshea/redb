//! Owned exact physical point envelope for the closed Source factory.
//! Numerical native input only: no Source identity, IR, Commit assignment or
//! publication/reconciliation authority is minted by this representation.
use super::{
    container_bounds,
    page_resources::{NativeControlAllocation, NativePageResources},
};
use crate::tree_store::{
    NativeKeyPathError as E, NativePointKey, NativePointMutation, NativePointRecipe,
};
use alloc::vec::Vec;
use core::ops::Range;

// This is the finite native supported physical envelope for the CURRENT closed
// first-party recipe: <=128 row changes +6 controls +3 history writes. It is
// not an application knob, row-count authority or general native transaction
// API. Public numerical signatures/profile registration remain root-reviewed
// before any complete factory activation.
const MAX_FIXED_POINTS: usize = 128 + 6 + 3;
type R<T> = core::result::Result<T, E>;

#[derive(Clone, Copy)]
pub(crate) struct NativeFixedPoint<'a> {
    pub(crate) table: &'a str,
    pub(crate) mutation: NativePointMutation<'a>,
}
struct PointCell {
    table: Range<usize>,
    key: Range<usize>,
    value: Option<Range<usize>>,
    string_key: bool,
}
struct TableCell {
    name: Range<usize>,
    ordered_indices: Range<usize>,
    string_key: bool,
}
struct Dimensions {
    bytes: usize,
    points: usize,
    charge: usize,
}

pub(crate) struct NativeOwnedFixedRecipe {
    bytes: Vec<u8>,
    points: Vec<PointCell>,
    ordered_indices: Vec<usize>,
    tables: Vec<TableCell>,
    // Every native owned buffer, including unused capacity, dies first.
    _allocation: NativeControlAllocation,
}
impl NativeOwnedFixedRecipe {
    fn dimensions(input: &[NativeFixedPoint<'_>]) -> R<Dimensions> {
        super::resource_build_profile::NativeResourceBuildProfile::require()
            .map_err(|_| E::UnsupportedState)?;
        if input.is_empty() || input.len() > MAX_FIXED_POINTS {
            return Err(E::UnsupportedState);
        }
        let mut bytes = 0_usize;
        for point in input {
            if point.table.is_empty() {
                return Err(E::Malformed);
            }
            if point.table.len() > crate::db::PLANNED_READ_KEY_BYTES {
                return Err(E::UnsupportedState);
            }
            let key = point.mutation.key();
            let key_bytes = key.bytes();
            let value = match point.mutation {
                NativePointMutation::Put { value, .. } => {
                    if value.len() > super::MAX_VALUE_LENGTH
                        || key_bytes
                            .len()
                            .checked_add(value.len())
                            .ok_or(E::Overflow)?
                            > super::MAX_PAIR_LENGTH
                    {
                        return Err(E::UnsupportedState);
                    }
                    value.len()
                }
                NativePointMutation::Delete { .. } => 0,
            };
            bytes = bytes
                .checked_add(point.table.len())
                .and_then(|n| n.checked_add(key_bytes.len()))
                .and_then(|n| n.checked_add(value))
                .ok_or(E::Overflow)?;
        }
        let mut charge = 0_u64;
        for part in [
            container_bounds::vector_storage::<u8>(bytes),
            container_bounds::vector_storage::<PointCell>(input.len()),
            container_bounds::vector_storage::<usize>(input.len()),
            // Actual table count is <=point count; the spare slots are owned.
            container_bounds::vector_storage::<TableCell>(input.len()),
        ] {
            charge = charge
                .checked_add(part.map_err(|_| E::Overflow)?.allocator_bytes)
                .ok_or(E::Overflow)?;
        }
        Ok(Dimensions {
            bytes,
            points: input.len(),
            charge: usize::try_from(charge).map_err(|_| E::Overflow)?,
        })
    }
    pub(crate) fn storage_capacity_bytes(input: &[NativeFixedPoint<'_>]) -> R<u64> {
        u64::try_from(Self::dimensions(input)?.charge).map_err(|_| E::Overflow)
    }
    pub(crate) fn prepare(
        input: &[NativeFixedPoint<'_>],
        resources: &NativePageResources,
    ) -> R<Self> {
        let dim = Self::dimensions(input)?;
        if !resources.is_source_write() {
            return Err(E::UnsupportedState);
        }
        // The caller already admitted the whole Native/first-party peak; this
        // actual same-pool guard precedes EVERY owned envelope allocation.
        let allocation = resources.admit_control(dim.charge)?;
        let mut result = Self {
            bytes: Vec::new(),
            points: Vec::new(),
            ordered_indices: Vec::new(),
            tables: Vec::new(),
            _allocation: allocation,
        };
        result
            .bytes
            .try_reserve_exact(dim.bytes)
            .map_err(|_| super::page_resources::refused())?;
        result
            .points
            .try_reserve_exact(dim.points)
            .map_err(|_| super::page_resources::refused())?;
        result
            .ordered_indices
            .try_reserve_exact(dim.points)
            .map_err(|_| super::page_resources::refused())?;
        result
            .tables
            .try_reserve_exact(dim.points)
            .map_err(|_| super::page_resources::refused())?;
        for (index, point) in input.iter().enumerate() {
            let table = result.store(point.table.as_bytes());
            let key = result.store(point.mutation.key().bytes());
            let value = match point.mutation {
                NativePointMutation::Put { value, .. } => Some(result.store(value)),
                NativePointMutation::Delete { .. } => None,
            };
            result.points.push(PointCell {
                table,
                key,
                value,
                string_key: point.mutation.key().is_str(),
            });
            result.ordered_indices.push(index);
        }
        assert_eq!(result.bytes.len(), dim.bytes);
        // Sorting only NUMERICAL observation indices preserves the exact
        // original physical execution order in points. No mutation reordering.
        let bytes = &result.bytes;
        let points = &result.points;
        result.ordered_indices.sort_unstable_by(|left, right| {
            let left = &points[*left];
            let right = &points[*right];
            (&bytes[left.table.clone()], &bytes[left.key.clone()])
                .cmp(&(&bytes[right.table.clone()], &bytes[right.key.clone()]))
        });
        let mut start = 0;
        while start < result.ordered_indices.len() {
            let first = &result.points[result.ordered_indices[start]];
            let name = first.table.clone();
            let string_key = first.string_key;
            let mut end = start + 1;
            while end < result.ordered_indices.len() {
                let previous = &result.points[result.ordered_indices[end - 1]];
                let next = &result.points[result.ordered_indices[end]];
                if result.bytes[next.table.clone()] != result.bytes[name.clone()] {
                    break;
                }
                if string_key != next.string_key
                    || result.bytes[previous.key.clone()] == result.bytes[next.key.clone()]
                {
                    return Err(E::Malformed);
                }
                end += 1;
            }
            result.tables.push(TableCell {
                name,
                ordered_indices: start..end,
                string_key,
            });
            start = end;
        }
        Ok(result)
    }
    fn store(&mut self, bytes: &[u8]) -> Range<usize> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        start..self.bytes.len()
    }
    pub(crate) fn belongs_to(&self, resources: &NativePageResources) -> bool {
        self._allocation.belongs_to(resources)
    }
    pub(crate) fn matches_observation_allocation(
        &self,
        allocation: &NativeControlAllocation,
    ) -> bool {
        self._allocation.same_pool(allocation)
    }
    // Allocation-free FIRST original execution index for each canonical table.
    // Canonical observation indices are never used as execution order.
    pub(crate) fn table_first_open_at(&self, point: usize) -> Option<usize> {
        if point >= self.points.len() {
            return None;
        }
        self.tables.iter().position(|table| {
            self.ordered_indices[table.ordered_indices.clone()]
                .iter()
                .copied()
                .min()
                == Some(point)
        })
    }
    pub(crate) fn points_len(&self) -> usize {
        self.points.len()
    }
    pub(crate) fn tables_len(&self) -> usize {
        self.tables.len()
    }
    pub(crate) fn point(&self, index: usize) -> Option<NativeFixedPoint<'_>> {
        let cell = self.points.get(index)?;
        // Constructor copies typed str input, private ranges never mutate.
        let table = core::str::from_utf8(&self.bytes[cell.table.clone()])
            .expect("prepared native table name");
        let key = if cell.string_key {
            NativePointKey::Str(
                core::str::from_utf8(&self.bytes[cell.key.clone()])
                    .expect("prepared native str key"),
            )
        } else {
            NativePointKey::Bytes(&self.bytes[cell.key.clone()])
        };
        let mutation = match &cell.value {
            Some(value) => NativePointMutation::Put {
                key,
                value: &self.bytes[value.clone()],
            },
            None => NativePointMutation::Delete { key },
        };
        Some(NativeFixedPoint { table, mutation })
    }
    pub(crate) fn matches_exact_input(&self, input: &[NativeFixedPoint<'_>]) -> bool {
        self.points.len() == input.len()
            && input.iter().enumerate().all(|(index, actual)| {
                let expected = self.point(index).expect("bounded actual recipe index");
                expected.table == actual.table
                    && expected.mutation.key().is_str() == actual.mutation.key().is_str()
                    && expected.mutation.key().bytes() == actual.mutation.key().bytes()
                    && match (expected.mutation, actual.mutation) {
                        (
                            NativePointMutation::Put { value: a, .. },
                            NativePointMutation::Put { value: b, .. },
                        ) => a == b,
                        (
                            NativePointMutation::Delete { .. },
                            NativePointMutation::Delete { .. },
                        ) => true,
                        _ => false,
                    }
            })
    }
    pub(crate) fn table_name(&self, index: usize) -> Option<&str> {
        let table = self.tables.get(index)?;
        Some(
            core::str::from_utf8(&self.bytes[table.name.clone()])
                .expect("prepared native table name"),
        )
    }
    pub(crate) fn table_uses_str_keys(&self, index: usize) -> Option<bool> {
        self.tables.get(index).map(|table| table.string_key)
    }
    pub(crate) fn table_views_capacity_bytes(&self, index: usize) -> R<u64> {
        let table = self.tables.get(index).ok_or(E::Malformed)?;
        container_bounds::vector_storage::<NativePointMutation<'_>>(table.ordered_indices.len())
            .map(|bound| bound.allocator_bytes)
            .map_err(|_| E::Overflow)
    }
    pub(crate) fn prepare_table_views<'recipe>(
        &'recipe self,
        index: usize,
        resources: &NativePageResources,
    ) -> R<NativeFixedTableViews<'recipe>> {
        if !self._allocation.belongs_to(resources) {
            return Err(E::ForeignOwner);
        }
        let table = self.tables.get(index).ok_or(E::Malformed)?;
        let bound = container_bounds::vector_storage::<NativePointMutation<'recipe>>(
            table.ordered_indices.len(),
        )
        .map_err(|_| E::Overflow)?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| E::Overflow)?)?;
        let mut operations = Vec::new();
        operations
            .try_reserve_exact(table.ordered_indices.len())
            .map_err(|_| super::page_resources::refused())?;
        for index in &self.ordered_indices[table.ordered_indices.clone()] {
            operations.push(self.point(*index).ok_or(E::Malformed)?.mutation);
        }
        // This compiler remains the sole existing typed point class/geometry
        // preflight; unsupported mixed classes are never silently widened.
        NativePointRecipe::compile(&operations)?;
        Ok(NativeFixedTableViews {
            operations,
            _allocation: allocation,
        })
    }
}
pub(crate) struct NativeFixedTableViews<'recipe> {
    operations: Vec<NativePointMutation<'recipe>>,
    _allocation: NativeControlAllocation,
}
impl NativeFixedTableViews<'_> {
    pub(crate) fn recipe(&self) -> R<NativePointRecipe<'_>> {
        NativePointRecipe::compile(&self.operations)
    }
}
