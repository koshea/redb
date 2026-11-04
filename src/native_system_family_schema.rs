//! Exact six native SYSTEM schemas, numerical metadata only.
use super::{
    ALLOCATOR_STATE_TABLE_NAME, AllocatorStateKey, DATA_ALLOCATED_TABLE, DATA_FREED_TABLE,
    NEXT_SAVEPOINT_TABLE, PageList, SAVEPOINT_TABLE, SYSTEM_FREED_TABLE, SerializedSavepoint,
    TransactionIdWithPagination,
};
use crate::transaction_tracker::SavepointId;
use crate::tree_store::{InternalTableDefinition, NativeDefinitionResourceError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeSystemFamily {
    NextSavepoint,
    Savepoints,
    DataAllocated,
    DataFreed,
    SystemFreed,
    Allocator,
}
impl NativeSystemFamily {
    pub(crate) fn all() -> [Self; 6] {
        [
            Self::NextSavepoint,
            Self::Savepoints,
            Self::DataAllocated,
            Self::DataFreed,
            Self::SystemFreed,
            Self::Allocator,
        ]
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::NextSavepoint => NEXT_SAVEPOINT_TABLE.name,
            Self::Savepoints => SAVEPOINT_TABLE.name,
            Self::DataAllocated => DATA_ALLOCATED_TABLE.name,
            Self::DataFreed => DATA_FREED_TABLE.name,
            Self::SystemFreed => SYSTEM_FREED_TABLE.name,
            Self::Allocator => ALLOCATOR_STATE_TABLE_NAME,
        }
    }
    pub(crate) fn for_name(name: &[u8]) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|family| family.name().as_bytes() == name)
    }
    pub(crate) fn index(self) -> usize {
        self as usize
    }
    pub(crate) fn encoded_definition_bytes(self) -> Result<usize, NativeDefinitionResourceError> {
        match self {
            Self::NextSavepoint => {
                InternalTableDefinition::native_resource_encoded_len::<(), SavepointId>()
            }
            Self::Savepoints => InternalTableDefinition::native_resource_encoded_len::<
                SavepointId,
                SerializedSavepoint<'static>,
            >(),
            Self::DataAllocated | Self::DataFreed | Self::SystemFreed => {
                InternalTableDefinition::native_resource_encoded_len::<
                    TransactionIdWithPagination,
                    PageList<'static>,
                >()
            }
            Self::Allocator => InternalTableDefinition::native_resource_encoded_len::<
                AllocatorStateKey,
                &'static [u8],
            >(),
        }
    }
    pub(crate) fn matches_definition(
        self,
        bytes: &[u8],
    ) -> Result<bool, NativeDefinitionResourceError> {
        match self {
            Self::NextSavepoint => {
                InternalTableDefinition::native_resource_matches::<(), SavepointId>(bytes)
            }
            Self::Savepoints => InternalTableDefinition::native_resource_matches::<
                SavepointId,
                SerializedSavepoint<'static>,
            >(bytes),
            Self::DataAllocated | Self::DataFreed | Self::SystemFreed => {
                InternalTableDefinition::native_resource_matches::<
                    TransactionIdWithPagination,
                    PageList<'static>,
                >(bytes)
            }
            Self::Allocator => InternalTableDefinition::native_resource_matches::<
                AllocatorStateKey,
                &'static [u8],
            >(bytes),
        }
    }
}
