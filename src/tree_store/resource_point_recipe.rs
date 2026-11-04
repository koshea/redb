//! Borrowed, finite native point operations. Numerical input, never authority.
use super::resource_key_paths::NativeKeyPathError as E;
use crate::tree_store::page_store::{MAX_PAIR_LENGTH, MAX_VALUE_LENGTH};

#[derive(Clone, Copy)]
pub(crate) enum NativePointKey<'a> {
    Bytes(&'a [u8]),
    Str(&'a str),
}
impl NativePointKey<'_> {
    pub(in crate::tree_store) fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(v) => v,
            Self::Str(v) => v.as_bytes(),
        }
    }
    pub(in crate::tree_store) fn is_str(&self) -> bool {
        matches!(self, Self::Str(_))
    }
}
#[derive(Clone, Copy)]
pub(crate) enum NativePointMutation<'a> {
    Put {
        key: NativePointKey<'a>,
        value: &'a [u8],
    },
    Delete {
        key: NativePointKey<'a>,
    },
}
impl NativePointMutation<'_> {
    pub(in crate::tree_store) fn key(&self) -> NativePointKey<'_> {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => *key,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativePointRecipeClass {
    PutOnly,
    DeleteOnly,
    OneDeleteOnePut,
}

pub(crate) struct NativePointRecipe<'a> {
    pub(super) operations: &'a [NativePointMutation<'a>],
    pub(super) puts: usize,
    pub(super) deletes: usize,
    pub(super) incoming_bytes: usize,
    pub(super) maximum_key: usize,
    pub(super) string_keys: bool,
    class: NativePointRecipeClass,
}
impl<'a> NativePointRecipe<'a> {
    pub(crate) fn compile(operations: &'a [NativePointMutation<'a>]) -> Result<Self, E> {
        let first = operations.first().ok_or(E::UnsupportedState)?;
        let string_keys = first.key().is_str();
        let mut puts = 0_usize;
        let mut deletes = 0_usize;
        let mut incoming_bytes = 0_usize;
        let mut maximum_key = 0_usize;
        let mut prior: Option<&[u8]> = None;
        for operation in operations {
            let key = operation.key();
            let bytes = match operation {
                NativePointMutation::Put { key, .. } | NativePointMutation::Delete { key } => {
                    match key {
                        NativePointKey::Bytes(v) => *v,
                        NativePointKey::Str(v) => v.as_bytes(),
                    }
                }
            };
            if key.is_str() != string_keys || prior.is_some_and(|p| p >= bytes) {
                return Err(E::Malformed);
            }
            maximum_key = maximum_key.max(bytes.len());
            match operation {
                NativePointMutation::Put { value, .. } => {
                    let pair = bytes.len().checked_add(value.len()).ok_or(E::Overflow)?;
                    if pair > MAX_PAIR_LENGTH || value.len() > MAX_VALUE_LENGTH {
                        return Err(E::UnsupportedState);
                    }
                    incoming_bytes = incoming_bytes.checked_add(pair).ok_or(E::Overflow)?;
                    puts = puts.checked_add(1).ok_or(E::Overflow)?;
                }
                NativePointMutation::Delete { .. } => {
                    deletes = deletes.checked_add(1).ok_or(E::Overflow)?;
                }
            }
            prior = Some(bytes);
        }
        let class = match (puts, deletes) {
            (_, 0) => NativePointRecipeClass::PutOnly,
            (0, _) => NativePointRecipeClass::DeleteOnly,
            (1, 1) => NativePointRecipeClass::OneDeleteOnePut,
            _ => return Err(E::UnsupportedState),
        };
        Ok(Self {
            operations,
            puts,
            deletes,
            incoming_bytes,
            maximum_key,
            string_keys,
            class,
        })
    }
    // The recipe remains immutably borrowed throughout numerical-plan use.
    // Compare the complete ordered native operation envelope, not shape/counts.
    pub(super) fn same_operations(&self, actual: &NativePointRecipe<'_>) -> bool {
        if self.class != actual.class || self.operations.len() != actual.operations.len() {
            return false;
        }
        self.operations
            .iter()
            .zip(actual.operations)
            .all(|(expected, actual)| {
                if expected.key().is_str() != actual.key().is_str()
                    || expected.key().bytes() != actual.key().bytes()
                {
                    return false;
                }
                match (expected, actual) {
                    (
                        NativePointMutation::Put { value: left, .. },
                        NativePointMutation::Put { value: right, .. },
                    ) => left == right,
                    (NativePointMutation::Delete { .. }, NativePointMutation::Delete { .. }) => {
                        true
                    }
                    _ => false,
                }
            })
    }
    pub(crate) fn class(&self) -> NativePointRecipeClass {
        self.class
    }
}
impl Drop for NativePointRecipe<'_> {
    fn drop(&mut self) {}
}
