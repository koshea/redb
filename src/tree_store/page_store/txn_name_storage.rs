//! Native namespace controls for a closed, completely prepared point recipe.
//! Ordinary storage retains the existing BTreeMap/String path. Source names
//! and every map slot are allocated before the checked writer constructor;
//! clear/remove/drain never retire their backing or acquire new allowance.
#[cfg(not(redb_no_std))]
use super::NativeOwnedShared;
#[cfg(not(redb_no_std))]
use super::page_resources::{NativeControlAllocation, NativePageResources};
use alloc::collections::{BTreeMap, btree_map};
use alloc::string::{String, ToString};
#[cfg(not(redb_no_std))]
use alloc::vec::Vec;
use core::ops::Deref;

pub(crate) struct NativeTableName(NameKind);
enum NameKind {
    Ordinary(String),
    #[cfg(not(redb_no_std))]
    Source(NativeOwnedShared<SourceName>),
}
#[cfg(not(redb_no_std))]
struct SourceName {
    // The String buffer retires before its charge. The outer closed shared
    // wrapper independently retires its Arc shell before its own charge.
    text: String,
    _allocation: NativeControlAllocation,
}
impl NativeTableName {
    pub(crate) fn ordinary(name: &str) -> Self {
        Self(NameKind::Ordinary(name.to_string()))
    }
    pub(crate) fn as_str(&self) -> &str {
        self
    }
    #[cfg(not(redb_no_std))]
    fn prepare_source(name: &str, resources: &NativePageResources) -> crate::Result<Self> {
        use super::{container_bounds, page_resources::refused};
        let bound = container_bounds::vector_storage::<u8>(name.len()).map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut text = String::new();
        text.try_reserve_exact(name.len()).map_err(|_| refused())?;
        text.push_str(name);
        Ok(Self(NameKind::Source(NativeOwnedShared::source(
            SourceName {
                text,
                _allocation: allocation,
            },
            resources,
        )?)))
    }
}
impl Deref for NativeTableName {
    type Target = str;
    fn deref(&self) -> &str {
        match &self.0 {
            NameKind::Ordinary(name) => name,
            #[cfg(not(redb_no_std))]
            NameKind::Source(name) => &name.text,
        }
    }
}
impl Clone for NativeTableName {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            NameKind::Ordinary(name) => NameKind::Ordinary(name.clone()),
            #[cfg(not(redb_no_std))]
            NameKind::Source(name) => NameKind::Source(name.clone()),
        })
    }
}

pub(crate) struct NativeTxnNameMap<V>(NameMapKind<V>);
enum NameMapKind<V> {
    Ordinary(BTreeMap<String, V>),
    #[cfg(not(redb_no_std))]
    Source(SourceNameSlots<V>),
}
#[cfg(not(redb_no_std))]
struct SourceNameSlots<V> {
    slots: Vec<(NativeTableName, Option<V>)>,
    _allocation: NativeControlAllocation,
}
impl<V> Default for NativeTxnNameMap<V> {
    fn default() -> Self {
        Self(NameMapKind::Ordinary(BTreeMap::new()))
    }
}
impl<V> NativeTxnNameMap<V> {
    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_source(
        names: &[&str],
        resources: &NativePageResources,
    ) -> crate::Result<Self> {
        use super::{container_bounds, page_resources::refused};
        // The complete recipe supplies its exact canonical table-name union.
        // No sorting Vec, subset closure or caller-chosen native authority.
        if names.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(refused());
        }
        let bound = container_bounds::vector_storage::<(NativeTableName, Option<V>)>(names.len())
            .map_err(|_| refused())?;
        let allocation = resources
            .admit_control(usize::try_from(bound.allocator_bytes).map_err(|_| refused())?)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(names.len())
            .map_err(|_| refused())?;
        for name in names {
            slots.push((NativeTableName::prepare_source(name, resources)?, None));
        }
        Ok(Self(NameMapKind::Source(SourceNameSlots {
            slots,
            _allocation: allocation,
        })))
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn belongs_to(&self, resources: &NativePageResources) -> bool {
        match &self.0 {
            NameMapKind::Ordinary(_) => false,
            NameMapKind::Source(source) => source._allocation.belongs_to(resources),
        }
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn matches_prepared_names(&self, names: &[&str]) -> bool {
        match &self.0 {
            NameMapKind::Ordinary(_) => false,
            NameMapKind::Source(source) => {
                source.slots.len() == names.len()
                    && source
                        .slots
                        .iter()
                        .zip(names)
                        .all(|((name, value), expected)| {
                            value.is_none() && name.as_str() == *expected
                        })
            }
        }
    }
    #[cfg(not(redb_no_std))]
    pub(crate) fn matches_fixed_recipe_names(
        &self,
        recipe: &super::NativeOwnedFixedRecipe,
    ) -> bool {
        match &self.0 {
            NameMapKind::Ordinary(_) => false,
            NameMapKind::Source(source) => {
                source.slots.len() == recipe.tables_len()
                    && source
                        .slots
                        .iter()
                        .enumerate()
                        .all(|(index, (name, value))| {
                            value.is_none() && recipe.table_name(index) == Some(name.as_str())
                        })
            }
        }
    }
    pub(crate) fn require_name(&self, name: &str) -> crate::Result<()> {
        #[cfg(not(redb_no_std))]
        if let NameMapKind::Source(source) = &self.0 {
            if source
                .slots
                .binary_search_by(|(key, _)| key.as_str().cmp(name))
                .is_err()
            {
                return Err(super::page_resources::refused());
            }
        }
        let _ = name;
        Ok(())
    }
    pub(crate) fn source_name_for_handle(
        &self,
        name: &str,
    ) -> crate::Result<Option<NativeTableName>> {
        match &self.0 {
            NameMapKind::Ordinary(_) => Ok(None),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(_) => self.name_for_handle(name).map(Some),
        }
    }
    pub(crate) fn name_for_handle(&self, name: &str) -> crate::Result<NativeTableName> {
        match &self.0 {
            NameMapKind::Ordinary(_) => Ok(NativeTableName::ordinary(name)),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => {
                let index = source
                    .slots
                    .binary_search_by(|(key, _)| key.as_str().cmp(name))
                    .map_err(|_| super::page_resources::refused())?;
                Ok(source.slots[index].0.clone())
            }
        }
    }
    pub(crate) fn get(&self, name: &str) -> Option<&V> {
        match &self.0 {
            NameMapKind::Ordinary(values) => values.get(name),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => source
                .slots
                .binary_search_by(|(key, _)| key.as_str().cmp(name))
                .ok()
                .and_then(|index| source.slots[index].1.as_ref()),
        }
    }
    pub(crate) fn insert(&mut self, name: &str, value: V) -> Option<V> {
        match &mut self.0 {
            NameMapKind::Ordinary(values) => values.insert(name.to_string(), value),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => {
                // require_name is checked BEFORE opening/creating a table.
                // A fixed executor cannot rename tables or expose raw aliases.
                let index = source
                    .slots
                    .binary_search_by(|(key, _)| key.as_str().cmp(name))
                    .expect("complete fixed namespace was preflighted before mutation");
                source.slots[index].1.replace(value)
            }
        }
    }
    pub(crate) fn remove(&mut self, name: &str) -> Option<V> {
        match &mut self.0 {
            NameMapKind::Ordinary(values) => values.remove(name),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => source
                .slots
                .binary_search_by(|(key, _)| key.as_str().cmp(name))
                .ok()
                .and_then(|index| source.slots[index].1.take()),
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        match &self.0 {
            NameMapKind::Ordinary(values) => values.is_empty(),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => source.slots.iter().all(|(_, value)| value.is_none()),
        }
    }
    pub(crate) fn clear(&mut self) {
        match &mut self.0 {
            NameMapKind::Ordinary(values) => values.clear(),
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => {
                for (_, value) in &mut source.slots {
                    *value = None;
                }
            }
        }
    }
    pub(crate) fn drain(&mut self) -> NativeTxnNameDrain<'_, V> {
        match &mut self.0 {
            NameMapKind::Ordinary(values) => {
                NativeTxnNameDrain::Ordinary(core::mem::take(values).into_iter())
            }
            #[cfg(not(redb_no_std))]
            NameMapKind::Source(source) => NativeTxnNameDrain::Source(source.slots.iter_mut()),
        }
    }
}
pub(crate) enum NativeTxnNameDrain<'a, V> {
    Ordinary(btree_map::IntoIter<String, V>),
    #[cfg(not(redb_no_std))]
    Source(core::slice::IterMut<'a, (NativeTableName, Option<V>)>),
    // NoStd's ordinary iterator still uses the same generic lifetime.
    #[cfg(redb_no_std)]
    Lifetime(core::marker::PhantomData<&'a mut V>),
}
impl<V> Iterator for NativeTxnNameDrain<'_, V> {
    type Item = (NativeTableName, V);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Ordinary(values) => values
                .next()
                .map(|(name, value)| (NativeTableName(NameKind::Ordinary(name)), value)),
            #[cfg(not(redb_no_std))]
            Self::Source(values) => {
                for (name, value) in values.by_ref() {
                    if let Some(value) = value.take() {
                        return Some((name.clone(), value));
                    }
                }
                None
            }
            #[cfg(redb_no_std)]
            Self::Lifetime(_) => None,
        }
    }
}
impl<V> Drop for NativeTxnNameDrain<'_, V> {
    fn drop(&mut self) {
        // Match mem::take(BTreeMap) semantics on early flush error: ALL still
        // staged values are discarded, without retiring prepared Source names.
        #[cfg(not(redb_no_std))]
        if let Self::Source(values) = self {
            for (_, value) in values.by_ref() {
                *value = None;
            }
        }
    }
}
