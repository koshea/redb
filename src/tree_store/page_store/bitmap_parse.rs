//! Checked borrowed framing for the existing native grouped-bitmap parser.
//! Capacity facts grant no allocator, file, read or Source authority.
use super::super::allocator_reopen_facts::AllocatorResourceError;
use super::super::container_bounds::{self, AllocationBound};
use super::super::read_only_resource::map_bound;
use super::super::resource_build_profile::NativeResourceBuildProfile;
use core::mem::size_of;

#[derive(Clone, Copy)]
pub(super) struct U64BitmapEncodedLayout<'a> {
    encoded: &'a [u8],
    logical_len: u32,
    words: usize,
}

impl<'a> U64BitmapEncodedLayout<'a> {
    // Exactly the existing four-byte length + whole native u64 word grammar.
    // No Vec, owning diagnostic or profile-dependent work precedes this result.
    pub(super) fn inspect(encoded: &'a [u8]) -> Result<Self, AllocatorResourceError> {
        let payload_bytes = encoded
            .len()
            .checked_sub(size_of::<u32>())
            .ok_or(AllocatorResourceError::Incomplete)?;
        if payload_bytes % size_of::<u64>() != 0 {
            return Err(AllocatorResourceError::Incomplete);
        }
        let logical_len = u32::from_le_bytes(encoded[..size_of::<u32>()].try_into().unwrap());
        Ok(Self {
            encoded,
            logical_len,
            words: payload_bytes / size_of::<u64>(),
        })
    }

    // Ordinary parsing keeps its existing failure classification and sequence.
    // This adapter is never used by checked inspection/resource measurement.
    pub(super) fn ordinary(encoded: &'a [u8]) -> Self {
        match Self::inspect(encoded) {
            Ok(layout) => layout,
            Err(_) => legacy_framing_failure(encoded),
        }
    }

    pub(super) fn encoded(self) -> &'a [u8] {
        self.encoded
    }
    pub(super) fn logical_len(self) -> u32 {
        self.logical_len
    }
    pub(super) fn words(self) -> usize {
        self.words
    }

    // The actual ordinary materializer uses Vec::with_capacity(words), then
    // exactly words ordered pushes. It does not use Vec growth or a Source
    // exact-reserve assumption. Initial cold construction is not enabled here.
    pub(super) fn capacity_bound(self) -> Result<AllocationBound, AllocatorResourceError> {
        NativeResourceBuildProfile::require().map_err(map_bound)?;
        container_bounds::vector_storage::<u64>(self.words).map_err(map_bound)
    }
}

fn legacy_framing_failure(encoded: &[u8]) -> ! {
    // This is the ORIGINAL parser's first expression, reached only when its
    // checked framing is invalid. Short inputs keep debug subtraction overflow
    // or release alignment-assert failure; partial words keep that same assert.
    // No second successful parser or new Source failure path is introduced.
    assert_eq!(0, (encoded.len() - size_of::<u32>()) % size_of::<u64>());
    unreachable!("legacy invalid grouped bitmap framing must fail its original assertion")
}

// Fixed-state borrowed offsets used by both owning nested allocator parsers.
// Checked mode is the only path used by Source inspection/materialization.
#[derive(Clone, Copy)]
pub(in crate::tree_store::page_store) struct NativeEncodedChildren<'a> {
    encoded: &'a [u8],
    metadata: usize,
    start: usize,
    remaining: usize,
    checked: bool,
    failed: bool,
}
impl<'a> NativeEncodedChildren<'a> {
    pub(in crate::tree_store::page_store) fn checked(
        encoded: &'a [u8],
        header: usize,
        count: usize,
    ) -> Result<Self, AllocatorResourceError> {
        let start = count
            .checked_mul(size_of::<u32>())
            .and_then(|n| header.checked_add(n))
            .ok_or(AllocatorResourceError::Overflow)?;
        if start > encoded.len() {
            return Err(AllocatorResourceError::Incomplete);
        }
        Ok(Self {
            encoded,
            metadata: header,
            start,
            remaining: count,
            checked: true,
            failed: false,
        })
    }
    pub(in crate::tree_store::page_store) fn ordinary(
        encoded: &'a [u8],
        header: usize,
        count: usize,
    ) -> Self {
        // Exact original header/offset-table arithmetic; no checked Source
        // inspection calls this legacy-only path.
        let start = header + count * size_of::<u32>();
        Self {
            encoded,
            metadata: header,
            start,
            remaining: count,
            checked: false,
            failed: false,
        }
    }
    pub(in crate::tree_store::page_store) fn consumed_end(self) -> usize {
        self.start
    }
}
impl<'a> Iterator for NativeEncodedChildren<'a> {
    type Item = Result<&'a [u8], AllocatorResourceError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.failed {
            return None;
        }
        let result = (|| {
            let end = read_encoded_u32(self.encoded, self.metadata, self.checked)? as usize;
            let child = if self.checked {
                self.encoded
                    .get(self.start..end)
                    .ok_or(AllocatorResourceError::Incomplete)?
            } else {
                &self.encoded[self.start..end]
            };
            self.start = end;
            self.metadata += size_of::<u32>();
            self.remaining -= 1;
            Ok(child)
        })();
        if result.is_err() {
            self.failed = true;
        }
        Some(result)
    }
}

pub(in crate::tree_store::page_store) fn read_encoded_u32(
    encoded: &[u8],
    start: usize,
    checked: bool,
) -> Result<u32, AllocatorResourceError> {
    let bytes = if checked {
        let end = start
            .checked_add(size_of::<u32>())
            .ok_or(AllocatorResourceError::Overflow)?;
        encoded
            .get(start..end)
            .ok_or(AllocatorResourceError::Incomplete)?
    } else {
        &encoded[start..start + size_of::<u32>()]
    };
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

#[derive(Clone, Copy)]
pub(in crate::tree_store::page_store) struct BtreeBitmapEncodedLayout<'a> {
    encoded: &'a [u8],
    height: u32,
    checked: bool,
    consumed_end: Option<usize>,
}
impl<'a> BtreeBitmapEncodedLayout<'a> {
    pub(in crate::tree_store::page_store) fn inspect(
        encoded: &'a [u8],
    ) -> Result<Self, AllocatorResourceError> {
        let height = read_encoded_u32(encoded, 0, true)?;
        let count = usize::try_from(height).map_err(|_| AllocatorResourceError::Overflow)?;
        let mut children = NativeEncodedChildren::checked(encoded, size_of::<u32>(), count)?;
        for child in children.by_ref() {
            U64BitmapEncodedLayout::inspect(child?)?;
        }
        Ok(Self {
            encoded,
            height,
            checked: true,
            consumed_end: Some(children.consumed_end()),
        })
    }
    pub(in crate::tree_store::page_store) fn ordinary(encoded: &'a [u8]) -> Self {
        let height =
            read_encoded_u32(encoded, 0, false).expect("ordinary fixed framing has no typed error");
        // Preserve original table-start arithmetic BEFORE the first Vec.
        let _ = NativeEncodedChildren::ordinary(encoded, size_of::<u32>(), height as usize);
        Self {
            encoded,
            height,
            checked: false,
            consumed_end: None,
        }
    }
    pub(in crate::tree_store::page_store) fn count(self) -> usize {
        self.height as usize
    }
    pub(super) fn is_checked(self) -> bool {
        self.checked
    }
    pub(in crate::tree_store::page_store) fn consumed_end(self) -> Option<usize> {
        self.consumed_end
    }
    pub(super) fn layers(self) -> BtreeBitmapLayers<'a> {
        let children = if self.checked {
            NativeEncodedChildren::checked(self.encoded, size_of::<u32>(), self.count())
                .expect("checked immutable bitmap layout")
        } else {
            NativeEncodedChildren::ordinary(self.encoded, size_of::<u32>(), self.count())
        };
        BtreeBitmapLayers {
            children,
            checked: self.checked,
        }
    }
    pub(in crate::tree_store::page_store) fn source_logical_len(
        self,
    ) -> Result<u32, AllocatorResourceError> {
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        checked
            .layers()
            .last()
            .ok_or(AllocatorResourceError::Incomplete)?
            .map(U64BitmapEncodedLayout::logical_len)
    }

    // The Source parser constructs exact encoded word capacities. The native
    // preparation retains all encoded words, including words beyond logical len.
    pub(in crate::tree_store::page_store) fn source_resize_capacity_bound(
        self,
        new_len: u32,
    ) -> super::super::allocator_growth::GrowthResult {
        use super::super::{allocator_growth as g, container_bounds as b};
        NativeResourceBuildProfile::require().map_err(map_bound)?;
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        if checked.count() == 0 || checked.count() > 6 {
            return Err(AllocatorResourceError::Incomplete);
        }
        let mut root_coverage = new_len;
        for _ in 0..checked.count() {
            root_coverage = root_coverage.div_ceil(64);
        }
        if root_coverage > 1 {
            return Err(AllocatorResourceError::Unsupported);
        }
        let mut result = g::unchanged(
            b::vector_storage::<super::U64GroupedBitmap>(checked.count())
                .map_err(g::map)?
                .allocator_bytes,
        );
        for (index, layer) in checked.layers().enumerate() {
            let layer = layer?;
            let mut target = new_len;
            for _ in index + 1..checked.count() {
                target = target.div_ceil(64);
            }
            let required = super::U64GroupedBitmap::required_words(target).max(layer.words());
            result = g::monotone_preparation(
                result,
                g::parsed_source_vec_preparation::<u64>(layer.words(), required)?,
            )?;
        }
        Ok(result)
    }

    pub(in crate::tree_store::page_store) fn source_capacity_bound(
        self,
    ) -> Result<u64, AllocatorResourceError> {
        // Unchecked ordinary layouts can never enter a legacy panic via quote.
        let checked = if self.checked {
            self
        } else {
            Self::inspect(self.encoded)?
        };
        NativeResourceBuildProfile::require().map_err(map_bound)?;
        let mut total =
            container_bounds::vector_storage::<super::U64GroupedBitmap>(checked.count())
                .map_err(map_bound)?
                .allocator_bytes;
        for layer in checked.layers() {
            total = total
                .checked_add(layer?.capacity_bound()?.allocator_bytes)
                .ok_or(AllocatorResourceError::Overflow)?;
        }
        Ok(total)
    }
}

pub(super) struct BtreeBitmapLayers<'a> {
    children: NativeEncodedChildren<'a>,
    checked: bool,
}
impl<'a> Iterator for BtreeBitmapLayers<'a> {
    type Item = Result<U64BitmapEncodedLayout<'a>, AllocatorResourceError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.children.next().map(|child| {
            let child = child?;
            if self.checked {
                U64BitmapEncodedLayout::inspect(child)
            } else {
                Ok(U64BitmapEncodedLayout::ordinary(child))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::allocator_reopen_facts::AllocatorParseRecorder;
    use super::super::{BtreeBitmap, U64GroupedBitmap};
    use super::*;
    use alloc::vec::Vec;
    use std::panic::catch_unwind;

    #[test]
    // req: STO-020, REC-001
    fn native_bitmap_borrowed_layout_matches_normal_nested_materialization_and_recording() {
        let mut original = BtreeBitmap::new_padded(8193, 8193, 1_048_576);
        for bit in [0, 64, 8192] {
            original.clear(bit);
        }
        let encoded = original.to_vec();
        let decoded = BtreeBitmap::from_bytes(&encoded);
        assert_eq!(decoded.to_vec(), encoded);
        for bit in [0, 64, 8192] {
            assert!(!decoded.get(bit));
        }
        assert!(decoded.get(1));
        assert_eq!(original.heights.len(), decoded.heights.len());
        for (original, decoded) in original.heights.iter().zip(decoded.heights.iter()) {
            let encoded = original.to_vec();
            let layout = U64BitmapEncodedLayout::inspect(&encoded).unwrap();
            assert!(core::ptr::eq(layout.encoded(), encoded.as_slice()));
            assert_eq!(layout.logical_len(), original.len());
            assert_eq!(layout.words(), decoded.data.capacity());
            let bound = layout.capacity_bound().unwrap();
            assert_eq!(
                bound,
                container_bounds::vector_storage::<u64>(decoded.data.capacity()).unwrap()
            );
            let mut recorder = AllocatorParseRecorder::new();
            let parsed = U64GroupedBitmap::from_bytes_recording(&encoded, Some(&mut recorder));
            let facts = recorder.finish().unwrap();
            assert_eq!(parsed.to_vec(), encoded);
            assert_eq!(
                facts.retained().capacity_bytes(),
                layout.words() * size_of::<u64>()
            );
            assert_eq!(
                facts.retained().buffer_count(),
                usize::from(layout.words() != 0)
            );
            assert_eq!(
                facts.peak_capacity_bytes(),
                facts.retained().capacity_bytes()
            );
        }
    }

    #[test]
    // req: STO-020, REC-001
    fn native_bitmap_layout_prices_pre_resize_words_retained_after_normal_shrink() {
        let mut original = BtreeBitmap::new_padded(65537, 65537, 1_048_576);
        original.clear(0);
        original.clear(64);
        let encoded = original.to_vec();
        let mut decoded = BtreeBitmap::from_bytes(&encoded);
        let before: Vec<_> = decoded
            .heights
            .iter()
            .map(|layer| {
                let encoded = layer.to_vec();
                let layout = U64BitmapEncodedLayout::inspect(&encoded).unwrap();
                (
                    layout.words(),
                    layout.capacity_bound().unwrap(),
                    layer.data.capacity(),
                )
            })
            .collect();
        decoded.resize(65, true);
        assert!(!decoded.get(0));
        assert!(!decoded.get(64));
        assert!(decoded.get(1));
        assert!(decoded.to_vec().len() < encoded.len());
        let mut reduced = false;
        for (layer, (parsed_words, parsed_bound, actual_capacity)) in
            decoded.heights.iter().zip(before)
        {
            assert_eq!(
                layer.data.capacity(),
                actual_capacity,
                "shrink keeps actual old backing"
            );
            assert_eq!(parsed_words, actual_capacity);
            assert_eq!(
                parsed_bound,
                container_bounds::vector_storage::<u64>(layer.data.capacity()).unwrap()
            );
            let reduced_encoding = layer.to_vec();
            let reduced_layout = U64BitmapEncodedLayout::inspect(&reduced_encoding).unwrap();
            assert!(reduced_layout.words() <= parsed_words);
            reduced |= reduced_layout.words() < parsed_words;
        }
        assert!(
            reduced,
            "current logical encoding cannot price pre-resize retained capacity"
        );
    }

    #[test]
    // req: STO-020, REC-001
    fn native_bitmap_checked_framing_refuses_before_materialization_and_keeps_legacy_failures() {
        let original = U64GroupedBitmap::new_full(65, 65);
        let encoded = original.to_vec();
        for truncated in [
            &encoded[..0],
            &encoded[..1],
            &encoded[..2],
            &encoded[..3],
            &encoded[..encoded.len() - 1],
        ] {
            assert!(matches!(
                U64BitmapEncodedLayout::inspect(truncated),
                Err(AllocatorResourceError::Incomplete)
            ));
            assert!(catch_unwind(|| U64GroupedBitmap::from_bytes(truncated)).is_err());
        }
        // Framing does not invent logical-bit validation absent from the sole
        // old parser. Its ordinary owned result and bytes remain unchanged.
        let mut framed = encoded.clone();
        framed[..size_of::<u32>()].copy_from_slice(&u32::MAX.to_le_bytes());
        let layout = U64BitmapEncodedLayout::inspect(&framed).unwrap();
        let decoded = U64GroupedBitmap::from_bytes(&framed);
        assert_eq!(layout.logical_len(), decoded.len());
        assert_eq!(layout.words(), decoded.data.capacity());
    }

    #[test]
    // req: STO-020, REC-001
    fn native_bitmap_zero_word_encoding_stays_valid_and_has_no_vector_storage() {
        let original = U64GroupedBitmap::new_full(0, 0);
        let encoded = original.to_vec();
        assert_eq!(encoded.len(), size_of::<u32>());
        let layout = U64BitmapEncodedLayout::inspect(&encoded).unwrap();
        assert_eq!(layout.logical_len(), 0);
        assert_eq!(layout.words(), 0);
        assert_eq!(layout.capacity_bound().unwrap().allocator_bytes, 0);
        let mut recorder = AllocatorParseRecorder::new();
        let decoded = U64GroupedBitmap::from_bytes_recording(&encoded, Some(&mut recorder));
        let facts = recorder.finish().unwrap();
        assert_eq!(decoded.to_vec(), encoded);
        assert_eq!(decoded.data.capacity(), 0);
        assert_eq!(facts.retained().capacity_bytes(), 0);
        assert_eq!(facts.retained().buffer_count(), 0);
    }
}
