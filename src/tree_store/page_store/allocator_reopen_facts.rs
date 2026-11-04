//! Native allocator capacity facts captured by the sole existing parser.
//!
//! These are maintenance-phase observations, NOT a complete resource plan or
//! pre-allocation admission. Backend/cache/pages/aliases/allocator overhead are
//! deliberately outside this component. No durable encoding changes.

use core::mem::size_of;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AllocatorResourceError {
    Overflow,
    Incomplete,
    Stale,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AllocatorCapacityFacts {
    capacity_bytes: usize,
    buffer_count: usize,
}

impl AllocatorCapacityFacts {
    pub(crate) fn capacity_bytes(self) -> usize {
        self.capacity_bytes
    }
    pub(crate) fn buffer_count(self) -> usize {
        self.buffer_count
    }

    pub(super) fn checked_sum(self, other: Self) -> Result<Self, AllocatorResourceError> {
        Ok(Self {
            capacity_bytes: self
                .capacity_bytes
                .checked_add(other.capacity_bytes)
                .ok_or(AllocatorResourceError::Overflow)?,
            buffer_count: self
                .buffer_count
                .checked_add(other.buffer_count)
                .ok_or(AllocatorResourceError::Overflow)?,
        })
    }

    #[cfg(test)]
    fn for_test(capacity_bytes: usize, buffer_count: usize) -> Self {
        Self {
            capacity_bytes,
            buffer_count,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AllocatorParseFacts {
    retained: AllocatorCapacityFacts,
    maximum_allocated_buffer_bytes: usize,
    peak_capacity_bytes: usize,
    peak_buffer_count: usize,
    region_records: usize,
    encoded_region_bytes: usize,
    maximum_encoded_region_bytes: usize,
    encoded_tracker_bytes: usize,
}

impl AllocatorParseFacts {
    pub(crate) fn retained(self) -> AllocatorCapacityFacts {
        self.retained
    }
    pub(crate) fn maximum_allocated_buffer_bytes(self) -> usize {
        self.maximum_allocated_buffer_bytes
    }
    pub(crate) fn peak_capacity_bytes(self) -> usize {
        self.peak_capacity_bytes
    }
    pub(crate) fn peak_buffer_count(self) -> usize {
        self.peak_buffer_count
    }
    pub(crate) fn region_records(self) -> usize {
        self.region_records
    }
    pub(crate) fn encoded_region_bytes(self) -> usize {
        self.encoded_region_bytes
    }
    pub(crate) fn maximum_encoded_region_bytes(self) -> usize {
        self.maximum_encoded_region_bytes
    }
    pub(crate) fn encoded_tracker_bytes(self) -> usize {
        self.encoded_tracker_bytes
    }

    pub(super) fn with_resize_after(
        self,
        final_retained: AllocatorCapacityFacts,
    ) -> Result<AllocatorReopenFacts, AllocatorResourceError> {
        // Conservative coexistence for resize: old graph plus complete final
        // graph covers every monotone Vec replacement in the existing resize.
        // This does not admit those allocations; the full factory does that.
        let both = self.retained.checked_sum(final_retained)?;
        Ok(AllocatorReopenFacts {
            parsed: self,
            final_retained,
            peak_capacity_bytes: self.peak_capacity_bytes.max(both.capacity_bytes),
            peak_buffer_count: self.peak_buffer_count.max(both.buffer_count),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AllocatorReopenFacts {
    parsed: AllocatorParseFacts,
    final_retained: AllocatorCapacityFacts,
    peak_capacity_bytes: usize,
    peak_buffer_count: usize,
}

impl AllocatorReopenFacts {
    pub(crate) fn parsed(self) -> AllocatorCapacityFacts {
        self.parsed.retained
    }
    pub(crate) fn parse_facts(self) -> AllocatorParseFacts {
        self.parsed
    }
    pub(crate) fn final_retained(self) -> AllocatorCapacityFacts {
        self.final_retained
    }
    pub(crate) fn peak_capacity_bytes(self) -> usize {
        self.peak_capacity_bytes
    }
    pub(crate) fn peak_buffer_count(self) -> usize {
        self.peak_buffer_count
    }
}

pub(super) struct AllocatorParseRecorder {
    current_bytes: usize,
    current_buffers: usize,
    maximum_buffer_bytes: usize,
    peak_capacity_bytes: usize,
    peak_buffer_count: usize,
    region_records: usize,
    encoded_region_bytes: usize,
    maximum_encoded_region_bytes: usize,
    encoded_tracker_bytes: usize,
    error: Option<AllocatorResourceError>,
}

impl AllocatorParseRecorder {
    pub(super) fn new() -> Self {
        Self {
            current_bytes: 0,
            current_buffers: 0,
            maximum_buffer_bytes: 0,
            peak_capacity_bytes: 0,
            peak_buffer_count: 0,
            region_records: 0,
            encoded_region_bytes: 0,
            maximum_encoded_region_bytes: 0,
            encoded_tracker_bytes: 0,
            error: None,
        }
    }

    // Called around the ORIGINAL Vec allocations by the ORIGINAL parser. The
    // conservative peak includes both buffers even if realloc grew in place.
    pub(super) fn replace_vec<T>(&mut self, old_capacity: usize, new_capacity: usize) {
        if self.error.is_some() || old_capacity == new_capacity {
            return;
        }
        let result = (|| {
            let old = old_capacity
                .checked_mul(size_of::<T>())
                .ok_or(AllocatorResourceError::Overflow)?;
            let new = new_capacity
                .checked_mul(size_of::<T>())
                .ok_or(AllocatorResourceError::Overflow)?;
            let peak = self
                .current_bytes
                .checked_add(new)
                .ok_or(AllocatorResourceError::Overflow)?;
            let peak_buffers = self
                .current_buffers
                .checked_add(usize::from(new != 0))
                .ok_or(AllocatorResourceError::Overflow)?;
            self.current_bytes = self
                .current_bytes
                .checked_sub(old)
                .ok_or(AllocatorResourceError::Incomplete)?
                .checked_add(new)
                .ok_or(AllocatorResourceError::Overflow)?;
            self.current_buffers = self
                .current_buffers
                .checked_sub(usize::from(old != 0))
                .ok_or(AllocatorResourceError::Incomplete)?
                .checked_add(usize::from(new != 0))
                .ok_or(AllocatorResourceError::Overflow)?;
            self.peak_capacity_bytes = self.peak_capacity_bytes.max(peak);
            self.peak_buffer_count = self.peak_buffer_count.max(peak_buffers);
            self.maximum_buffer_bytes = self.maximum_buffer_bytes.max(new);
            Ok(())
        })();
        if let Err(error) = result {
            self.error = Some(error);
        }
    }

    pub(super) fn region_record(&mut self, bytes: usize) {
        if self.error.is_some() {
            return;
        }
        match self
            .region_records
            .checked_add(1)
            .zip(self.encoded_region_bytes.checked_add(bytes))
        {
            Some((records, encoded)) => {
                self.region_records = records;
                self.encoded_region_bytes = encoded;
                self.maximum_encoded_region_bytes = self.maximum_encoded_region_bytes.max(bytes);
            }
            None => self.error = Some(AllocatorResourceError::Overflow),
        }
    }

    pub(super) fn tracker_record(&mut self, bytes: usize) {
        if self.encoded_tracker_bytes != 0 {
            self.error = Some(AllocatorResourceError::Incomplete);
        }
        self.encoded_tracker_bytes = bytes;
    }

    pub(super) fn finish(self) -> Result<AllocatorParseFacts, AllocatorResourceError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(AllocatorParseFacts {
            retained: AllocatorCapacityFacts {
                capacity_bytes: self.current_bytes,
                buffer_count: self.current_buffers,
            },
            maximum_allocated_buffer_bytes: self.maximum_buffer_bytes,
            peak_capacity_bytes: self.peak_capacity_bytes,
            peak_buffer_count: self.peak_buffer_count,
            region_records: self.region_records,
            encoded_region_bytes: self.encoded_region_bytes,
            maximum_encoded_region_bytes: self.maximum_encoded_region_bytes,
            encoded_tracker_bytes: self.encoded_tracker_bytes,
        })
    }
}

#[cfg(test)]
#[path = "allocator_reopen_facts_tests.rs"]
mod tests;
