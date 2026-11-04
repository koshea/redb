//! Test-only owning vector observations. No page/free/Source refund claims.
use alloc::sync::Arc;
use std::sync::Mutex;

#[derive(Default, Clone, Copy, Debug)]
pub(crate) struct NativePointTraceSnapshot {
    pub(crate) current_bytes: usize,
    pub(crate) peak_bytes: usize,
    pub(crate) leaf_live: usize,
    pub(crate) branch_live: usize,
    pub(crate) leaf_peak: usize,
    pub(crate) branch_peak: usize,
    pub(crate) maximum_leaf_pairs: usize,
    pub(crate) maximum_branch_children: usize,
    pub(crate) maximum_branch_keys: usize,
    pub(crate) partial_scopes: usize,
    pub(crate) conversions: usize,
}
#[derive(Default)]
pub(crate) struct NativePointMutatorTrace(Mutex<NativePointTraceSnapshot>);
impl NativePointMutatorTrace {
    pub(crate) fn snapshot(&self) -> NativePointTraceSnapshot {
        *self.0.lock().unwrap()
    }
    // kind: leaf, branch, partial; capacities are native Vec::capacity values.
    pub(crate) fn scope(
        self: &Arc<Self>,
        kind: u8,
        bytes: usize,
        first: usize,
        second: usize,
    ) -> NativePointTraceScope {
        let mut s = self.0.lock().unwrap();
        s.current_bytes = s.current_bytes.checked_add(bytes).unwrap();
        s.peak_bytes = s.peak_bytes.max(s.current_bytes);
        match kind {
            0 => {
                s.leaf_live += 1;
                s.leaf_peak = s.leaf_peak.max(s.leaf_live);
                s.maximum_leaf_pairs = s.maximum_leaf_pairs.max(first);
            }
            1 => {
                s.branch_live += 1;
                s.branch_peak = s.branch_peak.max(s.branch_live);
                s.maximum_branch_children = s.maximum_branch_children.max(first);
                s.maximum_branch_keys = s.maximum_branch_keys.max(second);
            }
            2 => {
                s.partial_scopes += 1;
            }
            _ => unreachable!(),
        }
        NativePointTraceScope {
            trace: self.clone(),
            kind,
            bytes,
        }
    }
    pub(crate) fn conversion(&self, new_outer_capacity: usize, new_keys_capacity_bytes: usize) {
        let mut s = self.0.lock().unwrap();
        s.conversions += 1;
        // Existing builder Cow/children and caller PartialBranch remain live.
        // Conservatively keep all of those while counting the actual new owned
        // key forest. This does not depend on an in-place collect optimization.
        let extra = new_outer_capacity
            .checked_mul(core::mem::size_of::<alloc::vec::Vec<u8>>())
            .unwrap()
            .checked_add(new_keys_capacity_bytes)
            .unwrap();
        s.peak_bytes = s
            .peak_bytes
            .max(s.current_bytes.checked_add(extra).unwrap());
    }
}
pub(crate) struct NativePointTraceScope {
    trace: Arc<NativePointMutatorTrace>,
    kind: u8,
    bytes: usize,
}
impl NativePointTraceScope {
    pub(crate) fn observe_capacity(&mut self, bytes: usize, first: usize, second: usize) {
        let mut s = self.trace.0.lock().unwrap();
        if bytes > self.bytes {
            // A real Vec growth may keep old storage while allocating new.
            s.peak_bytes = s
                .peak_bytes
                .max(s.current_bytes.checked_add(bytes).unwrap());
            s.current_bytes = s
                .current_bytes
                .checked_sub(self.bytes)
                .unwrap()
                .checked_add(bytes)
                .unwrap();
            self.bytes = bytes;
        }
        if self.kind == 0 {
            s.maximum_leaf_pairs = s.maximum_leaf_pairs.max(first);
        }
        if self.kind == 1 {
            s.maximum_branch_children = s.maximum_branch_children.max(first);
            s.maximum_branch_keys = s.maximum_branch_keys.max(second);
        }
    }
}
impl Drop for NativePointTraceScope {
    fn drop(&mut self) {
        let mut s = self.trace.0.lock().unwrap();
        s.current_bytes = s.current_bytes.checked_sub(self.bytes).unwrap();
        if self.kind == 0 {
            s.leaf_live -= 1;
        }
        if self.kind == 1 {
            s.branch_live -= 1;
        }
    }
}
