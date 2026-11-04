//! Bounded actual copied namespace incarnations, cfgtest ONLY.
//! No callback/retainer engine backedge or caller-adopted root exists.
use super::*;
use crate::tree_store::NativeFixedPhasePage as PageImpl;

pub(crate) struct NativeFixedPhaseWitness {
    copies: [Option<(TransactionId, BtreeHeader, PageImpl)>; 3],
}
impl NativeFixedPhaseWitness {
    pub(in crate::transactions) fn empty() -> Self {
        Self {
            copies: [None, None, None],
        }
    }
    pub(in crate::transactions) fn retain_actual(
        &mut self,
        phase: usize,
        transaction: TransactionId,
        header: BtreeHeader,
        page: PageImpl,
    ) {
        assert!(phase < self.copies.len() && self.copies[phase].is_none());
        assert_eq!(page.get_page_number(), header.root);
        self.copies[phase] = Some((transaction, header, page));
    }
    pub(crate) fn original_bytes_for_fixture(&self) -> usize {
        self.copies[0].as_ref().unwrap().2.memory().len()
    }
    pub(crate) fn live_bytes_for_fixture(&self) -> usize {
        self.copies
            .iter()
            .flatten()
            .map(|(_, _, p)| p.memory().len())
            .sum()
    }
    pub(crate) fn require_three_distinct_backings_for_fixture(&self) {
        let copies = self
            .copies
            .each_ref()
            .map(|copy| copy.as_ref().expect("actual owning phase copy"));
        for a in 0..3 {
            for b in (a + 1)..3 {
                // Physical identity: PN equality is deliberately irrelevant.
                assert_ne!(copies[a].2.memory().as_ptr(), copies[b].2.memory().as_ptr());
            }
        }
        // Phase classification comes from actual native publication boundaries,
        // not a PN-set subtraction. The genuine history changes namespace roots.
        assert_ne!(
            copies[0].1, copies[1].1,
            "actual main committed incarnation"
        );
        assert_ne!(
            copies[1].1, copies[2].1,
            "actual post committed incarnation"
        );
    }
    pub(in crate::transactions) fn actual_main_for_fixture(&self) -> (TransactionId, BtreeHeader) {
        let (transaction, header, _) = self.copies[1].as_ref().unwrap();
        (*transaction, *header)
    }
    pub(crate) fn legacy_original_namespace_misses_actual_main_for_fixture(&self) -> bool {
        let original = self.copies[0].as_ref().unwrap();
        let main = self.copies[1].as_ref().unwrap();
        // Both causally captured native incarnations, never a fabricated PN.
        original.0 != main.0 && original.1 != main.1
    }
    pub(crate) fn drop_copies_for_fixture(&mut self) {
        for copy in &mut self.copies {
            drop(copy.take());
        }
    }
}
impl WriteTransaction {
    pub(crate) fn install_fixed_phase_witness_for_fixture(
        &mut self,
        witness: NativeOwnedShared<Mutex<NativeFixedPhaseWitness>>,
    ) {
        assert!(self.checked_source_immediate);
        self.fixed_phase_witness = Some(witness);
    }
    pub(super) fn retain_fixed_phase_for_fixture(&self, phase: usize) -> Result<()> {
        if let Some(witness) = &self.fixed_phase_witness {
            let header = self.mem.get_system_root().expect("genuine namespace root");
            // Actual native writer exclusion is held; this is a fresh admitted
            // Source-private immutable copy, not a leaked mutable/cached alias.
            let page = self
                .page_allocator()
                .resolver()
                .get_page(header.root, PageHint::Clean)?;
            witness.lock()?.retain_actual(
                phase,
                self.mem.get_last_committed_transaction_id()?,
                header,
                page,
            );
        }
        Ok(())
    }
}
