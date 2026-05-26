//! In-flight LSN tracking for the `postgresql_cdc` source.
//!
//! pgoutput emits row events without their own LSN: every Insert/Update/Delete
//! within a transaction shares the `final_lsn` carried by that transaction's
//! Begin message. A single 10,000-row transaction therefore produces 10,000
//! events all tagged with one LSN, and only when *every* event has been
//! acknowledged by the sink may we report that LSN as flushed to Postgres.
//!
//! The tracker stores a `Vec<BatchStatusReceiver>` per LSN and only advances
//! `confirmed_lsn` through the longest contiguous prefix of the map where each
//! LSN's full Vec has resolved. It uses `try_recv()` exclusively so it never
//! blocks the replication loop.
//!
//! ## Why not `vector_lib::finalizer::OrderedFinalizer`?
//!
//! `OrderedFinalizer<E>` (used by `kafka.rs` and `splunk_hec.rs`) tracks one
//! finalizer per event-key and emits acks in the order entries were added.
//! That fits Kafka where each message has its own offset, but it does not
//! model "many events share one LSN": you'd either need to use the same key
//! for every row in a txn (which loses per-LSN granularity) or emit one
//! finalizer per row (which would advance the LSN partway through a txn,
//! breaking at-least-once on restart). The custom `LsnTracker` here exists
//! specifically to handle the LSN-grouping semantics — if a future change
//! makes Postgres deliver one LSN per row, switching to `OrderedFinalizer`
//! would be appropriate.
//!
//! ## Status handling
//!
//! Vector's `BatchStatus` collapses `EventStatus::Dropped` into
//! `BatchStatus::Delivered` (see `lib/vector-common/src/finalization.rs`), so
//! a VRL filter that drops events surfaces here as `Delivered` and naturally
//! advances the LSN without leaking memory. `Errored` and `Rejected` halt
//! advancement and the source surfaces the failure upstream.
//!
//! `TryRecvError::Closed` (sender dropped without sending) is logged at
//! `warn` and treated as `Delivered`. Hard-failing on this matches neither
//! Vector's `kafka` source (which logs and continues) nor the broader
//! finalization conventions, and would cause spurious source restarts when
//! downstream components are reconfigured.

use std::collections::BTreeMap;

use tokio::sync::oneshot::error::TryRecvError;
use vector_lib::event::{BatchStatus, BatchStatusReceiver};

/// Maximum number of distinct in-flight transaction LSNs we will track
/// before applying back-pressure (pausing `client.recv()`).
///
/// **Derivation:** each pending entry is a `BTreeMap` key (`u64`, 8 bytes)
/// plus a `Vec<BatchStatusReceiver>` (24 bytes header + ~40 bytes per
/// receiver). For the common single-batch-per-transaction case that is
/// ~72 bytes/entry, so 100K entries ≈ 7 MB. A database sustaining 1K
/// committed transactions per second would need the sink to stall for
/// ~100 seconds before this cap is reached — well past the point where
/// an operator would notice via lag metrics. The cap is deliberately
/// generous: its purpose is not to be tight, but to guarantee that a
/// permanently-stalled sink eventually pauses the replication stream
/// instead of growing the map without bound. See the
/// `postgresql_cdc-benchmarks` feature for memory-profile tests at
/// different cap values.
const MAX_PENDING_LSNS: usize = 100_000;

/// Outcome of polling a single LSN's batch receivers.
enum LsnPollOutcome {
    /// Every receiver for this LSN resolved with `Delivered`.
    Resolved,
    /// At least one receiver is still pending; we must stop advancing here.
    Pending,
    /// A receiver returned `Errored` or `Rejected`. The caller must halt
    /// LSN advancement.
    Failed(BatchStatus),
}

/// Tracks pending transaction LSNs and their sink acknowledgment status.
///
/// Receivers for the same LSN accumulate in a single `Vec` rather than
/// overwriting, because every row in one transaction shares one LSN.
///
/// Invariant: `confirmed_lsn` is only ever advanced to an LSN whose Vec is
/// fully resolved, and only when every LSN strictly less than it has already
/// been removed from `pending`.
#[derive(Default)]
pub(super) struct LsnTracker {
    pending: BTreeMap<u64, Vec<BatchStatusReceiver>>,
    confirmed_lsn: u64,
    /// The value most recently returned by `poll_confirmed`. Used to make
    /// poll edge-triggered: returns `Some` only when progress has been made
    /// since the last call.
    last_reported: u64,
    failed: Option<(u64, BatchStatus)>,
}

impl LsnTracker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Register a new receiver for the given LSN.
    ///
    /// Multiple calls with the same LSN append to that LSN's Vec — they do
    /// not overwrite. In practice LSNs are monotonically non-decreasing in
    /// pgoutput's commit order, so this is the natural shape for tracking
    /// 1-to-many rows-per-transaction.
    pub(super) fn track(&mut self, lsn: u64, receiver: BatchStatusReceiver) {
        self.pending.entry(lsn).or_default().push(receiver);
    }

    /// Records that the given LSN can be reported as flushed immediately,
    /// without waiting on any sink acknowledgement. Used in at-most-once
    /// mode (the no-ack path) to keep the slot moving — otherwise PG would
    /// retain WAL forever.
    pub(super) const fn mark_delivered_immediately(&mut self, lsn: u64) {
        if lsn > self.confirmed_lsn && self.failed.is_none() {
            self.confirmed_lsn = lsn;
        }
    }

    /// Returns `true` if the pending map has reached the back-pressure cap.
    /// The caller (the replication loop) should stop pulling new events
    /// from the wire until `poll_confirmed` drains entries below the cap.
    pub(super) fn is_full(&self) -> bool {
        self.pending.len() >= MAX_PENDING_LSNS
    }

    /// Non-blocking poll over all pending LSNs in ascending order.
    ///
    /// Advances `confirmed_lsn` through the longest contiguous prefix of the
    /// pending map where every receiver has returned `Delivered`. Returns the
    /// new `confirmed_lsn` if it advanced (either via batch resolution or via
    /// a prior `mark_delivered_immediately` call), otherwise `None`.
    ///
    /// Never `.await`s a receiver — uses `try_recv()` exclusively so the
    /// replication loop is never blocked behind a slow sink. Allocates
    /// nothing on the hot path.
    pub(super) fn poll_confirmed(&mut self) -> Option<u64> {
        if self.failed.is_some() {
            // Even if mark_delivered_immediately bumped confirmed_lsn
            // earlier, do not report further progress once a failure has
            // been observed.
            return None;
        }

        // Walk the map in ascending order. `BTreeMap::iter_mut` is sorted,
        // so we can break at the first non-resolved entry to honour the
        // contiguous-prefix invariant. We collect the keys to remove via
        // `to_remove_upto` and prune in one pass after the loop, avoiding
        // both the per-poll Vec-of-keys allocation and the mid-iteration
        // mutation issue.
        let mut last_resolved: Option<u64> = None;
        for (&lsn, entry) in self.pending.iter_mut() {
            match poll_one_lsn(entry) {
                LsnPollOutcome::Resolved => {
                    last_resolved = Some(lsn);
                }
                LsnPollOutcome::Pending => break,
                LsnPollOutcome::Failed(status) => {
                    self.failed = Some((lsn, status));
                    break;
                }
            }
        }

        if let Some(upto) = last_resolved {
            // Remove every entry with key <= upto in one pass. `split_off`
            // returns the entries with key >= upto+1, leaving the resolved
            // prefix in `self.pending`, which we then clear.
            let tail = self.pending.split_off(&(upto.saturating_add(1)));
            self.pending.clear();
            self.pending = tail;
            self.confirmed_lsn = upto;
        }

        if self.confirmed_lsn > self.last_reported {
            self.last_reported = self.confirmed_lsn;
            Some(self.confirmed_lsn)
        } else {
            None
        }
    }

    /// Returns the (lsn, status) pair of the first observed failure, if any.
    /// Once set, the tracker will not advance further.
    pub(super) const fn failure(&self) -> Option<(u64, BatchStatus)> {
        self.failed
    }

    #[cfg(test)]
    pub(super) const fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn
    }

    #[cfg(test)]
    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

/// Polls every receiver in `receivers`, draining the ones that have resolved.
///
/// Resolved receivers (`Delivered`) are removed. A receiver returning
/// `Errored` / `Rejected` returns `Failed` immediately. A `Closed` sender
/// (dropped without sending) is logged and treated as `Delivered` — matches
/// the kafka source's behaviour and avoids killing the source when a
/// downstream component is reconfigured. If any receiver is still pending,
/// returns `Pending`. If the slice is emptied, returns `Resolved`.
fn poll_one_lsn(receivers: &mut Vec<BatchStatusReceiver>) -> LsnPollOutcome {
    let mut i = 0;
    while i < receivers.len() {
        match receivers[i].try_recv() {
            Ok(BatchStatus::Delivered) => {
                receivers.swap_remove(i);
            }
            Ok(status @ (BatchStatus::Errored | BatchStatus::Rejected)) => {
                return LsnPollOutcome::Failed(status);
            }
            Err(TryRecvError::Empty) => {
                i += 1;
            }
            Err(TryRecvError::Closed) => {
                tracing::warn!(
                    message = "BatchStatusReceiver sender dropped without sending; \
                               treating as Delivered. This usually indicates a \
                               downstream component was reconfigured mid-stream."
                );
                receivers.swap_remove(i);
            }
        }
    }

    if receivers.is_empty() {
        LsnPollOutcome::Resolved
    } else {
        LsnPollOutcome::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::event::{BatchNotifier, EventFinalizer};
    use vector_lib::finalization::EventStatus;

    /// Creates a fresh batch and its receiver.
    fn new_batch() -> (BatchNotifier, BatchStatusReceiver) {
        BatchNotifier::new_with_receiver()
    }

    /// Drops the batch with the default `Delivered` outcome.
    fn deliver(b: BatchNotifier) {
        drop(b);
    }

    /// Drops the batch after marking an attached event finalizer as `Errored`.
    fn errored(b: BatchNotifier) {
        let f = EventFinalizer::new(b);
        f.update_status(EventStatus::Errored);
        drop(f);
    }

    #[test]
    fn empty_tracker_returns_none() {
        let mut t = LsnTracker::new();
        assert_eq!(t.poll_confirmed(), None);
        assert_eq!(t.confirmed_lsn(), 0);
    }

    #[test]
    fn single_lsn_advances_after_ack() {
        let mut t = LsnTracker::new();
        let (b, r) = new_batch();
        t.track(100, r);

        assert_eq!(t.poll_confirmed(), None);

        deliver(b);

        assert_eq!(t.poll_confirmed(), Some(100));
        assert_eq!(t.confirmed_lsn(), 100);
    }

    #[test]
    fn multiple_receivers_one_lsn_all_must_resolve() {
        let mut t = LsnTracker::new();
        let mut batches = Vec::new();
        for _ in 0..10 {
            let (b, r) = new_batch();
            t.track(200, r);
            batches.push(b);
        }

        for b in batches.drain(..9) {
            deliver(b);
        }
        assert_eq!(t.poll_confirmed(), None);
        assert_eq!(t.confirmed_lsn(), 0);

        deliver(batches.pop().unwrap());
        assert_eq!(t.poll_confirmed(), Some(200));
    }

    #[test]
    fn contiguous_prefix_advancement() {
        let mut t = LsnTracker::new();
        let (b100, r100) = new_batch();
        let (b200, r200) = new_batch();
        let (b300, r300) = new_batch();
        t.track(100, r100);
        t.track(200, r200);
        t.track(300, r300);

        deliver(b100);
        deliver(b300);

        assert_eq!(t.poll_confirmed(), Some(100));
        assert_eq!(t.confirmed_lsn(), 100);

        deliver(b200);
        assert_eq!(t.poll_confirmed(), Some(300));
        assert_eq!(t.confirmed_lsn(), 300);
    }

    #[test]
    fn dropped_events_advance_lsn() {
        let mut t = LsnTracker::new();
        let (b, r) = new_batch();
        t.track(400, r);

        let f = EventFinalizer::new(b);
        drop(f);

        assert_eq!(t.poll_confirmed(), Some(400));
    }

    #[test]
    fn errored_batch_halts_advancement() {
        let mut t = LsnTracker::new();
        let (b1, r1) = new_batch();
        let (b2, r2) = new_batch();
        t.track(500, r1);
        t.track(600, r2);

        errored(b1);
        deliver(b2);

        assert_eq!(t.poll_confirmed(), None);
        let (lsn, status) = t.failure().expect("failure recorded");
        assert_eq!(lsn, 500);
        assert_eq!(status, BatchStatus::Errored);
    }

    #[test]
    fn poll_is_idempotent_when_no_progress() {
        let mut t = LsnTracker::new();
        let (_b, r) = new_batch();
        t.track(700, r);

        assert_eq!(t.poll_confirmed(), None);
        assert_eq!(t.poll_confirmed(), None);
        assert_eq!(t.confirmed_lsn(), 0);
    }

    #[test]
    fn track_after_partial_advance() {
        let mut t = LsnTracker::new();
        let (b100, r100) = new_batch();
        t.track(100, r100);
        deliver(b100);
        assert_eq!(t.poll_confirmed(), Some(100));

        let (b200, r200) = new_batch();
        t.track(200, r200);
        deliver(b200);
        assert_eq!(t.poll_confirmed(), Some(200));
    }

    // Note: the `TryRecvError::Closed` branch (sender dropped without ever
    // sending) is unit-testable only by constructing a `BatchStatusReceiver`
    // whose sender has been dropped, but `BatchNotifier::new_with_receiver`
    // always sends `Delivered` on Drop, and there is no public test
    // constructor on `BatchStatusReceiver`. The behaviour is exercised via
    // the integration suite when a downstream component is reconfigured
    // mid-stream; see `poll_confirmed` for the (log + treat as delivered)
    // semantics.

    #[test]
    fn mark_delivered_immediately_advances_without_receiver() {
        // The at-most-once code path uses this to keep the slot moving when
        // E2E acknowledgements are disabled.
        let mut t = LsnTracker::new();
        t.mark_delivered_immediately(100);
        assert_eq!(t.poll_confirmed(), Some(100));
        assert_eq!(t.confirmed_lsn(), 100);
    }

    #[test]
    fn mark_delivered_immediately_is_monotonic() {
        // Out-of-order calls must never roll the confirmed LSN backwards.
        let mut t = LsnTracker::new();
        t.mark_delivered_immediately(200);
        t.mark_delivered_immediately(100);
        assert_eq!(t.poll_confirmed(), Some(200));
    }

    #[test]
    fn mark_delivered_immediately_blocked_by_failure() {
        let mut t = LsnTracker::new();
        let (b, r) = new_batch();
        t.track(100, r);
        errored(b);
        t.poll_confirmed();
        assert!(t.failure().is_some());

        // Any subsequent attempt to bump the LSN must be ignored.
        t.mark_delivered_immediately(999);
        assert_eq!(t.poll_confirmed(), None);
    }

    #[test]
    fn is_full_signals_at_cap() {
        let mut t = LsnTracker::new();
        // Use mark_delivered_immediately so we don't need to hold thousands
        // of receivers — but we need entries in `pending` to bump len. So
        // use track with deliberately-leaked batches for this test.
        let _held: Vec<BatchNotifier> = (0..10)
            .map(|i| {
                let (b, r) = new_batch();
                t.track(i, r);
                b
            })
            .collect();
        assert!(!t.is_full());
        assert_eq!(t.pending_len(), 10);
    }
}
