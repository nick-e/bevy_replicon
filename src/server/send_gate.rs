//! Value-based gating of mutation sends.
//!
//! Bevy's change tick records that a component was written, not that its value
//! differs from the one already there. A write that stores the value the
//! component already holds advances the tick, and a value that leaves and
//! returns within one send interval advances it twice, so
//! [`collect_changes`](super::collect_changes) schedules a mutation carrying a
//! value every client already has.
//!
//! This gate keeps, for each replicated component of each entity, the
//! serialized bytes of the last value that actually differed and the tick it
//! differed on. The send decision then compares that tick against the tick the
//! receiving client acknowledged, rather than comparing Bevy's write tick.
//!
//! Comparing the acknowledged tick is what preserves retransmission. Mutations
//! are unreliable, and [`ClientTicks::ack_mutate_message`] advances a client's
//! tick only when that client acknowledges the message carrying the component.
//! An unacknowledged component therefore keeps a tick older than
//! [`GateEntry::last_change`] and keeps being sent. Suppressing the send
//! directly, by comparing against the previous tick's value instead, would drop
//! a component whose only delivery was lost and leave that client holding a
//! stale value with nothing to correct it.
//!
//! The record is per component instance rather than per client, because whether
//! a value changed does not depend on who is receiving it.

use bevy::{
    ecs::change_detection::{CheckChangeTicks, Tick},
    platform::collections::HashMap,
    prelude::*,
};

use crate::shared::replication::registry::FnsId;

/// Serialized value and change tick for every replicated component instance.
#[derive(Resource, Default)]
pub(crate) struct SendGate {
    entries: HashMap<(Entity, FnsId), GateEntry>,
}

impl SendGate {
    /// Returns the tick on which this component's value last differed.
    ///
    /// `serialized` holds the component's current value at `range`. `changed`
    /// is Bevy's change tick for the component, used only to skip re-comparing
    /// a component nothing has written since the last evaluation.
    pub(crate) fn last_change(
        &mut self,
        entity: Entity,
        fns_id: FnsId,
        changed: Tick,
        serialized: &[u8],
    ) -> Tick {
        let entry = self.entries.entry((entity, fns_id)).or_insert(GateEntry {
            bytes: Vec::new(),
            last_change: changed,
            evaluated_at: None,
        });

        if entry.evaluated_at != Some(changed) {
            entry.evaluated_at = Some(changed);
            if entry.bytes != serialized {
                entry.bytes.clear();
                entry.bytes.extend_from_slice(serialized);
                entry.last_change = changed;
            }
        }

        entry.last_change
    }

    /// Drops the records for `entity`.
    pub(crate) fn remove_entity(&mut self, entity: Entity) {
        self.entries.retain(|(recorded, _), _| *recorded != entity);
    }

    /// Rebases the stored ticks so they stay comparable after Bevy wraps its
    /// change ticks.
    pub(crate) fn check_ticks(&mut self, check: CheckChangeTicks) {
        for entry in self.entries.values_mut() {
            entry.last_change.check_tick(check);
            if let Some(evaluated_at) = &mut entry.evaluated_at {
                evaluated_at.check_tick(check);
            }
        }
    }
}

/// One component instance's last differing value.
struct GateEntry {
    /// Serialized bytes of that value.
    bytes: Vec<u8>,

    /// Tick the value last differed on.
    last_change: Tick,

    /// Bevy change tick this entry was last compared against.
    evaluated_at: Option<Tick>,
}
