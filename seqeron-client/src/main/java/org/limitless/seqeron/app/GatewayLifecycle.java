package org.limitless.seqeron.app;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.limitless.seqeron.protocol.SnapshotHeader;

/**
 * The election lifecycle of one gateway instance: which instance of a logical gateway opens its gate, and
 * when it stops. The gate is whatever the edge does to serve — an acceptor binds, an initiator dials.
 *
 * <p><b>{@code GatewayActive} is a state assignment, not an event.</b> The last one naming any instance of
 * this pair is in force, so this tracks rather than latches: a restarting instance replays superseded
 * activations and must not re-activate itself off one.
 *
 * <p><b>{@code GatewayStarted} is published once per activation, and before the gate opens.</b> It makes
 * the sequencer release the connections a predecessor left open; opening first would let this instance's
 * own {@code ConnectionOpened} frames precede it and be released as stale.
 *
 * <p><b>Standing down publishes nothing</b>, so to the cluster it looks like the process dying. The cluster
 * session is kept and the instance can be activated again.
 *
 * <p>Single-threaded. The C++ twin is {@code app/GatewayLifecycle.hpp}; keep the two in step.
 */
final class GatewayLifecycle {
    /** No {@code GatewayRegistered} row has named this instance yet. */
    public static final int UNRESOLVED = -1;

    /** Where this instance stands. */
    public enum State {
        /** Following the tap through history; the gate stays shut whatever the activation says. */
        REPLAYING,
        /** Caught up with the gate shut. Where a standby sits and a superseded instance lands. */
        PASSIVE,
        /** The gate is open. */
        SERVING
    }

    /** What the lifecycle cannot do itself. */
    public interface Actions {
        /**
         * Publishes this instance's {@code GatewayStarted}.
         * @return whether it landed; false is back-pressure, retried on {@link #advance()}
         */
        boolean publishGatewayStarted(int gatewayId);

        /**
         * Opens the gate: the instance begins accepting connections.
         * @return whether the gate opened; false is retried on {@link #advance()}
         */
        boolean openGate();

        /** Closes the gate and drops every session it let in. Publishes nothing. */
        void closeGate();
    }

    private final String gatewayName;
    private final Actions actions;

    /**
     * Every row by {@code gatewayId}, in list order, a re-published row replacing its earlier one in place as the
     * sequencer's list does: a {@code GatewayActive} carries only the id, and a snapshot header every row of the pair.
     */
    private final Map<Integer, Row> rows = new LinkedHashMap<>();

    private State state = State.REPLAYING;
    private int gatewayId = UNRESOLVED;
    private int gatewaySourceId = UNRESOLVED;
    private boolean activated;
    private boolean registered;

    /** The instance of this pair the last {@code GatewayActive} named, or {@link SnapshotHeader#NO_GATEWAY}. */
    private int activeGatewayId = SnapshotHeader.NO_GATEWAY;

    /** One list row. */
    private record Row(int gatewaySourceId, int preferenceRank, String gatewayName) { }

    /**
     * An instance that starts out replaying, with no identity until the list names it.
     *
     * @param gatewayName the {@code GatewayRegistered} row name this instance joins on
     * @param actions     what this class cannot do itself
     */
    public GatewayLifecycle(final String gatewayName, final Actions actions) {
        this.gatewayName = gatewayName;
        this.actions = actions;
    }

    /**
     * The tap reached the live frontier for the first time.
     * @return whether this left {@code REPLAYING}
     */
    public boolean onCaughtUp() {
        if (state != State.REPLAYING) {
            return false;
        }
        state = State.PASSIVE;
        return true;
    }

    /** A {@code GatewayRegistered} row. Only the one carrying this instance's name resolves its identity. */
    public void onGatewayRegistered(final int rowGatewayId, final int rowGatewaySourceId, final String rowName,
                                    final int preferenceRank) {
        rows.put(rowGatewayId, new Row(rowGatewaySourceId, preferenceRank, rowName));
        if (!gatewayName.equals(rowName)) {
            return;
        }
        gatewayId = rowGatewayId;
        gatewaySourceId = rowGatewaySourceId;
    }

    /** A {@code GatewayActive}. One naming a sibling stands this instance down; another pair's is ignored. */
    public void onGatewayActive(final int targetGatewayId) {
        if (gatewayId == UNRESOLVED) {
            return;
        }
        final Row target = rows.get(targetGatewayId);
        if (target == null || target.gatewaySourceId() != gatewaySourceId) {
            return;
        }
        activeGatewayId = targetGatewayId;
        final boolean wasActivated = activated;
        activated = targetGatewayId == gatewayId;
        if (activated || !wasActivated) {
            return;
        }
        registered = false; // being asked back is a new epoch
        if (state == State.SERVING) {
            state = State.PASSIVE;
            actions.closeGate();
        }
    }

    /**
     * Takes the election state a snapshot header holds in place of the frames before its cut (doc/snapshot.md §6):
     * the pair's rows, which resolve this instance's identity by name, and the instance its {@code GatewayActive}
     * names. Before the gate has opened, so nothing is published or closed.
     */
    public void onSnapshotHeader(final SnapshotHeader.GatewayState state) {
        rows.clear();
        for (final SnapshotHeader.GatewayRow row : state.rows()) {
            onGatewayRegistered(row.gatewayId(), state.gatewaySourceId(), row.gatewayName(), row.preferenceRank());
        }
        activeGatewayId = state.activeGatewayId();
        activated = gatewayId != UNRESOLVED && activeGatewayId == gatewayId;
    }

    /**
     * The gate closed without being asked to: a dial failed or the counterparty hung up. The activation
     * stands, so {@link #advance()} reopens it without a second {@code GatewayStarted}.
     */
    public void onGateClosed() {
        if (state == State.SERVING) {
            state = State.PASSIVE;
        }
    }

    /**
     * A session arrived through the gate.
     * @return whether to keep it; false means the gate closed while it was in flight, so drop it without
     *         publishing anything about it
     */
    public boolean onSessionAcquired() {
        return state == State.SERVING;
    }

    /**
     * One duty cycle: an activated, caught-up instance publishes {@code GatewayStarted}, then opens the gate.
     * @return work done
     */
    public int advance() {
        if (state != State.PASSIVE || !activated) {
            return 0;
        }
        if (!registered) {
            if (!actions.publishGatewayStarted(gatewayId)) {
                return 0;
            }
            registered = true;
        }
        if (!actions.openGate()) {
            return 0;
        }
        state = State.SERVING;
        return 1;
    }

    /** Where this instance stands right now. */
    public State state() {
        return state;
    }

    /** Whether the gate is open — the one question a producer asks before submitting. */
    public boolean isServing() {
        return state == State.SERVING;
    }

    /** Whether the last {@code GatewayActive} for this pair named this instance. */
    public boolean isActivated() {
        return activated;
    }

    /** Whether this activation's {@code GatewayStarted} has been placed, binding the session to the pair's frames. */
    public boolean isAnnounced() {
        return activated && registered;
    }

    /** The instance of this pair the last {@code GatewayActive} named, or {@link SnapshotHeader#NO_GATEWAY}. */
    public int activeGatewayId() {
        return activeGatewayId;
    }

    /** Every row of this instance's pair, in list order: what a snapshot header carries. */
    public List<SnapshotHeader.GatewayRow> pairRows() {
        final List<SnapshotHeader.GatewayRow> pair = new ArrayList<>();
        rows.forEach((rowGatewayId, row) -> {
            if (row.gatewaySourceId() == gatewaySourceId) {
                pair.add(new SnapshotHeader.GatewayRow(rowGatewayId, row.preferenceRank(), row.gatewayName()));
            }
        });
        return pair;
    }

    /** This instance's list row, or {@link #UNRESOLVED} until a row names it. */
    public int gatewayId() {
        return gatewayId;
    }

    /** The logical gateway this instance belongs to, shared with its standby; {@link #UNRESOLVED} until then. */
    public int gatewaySourceId() {
        return gatewaySourceId;
    }
}
