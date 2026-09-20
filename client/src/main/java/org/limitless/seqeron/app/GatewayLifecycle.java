package org.limitless.seqeron.app;

import java.util.HashMap;
import java.util.Map;

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
public final class GatewayLifecycle {
    /** No {@code GatewayRegistered} row has named this instance yet. */
    public static final int UNRESOLVED = -1;

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
        /** A {@code GatewayRegistered} row named this instance. */
        void identityResolved(int gatewayId, int gatewaySourceId, int preferenceRank);

        /** @return whether it landed; false is back-pressure, retried on {@link #advance()} */
        boolean publishGatewayStarted(int gatewayId);

        /** @return whether the gate opened; false is retried on {@link #advance()} */
        boolean openGate();

        /** Closes the gate and drops every session it let in. Publishes nothing. */
        void closeGate();
    }

    private final String gatewayName;
    private final Actions actions;

    /** Every row's {@code gatewayId -> gatewaySourceId}: a {@code GatewayActive} carries only the former. */
    private final Map<Integer, Integer> gatewaySourceIds = new HashMap<>();

    private State state = State.REPLAYING;
    private int gatewayId = UNRESOLVED;
    private int gatewaySourceId = UNRESOLVED;
    private boolean activated;
    private boolean registered;

    /**
     * @param gatewayName the {@code GatewayRegistered} row name this instance joins on
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
        gatewaySourceIds.put(rowGatewayId, rowGatewaySourceId);
        if (!gatewayName.equals(rowName)) {
            return;
        }
        gatewayId = rowGatewayId;
        gatewaySourceId = rowGatewaySourceId;
        actions.identityResolved(gatewayId, gatewaySourceId, preferenceRank);
    }

    /** A {@code GatewayActive}. One naming a sibling stands this instance down; another pair's is ignored. */
    public void onGatewayActive(final int targetGatewayId) {
        if (gatewayId == UNRESOLVED) {
            return;
        }
        final Integer targetSourceId = gatewaySourceIds.get(targetGatewayId);
        if (targetSourceId == null || targetSourceId != gatewaySourceId) {
            return;
        }
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

    public State state() {
        return state;
    }

    public boolean isServing() {
        return state == State.SERVING;
    }

    /** Whether the last {@code GatewayActive} for this pair named this instance. */
    public boolean isActivated() {
        return activated;
    }

    public int gatewayId() {
        return gatewayId;
    }

    public int gatewaySourceId() {
        return gatewaySourceId;
    }
}
