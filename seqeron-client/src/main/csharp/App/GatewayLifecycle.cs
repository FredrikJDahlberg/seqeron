using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The election lifecycle of one gateway instance: which instance of a logical gateway opens its gate, and when it
/// stops. The gate is whatever the edge does to serve — an acceptor binds, an initiator dials.
/// <para><b><c>GatewayActive</c> is a state assignment, not an event.</b> The last one naming any instance of this
/// pair is in force, so this tracks rather than latches: a restarting instance replays superseded activations and
/// must not re-activate itself off one.</para>
/// <para><b><c>GatewayStarted</c> is published once per activation, and before the gate opens.</b> It makes the
/// sequencer release the connections a predecessor left open; opening first would let this instance's own
/// <c>ConnectionOpened</c> frames precede it and be released as stale.</para>
/// <para><b>Standing down publishes nothing</b>, so to the cluster it looks like the process dying. The cluster
/// session is kept and the instance can be activated again.</para>
/// <para>Single-threaded. <c>GatewayLifecycle.java</c> and <c>app/detail/GatewayLifecycle.hpp</c> are its twins;
/// keep the three in step.</para>
/// </summary>
internal sealed class GatewayLifecycle
{
    /// <summary>No <c>GatewayRegistered</c> row has named this instance yet.</summary>
    public const int Unresolved = -1;

    /// <summary>Where this instance stands.</summary>
    public enum State
    {
        /// <summary>Following the tap through history; the gate stays shut whatever the activation says.</summary>
        Replaying,

        /// <summary>Caught up with the gate shut. Where a standby sits and a superseded instance lands.</summary>
        Passive,

        /// <summary>The gate is open.</summary>
        Serving
    }

    /// <summary>What the lifecycle cannot do itself.</summary>
    public interface IActions
    {
        /// <summary>Publishes this instance's <c>GatewayStarted</c>.</summary>
        /// <param name="gatewayId">this instance's list row</param>
        /// <returns>whether it landed; false is back-pressure, retried on <see cref="Advance"/></returns>
        bool PublishGatewayStarted(int gatewayId);

        /// <summary>Opens the gate: the instance begins accepting connections.</summary>
        /// <returns>whether the gate opened; false is retried on <see cref="Advance"/></returns>
        bool OpenGate();

        /// <summary>Closes the gate and drops every session it let in. Publishes nothing.</summary>
        void CloseGate();
    }

    // One list row.
    private sealed record Row(int GatewaySourceId, int PreferenceRank, string GatewayName);

    private readonly string _gatewayName;
    private readonly IActions _actions;

    // Every row by gatewayId, in list order, a re-published row replacing its earlier one in place as the
    // sequencer's list does: a GatewayActive carries only the id, and a snapshot header every row of the pair.
    private readonly OrderedDictionary<int, Row> _rows = new OrderedDictionary<int, Row>();

    private State _state = State.Replaying;
    private int _gatewayId = Unresolved;
    private int _gatewaySourceId = Unresolved;
    private bool _activated;
    private bool _registered;

    // The instance of this pair the last GatewayActive named, or SnapshotHeader.NoGateway.
    private int _activeGatewayId = SnapshotHeader.NoGateway;

    /// <summary>An instance that starts out replaying, with no identity until the list names it.</summary>
    /// <param name="gatewayName">the <c>GatewayRegistered</c> row name this instance joins on</param>
    /// <param name="actions">what this class cannot do itself</param>
    public GatewayLifecycle(string gatewayName, IActions actions)
    {
        _gatewayName = gatewayName;
        _actions = actions;
    }

    /// <summary>The tap reached the live frontier for the first time.</summary>
    /// <returns>whether this left <see cref="State.Replaying"/></returns>
    public bool OnCaughtUp()
    {
        if (_state != State.Replaying)
        {
            return false;
        }
        _state = State.Passive;
        return true;
    }

    /// <summary>A <c>GatewayRegistered</c> row. Only the one carrying this instance's name resolves its
    /// identity.</summary>
    /// <param name="rowGatewayId">the row's instance</param>
    /// <param name="rowGatewaySourceId">the row's logical gateway</param>
    /// <param name="rowName">the row's name</param>
    /// <param name="preferenceRank">the row's rank</param>
    public void OnGatewayRegistered(int rowGatewayId, int rowGatewaySourceId, string rowName, int preferenceRank)
    {
        _rows[rowGatewayId] = new Row(rowGatewaySourceId, preferenceRank, rowName);
        if (_gatewayName != rowName)
        {
            return;
        }
        _gatewayId = rowGatewayId;
        _gatewaySourceId = rowGatewaySourceId;
    }

    /// <summary>A <c>GatewayActive</c>. One naming a sibling stands this instance down; another pair's is
    /// ignored.</summary>
    /// <param name="targetGatewayId">the instance it names</param>
    public void OnGatewayActive(int targetGatewayId)
    {
        if (_gatewayId == Unresolved)
        {
            return;
        }
        if (!_rows.TryGetValue(targetGatewayId, out Row target) || target.GatewaySourceId != _gatewaySourceId)
        {
            return;
        }
        _activeGatewayId = targetGatewayId;
        bool wasActivated = _activated;
        _activated = targetGatewayId == _gatewayId;
        if (_activated || !wasActivated)
        {
            return;
        }
        _registered = false; // being asked back is a new epoch
        if (_state == State.Serving)
        {
            _state = State.Passive;
            _actions.CloseGate();
        }
    }

    /// <summary>Takes the election state a snapshot header holds in place of the frames before its cut
    /// (doc/snapshot.md §6): the pair's rows, which resolve this instance's identity by name, and the instance its
    /// <c>GatewayActive</c> names. Before the gate has opened, so nothing is published or closed.</summary>
    /// <param name="state">the header's gateway continuation</param>
    public void OnSnapshotHeader(SnapshotHeader.GatewayState state)
    {
        _rows.Clear();
        foreach (SnapshotHeader.GatewayRow row in state.Rows)
        {
            OnGatewayRegistered(row.GatewayId, state.GatewaySourceId, row.GatewayName, row.PreferenceRank);
        }
        _activeGatewayId = state.ActiveGatewayId;
        _activated = _gatewayId != Unresolved && _activeGatewayId == _gatewayId;
    }

    /// <summary>The gate closed without being asked to: a dial failed or the counterparty hung up. The activation
    /// stands, so <see cref="Advance"/> reopens it without a second <c>GatewayStarted</c>.</summary>
    public void OnGateClosed()
    {
        if (_state == State.Serving)
        {
            _state = State.Passive;
        }
    }

    /// <summary>A session arrived through the gate.</summary>
    /// <returns>whether to keep it; false means the gate closed while it was in flight, so drop it without
    /// publishing anything about it</returns>
    public bool OnSessionAcquired()
    {
        return _state == State.Serving;
    }

    /// <summary>One duty cycle: an activated, caught-up instance publishes <c>GatewayStarted</c>, then opens the
    /// gate.</summary>
    /// <returns>work done</returns>
    public int Advance()
    {
        if (_state != State.Passive || !_activated)
        {
            return 0;
        }
        if (!_registered)
        {
            if (!_actions.PublishGatewayStarted(_gatewayId))
            {
                return 0;
            }
            _registered = true;
        }
        if (!_actions.OpenGate())
        {
            return 0;
        }
        _state = State.Serving;
        return 1;
    }

    /// <summary>Where this instance stands right now.</summary>
    public State CurrentState => _state;

    /// <summary>Whether the gate is open — the one question a producer asks before submitting.</summary>
    public bool IsServing => _state == State.Serving;

    /// <summary>Whether the last <c>GatewayActive</c> for this pair named this instance.</summary>
    public bool IsActivated => _activated;

    /// <summary>Whether this activation's <c>GatewayStarted</c> has been placed, binding the session to the pair's
    /// frames.</summary>
    public bool IsAnnounced => _activated && _registered;

    /// <summary>The instance of this pair the last <c>GatewayActive</c> named, or
    /// <see cref="SnapshotHeader.NoGateway"/>.</summary>
    public int ActiveGatewayId => _activeGatewayId;

    /// <summary>Every row of this instance's pair, in list order: what a snapshot header carries.</summary>
    public List<SnapshotHeader.GatewayRow> PairRows()
    {
        var pair = new List<SnapshotHeader.GatewayRow>();
        foreach (KeyValuePair<int, Row> row in _rows)
        {
            if (row.Value.GatewaySourceId == _gatewaySourceId)
            {
                pair.Add(new SnapshotHeader.GatewayRow(row.Key, row.Value.PreferenceRank, row.Value.GatewayName));
            }
        }
        return pair;
    }

    /// <summary>This instance's list row, or <see cref="Unresolved"/> until a row names it.</summary>
    public int GatewayId => _gatewayId;

    /// <summary>The logical gateway this instance belongs to, shared with its standby; <see cref="Unresolved"/>
    /// until then.</summary>
    public int GatewaySourceId => _gatewaySourceId;
}
