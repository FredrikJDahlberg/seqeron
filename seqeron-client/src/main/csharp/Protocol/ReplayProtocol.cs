using Adaptive.Aeron;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>The replay protocol's addresses, shared by the Replayer and its co-located clients.</summary>
public static class ReplayProtocol
{
    /// <summary>The whole protocol is node-local: a replica and its Replayer share one media driver.</summary>
    public const string IpcChannel = "aeron:ipc";

    /// <summary>ReplayerService to apps: on-demand archive replays (one Aeron session per in-flight
    /// replay).</summary>
    public const int ReplayStreamId = 201;

    /// <summary>Apps to ReplayerService: <c>ReplayRequest</c>.</summary>
    public const int RequestStreamId = 202;

    /// <summary>ReplayerService to apps: <c>Replaying</c> / <c>ReplayPending</c>.</summary>
    public const int ControlStreamId = 203;

    /// <summary><c>ReplayRequest.fromPosition</c> asking for the active recording from its start.</summary>
    public const long FromStart = Aeron.NULL_VALUE;

    /// <summary>Answer to a resume request the Replayer refuses, or one that needs no replay at all.</summary>
    public const long NoReplayNeeded = Aeron.NULL_VALUE;
}
