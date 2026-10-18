using Adaptive.Agrona;
using Org.Limitless.Seqeron.Util;
using Replay = Org.Limitless.Seqeron.Sbe.Replay;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Which queue this instance reads (spec R-6): a <c>QueueRequest</c> to the node's Replayer, resent until it answers
/// with a <c>QueueResolved</c>. Until then the Replayer answers <c>ReplayPending</c>, which holds the resend off as it
/// does a replay request's. Holds no Aeron runtime; <see cref="Session"/> is its transport.
/// <para><c>QueueResolution.java</c> and <c>app/detail/QueueResolution.hpp</c> are its twins; keep the three in
/// step.</para>
/// </summary>
internal sealed class QueueResolution
{
    /// <summary><see cref="GroupId"/> before the Replayer has answered.</summary>
    internal const int Unresolved = 0;

    internal const long ResendIntervalMs = 500;

    /// <summary>What this cannot do itself: the send, the id replies are addressed to, and the clock.</summary>
    internal interface IActions
    {
        /// <summary>Offers one <c>QueueRequest</c>; best-effort, since the resend timer retries.</summary>
        /// <param name="requestId">the id the reply must carry</param>
        void SendQueueRequest(long requestId);

        /// <summary>The id the Replayer addresses this instance's replies to (R-4).</summary>
        long ClientId { get; }

        /// <summary>Monotonic millis.</summary>
        long NowMs();
    }

    private readonly IActions _actions;
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Replay.MessageHeader _header = new Replay.MessageHeader();
    private readonly Replay.QueueResolved _resolved = new Replay.QueueResolved();
    private readonly Replay.ReplayPending _pending = new Replay.ReplayPending();

    private bool _started;
    private long _lastRequestMs;

    internal QueueResolution(IActions actions)
    {
        _actions = actions;
    }

    /// <summary>The group whose queue this instance reads, or <see cref="Unresolved"/>.</summary>
    internal int GroupId { get; private set; } = Unresolved;

    /// <summary>The id the next reply must carry to count.</summary>
    internal long RequestId { get; private set; }

    /// <summary>Sends the first request.</summary>
    internal void Start()
    {
        _started = true;
        Send();
    }

    /// <summary>The resend, once per duty cycle.</summary>
    /// <param name="publicationPending">the request publication has not connected yet: retried every cycle rather
    /// than waiting out a resend interval for a race much shorter than one</param>
    internal void DoTimers(bool publicationPending)
    {
        if (_started && GroupId == Unresolved &&
            (publicationPending || _actions.NowMs() - _lastRequestMs > ResendIntervalMs))
        {
            Send();
        }
    }

    /// <summary>Takes one control message; only a reply to the current request counts (R-1, R-4).</summary>
    /// <param name="buffer">holding the message</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">of the message</param>
    internal void OnControl(IDirectBuffer buffer, int offset, int length)
    {
        if (GroupId != Unresolved || length < Replay.MessageHeader.Size)
        {
            return;
        }
        SbeBuffers.Wrap(_view, buffer, offset, length);
        _header.Wrap(_view, 0, Replay.MessageHeader.SbeSchemaVersion);
        int bodyOffset = Replay.MessageHeader.Size;
        int blockLength = _header.BlockLength;
        // SBE throws on a block past the view, and Image.Poll would swallow it.
        if (_header.SchemaId != Replay.MessageHeader.SbeSchemaId || bodyOffset + blockLength > length)
        {
            return;
        }
        if (_header.TemplateId == Replay.QueueResolved.TemplateId)
        {
            _resolved.WrapForDecode(_view, bodyOffset, blockLength, _header.Version);
            if (_resolved.ClientId == _actions.ClientId && _resolved.RequestId == RequestId)
            {
                GroupId = _resolved.QueueId;
            }
        }
        else if (_header.TemplateId == Replay.ReplayPending.TemplateId)
        {
            _pending.WrapForDecode(_view, bodyOffset, blockLength, _header.Version);
            if (_pending.ClientId == _actions.ClientId && _pending.RequestId == RequestId)
            {
                _lastRequestMs = _actions.NowMs();
            }
        }
    }

    private void Send()
    {
        _lastRequestMs = _actions.NowMs();
        _actions.SendQueueRequest(++RequestId);
    }
}
