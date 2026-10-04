using System;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// Encode-and-offer for cluster ingress. An instance owning its buffer rather than free functions over a stack
/// array, and the payload arrives pre-encoded rather than through a <c>Fill</c>, as in Java.
/// <para>Given an <see cref="IIngressTracker"/> (spec §16 A-4, A-5), each published frame is tracked under the
/// sender's session and term, and nothing is sent while the tracker holds or is full.</para>
/// <para>Not thread-safe: one publisher per producing thread. <c>IngressPublisher.java</c> and
/// <c>sequencer/client/IngressPublisher.hpp</c> are its twins.</para>
/// </summary>
public sealed class IngressPublisher
{
    private readonly UnsafeBuffer _frame = new UnsafeBuffer(GC.AllocateArray<byte>(FrameLayer.MaxIngressLength, true));
    private readonly SystemFrame _envelope = new SystemFrame();
    private readonly IIngressTracker _tracker;

    /// <summary>A publisher that tracks every frame it places with <paramref name="tracker"/>, and sends nothing
    /// while it holds.</summary>
    /// <param name="tracker">the tracker, or null to track nothing</param>
    public IngressPublisher(IIngressTracker tracker = null)
    {
        _tracker = tracker;
    }

    /// <summary>
    /// Wraps one application payload in an <c>Unsequenced</c> frame and offers it.
    /// <para>Two header fields are not parameters: the frame's template, and the cluster session, which is the
    /// sender's. What varies is <c>sourceId</c> — for a reply, the requester's rather than this process's own —
    /// <c>connectionId</c>, <c>payloadId</c>, and the payload.</para>
    /// </summary>
    /// <param name="sender">the session to offer it on</param>
    /// <param name="sourceId">the producer the frame is on behalf of</param>
    /// <param name="connectionId">the connection it belongs to, or -1 for a producer-scoped one</param>
    /// <param name="payloadId">which protocol the payload speaks</param>
    /// <param name="payload">the payload's bytes, whatever encoding they carry: the tier copies them through
    /// unopened, so an SBE payload includes its own 8-byte <c>MessageHeader</c> and a raw one no framing at
    /// all</param>
    /// <param name="payloadLength">bytes of <paramref name="payload"/> from offset 0</param>
    public Publish PublishPayload(IIngressSender sender, int sourceId, int connectionId, int payloadId,
                                  IDirectBuffer payload, int payloadLength)
    {
        int length = _envelope.WrapPayload(_frame, sourceId, connectionId, sender.ClusterSessionId, payloadId, payload,
                                           payloadLength);
        return Offer(sender, length);
    }

    /// <summary>The same for one of seqeron's own events (doc/seqeron-protocol-spec.md §7), in an
    /// <c>UnsequencedSystem</c> frame.</summary>
    /// <param name="sender">the session to offer it on</param>
    /// <param name="sourceId">the producer the frame is on behalf of</param>
    /// <param name="connectionId">the connection it belongs to, or -1 for a producer-scoped one</param>
    /// <param name="systemEventType">which event <paramref name="body"/> holds</param>
    /// <param name="body">the event's SBE block, with no <c>MessageHeader</c> of its own — <c>systemEventType</c>
    /// is what names it</param>
    /// <param name="bodyLength">bytes of <paramref name="body"/> from offset 0</param>
    public Publish PublishSystem(IIngressSender sender, int sourceId, int connectionId, int systemEventType,
                                 IDirectBuffer body, int bodyLength)
    {
        int length =
            _envelope.Wrap(_frame, sourceId, connectionId, sender.ClusterSessionId, systemEventType, body, bodyLength);
        return Offer(sender, length);
    }

    private Publish Offer(IIngressSender sender, int length)
    {
        if (length == SystemFrame.Refused)
        {
            return Publish.Refused;
        }
        if (_tracker != null && (_tracker.IsHolding || _tracker.IsFull))
        {
            return Publish.Declined;
        }
        if (!sender.Send(_frame, length))
        {
            return Publish.Declined;
        }
        _tracker?.Track(_frame, length, sender.ClusterSessionId, sender.LeadershipTermId);
        return Publish.Published;
    }
}
