using Adaptive.Agrona;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// What <see cref="IngressPublisher"/> needs of a cluster session: place this frame, and say which session it
/// belongs to. The seam the suite drives without an Aeron runtime; <c>ClusterStreamSender</c> implements it.
/// </summary>
public interface IIngressSender
{
    /// <summary>Offers one pre-encoded frame to cluster ingress.</summary>
    /// <param name="frame">the frame, from offset 0</param>
    /// <param name="length">its length in bytes</param>
    /// <returns>whether it was placed; false with a session still open means a new leader arrived while an
    /// <see cref="IIngressTracker"/> held, and the frame goes again once it releases</returns>
    bool Send(IDirectBuffer frame, int length);

    /// <summary>This process's cluster session id, or -1 with no session. Stamped into a frame's advisory
    /// field.</summary>
    long ClusterSessionId { get; }

    /// <summary>The leadership term ingress is stamped with, or -1 with no session. Read straight after a
    /// <see cref="Send"/> that returned true, it is the term that frame carried: the cluster drops a frame stamped
    /// with any term but its own.</summary>
    long LeadershipTermId { get; }
}
