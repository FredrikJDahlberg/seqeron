namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// What a publish did. <see cref="Refused"/> is local and permanent: the payload is too long, or the frame breaks
/// §9.2 conditions 6 to 9, so retrying it cannot succeed. <see cref="Declined"/> is the one a caller may retry: the
/// gate shut, ingress held or the tracker full, the session lost or being replaced, or a hosted stub's link to its
/// host full. Cluster ingress back-pressure is not a decline: a publish spins through it, for up to 10 s.
/// </summary>
public enum Publish
{
    /// <summary>The frame was offered, and tracked if a tracker was given.</summary>
    Published,

    /// <summary>Nothing was offered and retrying cannot help.</summary>
    Refused,

    /// <summary>Nothing was offered; the same frame may be offered again.</summary>
    Declined
}
