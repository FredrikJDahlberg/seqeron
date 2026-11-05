namespace Org.Limitless.Seqeron.App;

/// <summary>What a reply needs of the request it answers: the requester's <c>sourceId</c>, <c>connectionId</c> and
/// <c>groupMask</c>. Unlike the <see cref="Payload"/> it came from, it outlives the callback, so keep it for a reply
/// dispatched later.</summary>
public readonly record struct ReplyAddress(int SourceId, int ConnectionId, ushort GroupMask);
