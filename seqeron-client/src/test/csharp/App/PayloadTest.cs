using System.Buffers.Binary;
using Adaptive.Agrona;
using Org.Limitless.Seqeron.Replayer.Client;
using Xunit;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Unit tests for the two ways a payload is addressed: past its own <c>MessageHeader</c> when it has one, and from
/// its first byte when it has none (spec §13.2). No Aeron runtime — the event is a view over an encoded tap frame.
/// Case for case with <c>PayloadTest.java</c>.
/// </summary>
public class PayloadTest
{
    private const int PayloadId = 5;

    private readonly Payload _payload = new Payload();

    [Fact(DisplayName = "a payload carrying no MessageHeader is addressed whole by payloadOffset")]
    public void HeaderlessPayloadIsAddressedWhole()
    {
        var body = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(body, 0x0123456789ABCDEFL);
        _payload.Wrap(SequencedEvents.Of(1, false, PayloadId, body));

        Assert.Equal(sizeof(long), _payload.PayloadLength); // the raw body's every byte is the payload
        Assert.Equal(0x0123456789ABCDEFL, _payload.Buffer.GetLong(_payload.PayloadOffset, ByteOrder.LittleEndian));
    }

    [Fact(DisplayName = "bodyOffset skips the payload's own MessageHeader, so the two differ by its length")]
    public void BodyOffsetSkipsTheHeader()
    {
        _payload.Wrap(SequencedEvents.Of(1, false, PayloadId, new byte[sizeof(long)]));

        Assert.Equal(Frame.MessageHeader.Size, _payload.BodyOffset - _payload.PayloadOffset);
        Assert.Equal(Frame.MessageHeader.Size, _payload.PayloadLength - _payload.BodyLength);
    }
}
