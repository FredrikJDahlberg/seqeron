using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Sbe.Frame;
using Xunit;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// What the decoder does with a fragment that is not a whole frame. A rejection means the recording itself is
/// damaged, but the read must fail as a <c>false</c>, never as an exception or a read past the fragment:
/// <c>Image.Poll</c> advances past a fragment whose handler throws, so a throw loses the frame silently.
/// <para>Case for case, in the same order, with <c>SequencedFrameDecoderTest.java</c>; the C++ twin is
/// <c>SequencedFrameTest.cpp</c>, which has no offset case.</para>
/// </summary>
public class SequencedFrameDecoderTest
{
    // Offsets inside the outer MessageHeader.
    private const int TemplateIdOffset = 2;
    private const int SchemaIdOffset = 4;

    // Offset of the payload's length prefix: past the framing header and the 34-byte composite.
    private const int PrefixOffset = MessageHeader.Size + SequencedHeader.Size;

    private readonly SequencedFrameDecoder _view = new SequencedFrameDecoder();

    [Fact(DisplayName = "a fragment shorter than a MessageHeader is not a frame")]
    public void ShorterThanAMessageHeader()
    {
        byte[] frame = Frames.Payload(1, 8);
        for (int length = 0; length < MessageHeader.Size; length++)
        {
            Assert.False(Wrap(frame[..length]), $"{length} bytes cannot name a template");
        }
    }

    [Fact(DisplayName = "a foreign schemaId is not a frame, whatever its templateId says")]
    public void ForeignSchemaId()
    {
        byte[] frame = Frames.Payload(1, 8);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(SchemaIdOffset), MessageHeader.SbeSchemaId + 1);
        Assert.False(Wrap(frame), "template ids are unique per schema, so the schema has to match first");
    }

    [Fact(DisplayName = "an unknown templateId in seqeron's own schema is not a frame")]
    public void UnknownTemplateId()
    {
        byte[] frame = Frames.Payload(1, 8);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(TemplateIdOffset), 999);
        Assert.False(Wrap(frame), "only the sequenced messages decode");
    }

    [Fact(DisplayName = "an application frame cut before its payload prefix is rejected")]
    public void ApplicationFrameCutBeforeItsPrefix()
    {
        byte[] frame = Frames.Payload(1, 8);
        int prefixEnd = PrefixOffset + Sequenced.PayloadHeaderSize;
        for (int length = MessageHeader.Size; length < prefixEnd; length++)
        {
            Assert.False(Wrap(frame[..length]), $"the payload's length is unreadable at {length} bytes");
        }
        Assert.True(Wrap(frame[..(prefixEnd + 8)]), "the whole frame still decodes");
    }

    [Fact(DisplayName = "an application payload declaring more bytes than the fragment holds is rejected")]
    public void ApplicationPayloadRunsPastTheFragment()
    {
        byte[] frame = Frames.Payload(1, 8);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(PrefixOffset), 9);
        Assert.False(Wrap(frame), "a payload prefix is a claim about the fragment, not a fact");
    }

    [Fact(DisplayName = "a submitted system frame cut before its payload prefix is rejected")]
    public void SystemFrameCutBeforeItsPrefix()
    {
        byte[] frame = Frames.SystemEvent(1, SystemFrame.ConnectionClosed, 0);
        int prefixEnd = PrefixOffset + SequencedSystem.BodyHeaderSize;
        for (int length = MessageHeader.Size; length < prefixEnd; length++)
        {
            Assert.False(Wrap(frame[..length]), $"the payload's length is unreadable at {length} bytes");
        }
        Assert.True(Wrap(frame), "an empty payload is a whole frame (§5)");
    }

    [Fact(DisplayName = "a system payload declaring more bytes than the fragment holds is rejected")]
    public void SystemBodyRunsPastTheFragment()
    {
        byte[] frame = Frames.SystemEvent(1, SystemFrame.ClusterStarted, 8);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(PrefixOffset), 9);
        Assert.False(Wrap(frame));
    }

    [Fact(DisplayName = "a synthesized frame cut inside its header composite is rejected")]
    public void SynthesizedFrameCutInsideItsHeader()
    {
        byte[] frame = Frames.ClusterHeartbeat(1);
        int headerEnd = MessageHeader.Size + SequencedSystemHeader.Size;
        for (int length = MessageHeader.Size; length < headerEnd; length++)
        {
            Assert.False(Wrap(frame[..length]), $"globalSeqNo is not readable at {length} bytes, so P-3 cannot count");
        }
        Assert.True(Wrap(frame));
    }

    /// <summary>
    /// The property behind the cases above, swept over every case and every cut. The fragment handed to
    /// <see cref="SequencedFrameDecoder.Wrap"/> is a copy of exactly that length, so a read past its end is a
    /// bounds failure rather than a read of the next byte.
    /// </summary>
    [Fact(DisplayName = "every truncation of every case is rejected or stays inside the fragment")]
    public void EveryTruncationIsRejectedOrStaysInsideTheFragment()
    {
        foreach ((string name, byte[] whole) in EveryCase())
        {
            Assert.True(Wrap(whole), $"{name} does not decode whole");
            for (int length = 0; length < whole.Length; length++)
            {
                if (Wrap(whole[..length]))
                {
                    Assert.True(_view.PayloadOffset + _view.PayloadLength <= length,
                                $"{name} cut to {length} of {whole.Length} bytes decodes with a payload running " +
                                    "past the fragment");
                }
            }
        }
    }

    /// <summary>
    /// A whole frame this time. A system payload is decoded with its decoder's compiled constants (<b>V-3</b>), so
    /// the view declares none, synthesized or submitted.
    /// </summary>
    [Fact(DisplayName = "a system frame declares no block length or version")]
    public void ASystemFrameDeclaresNoBlockLengthOrVersion()
    {
        foreach (byte[] frame in new[] { Frames.SystemEvent(1, SystemFrame.ClusterStarted, 8),
                                         Frames.ClusterHeartbeat(2) })
        {
            Assert.True(Wrap(frame));
            Assert.Equal(0, _view.BlockLength);
            Assert.Equal(0, _view.Version);
        }
    }

    [Fact(DisplayName = "the bounds are taken from the fragment, not from the buffer behind it")]
    public void BoundsAreRelativeToTheFragment()
    {
        byte[] frame = Frames.Payload(1, 8);
        IMutableDirectBuffer batch = new ExpandableArrayBuffer(frame.Length * 2);
        int offset = frame.Length;
        batch.PutBytes(offset, frame);

        Assert.True(_view.Wrap(batch, offset, frame.Length));
        Assert.Equal(offset + PrefixOffset + Sequenced.PayloadHeaderSize, _view.PayloadOffset);
        Assert.False(_view.Wrap(batch, offset, frame.Length - 1),
                     "the bytes past the fragment belong to the next one, not to this payload");
    }

    private bool Wrap(byte[] fragment)
    {
        return _view.Wrap(new UnsafeBuffer(fragment), 0, fragment.Length);
    }

    private static IEnumerable<(string, byte[])> EveryCase()
    {
        yield return ("an empty payload", Frames.Payload(1, 0));
        yield return ("a payload shorter than a MessageHeader", Frames.Payload(2, 3));
        yield return ("a payload naming its own message", Frames.Payload(3, 24));
        yield return ("a system event with no payload", Frames.SystemEvent(4, SystemFrame.ConnectionClosed, 0));
        yield return ("a system event with a payload", Frames.SystemEvent(5, SystemFrame.ClusterStarted, 8));
        yield return ("a synthesized ClusterHeartbeat", Frames.ClusterHeartbeat(6));
    }
}
