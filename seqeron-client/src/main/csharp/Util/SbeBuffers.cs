using System;
using Adaptive.Agrona;
using Org.SbeTool.Sbe.Dll;

namespace Org.Limitless.Seqeron.Util;

/// <summary>
/// Where Aeron.NET's buffers meet the SBE codecs. Agrona.NET pins every buffer it hands out, so a codec reads
/// and writes the same bytes through the pointer, and nothing is copied.
/// </summary>
internal static unsafe class SbeBuffers
{
    /// <summary>Points <paramref name="view"/> at <paramref name="length"/> bytes of <paramref name="source"/> from
    /// <paramref name="offset"/>.</summary>
    /// <param name="view">the SBE buffer a codec is wrapped over</param>
    /// <param name="source">the bytes, as Aeron.NET or the caller holds them</param>
    /// <param name="offset">of the first byte in <paramref name="source"/></param>
    /// <param name="length">how many bytes the view covers</param>
    public static void Wrap(DirectBuffer view, IDirectBuffer source, int offset, int length)
    {
        source.BoundsCheck(offset, length);
        view.Wrap((byte*)source.BufferPointer + offset, length);
    }

    /// <summary>The same bytes as a span.</summary>
    /// <param name="source">the bytes</param>
    /// <param name="offset">of the first</param>
    /// <param name="length">how many</param>
    public static ReadOnlySpan<byte> Bytes(IDirectBuffer source, int offset, int length)
    {
        source.BoundsCheck(offset, length);
        return new ReadOnlySpan<byte>((byte*)source.BufferPointer + offset, length);
    }
}
