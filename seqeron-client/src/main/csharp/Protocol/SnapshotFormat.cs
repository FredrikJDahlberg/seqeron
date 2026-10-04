using System;
using System.Numerics;
using System.Runtime.InteropServices;
using Adaptive.Agrona;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// What a snapshot is (doc/snapshot.md §2): a sequence of records, the façade's header first, checked by the
/// CRC-32C of their bytes in order that its <c>SnapshotEnd</c> carries. <c>SnapshotFormat.java</c> and
/// <c>Snapshot.hpp</c> are its twins; keep the three in step.
/// </summary>
public static class SnapshotFormat
{
    /// <summary>The most bytes one record holds: what its uint16 length prefix in the file can say.</summary>
    public const int MaxRecordLength = 65535;

    /// <summary>The CRC-32C of a span of bytes, as <c>SnapshotEnd.crc32c</c> carries it.</summary>
    /// <param name="buffer">holding the bytes</param>
    /// <param name="offset">of the first</param>
    /// <param name="length">how many</param>
    public static uint Crc32C(IDirectBuffer buffer, int offset, int length)
    {
        return Update(0, SbeBuffers.Bytes(buffer, offset, length));
    }

    /// <summary>Feeds bytes into a running CRC-32C.</summary>
    /// <param name="crc">the CRC of the bytes before these, or 0 to start</param>
    /// <param name="buffer">holding the bytes</param>
    /// <param name="offset">of the first</param>
    /// <param name="length">how many</param>
    /// <returns>the CRC of everything so far</returns>
    public static uint Update(uint crc, IDirectBuffer buffer, int offset, int length)
    {
        return Update(crc, SbeBuffers.Bytes(buffer, offset, length));
    }

    /// <summary>Feeds bytes into a running CRC-32C.</summary>
    /// <param name="crc">the CRC of the bytes before these, or 0 to start</param>
    /// <param name="bytes">the bytes</param>
    /// <returns>the CRC of everything so far</returns>
    public static uint Update(uint crc, ReadOnlySpan<byte> bytes)
    {
        // BitOperations.Crc32C is the bare Castagnoli step, hardware where the CPU has it; the inversions on
        // either side make it the standard CRC-32C. Eight bytes at a time read little-endian are the same eight
        // bytes in order, which is how the step consumes them.
        uint c = ~crc;
        int i = 0;
        if (BitConverter.IsLittleEndian)
        {
            for (; i + sizeof(ulong) <= bytes.Length; i += sizeof(ulong))
            {
                c = BitOperations.Crc32C(c, MemoryMarshal.Read<ulong>(bytes.Slice(i)));
            }
        }
        for (; i < bytes.Length; i++)
        {
            c = BitOperations.Crc32C(c, bytes[i]);
        }
        return ~c;
    }
}
