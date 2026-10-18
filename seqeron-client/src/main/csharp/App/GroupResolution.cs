using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Adaptive.Agrona;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The groups a producer addresses (spec A-9): the latest group table on its queue, or its snapshot header's when
/// none follows the cut, resolved once when it has caught up, and the mask each frame it publishes then carries.
/// Holds no Aeron runtime; <see cref="Session"/> drives it.
/// <para><c>GroupResolution.java</c> and <c>app/detail/GroupResolution.hpp</c> are its twins; keep the three in
/// step.</para>
/// </summary>
internal sealed class GroupResolution
{
    /// <summary>What <see cref="FrameMask"/> returns for a mask naming a group the table lacks.</summary>
    internal const int NotAddressable = -1;

    private const int NameLength = Frame.GroupsRegistered.GroupNamesLength / FrameLayer.MaxGroups;

    private readonly IReadOnlyList<string> _addressedGroups;
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Frame.GroupsRegistered _groupsRegistered = new Frame.GroupsRegistered();
    private ushort _ownBit;

    /// <param name="addressedGroups">the names of the other groups the producer publishes to</param>
    internal GroupResolution(IReadOnlyList<string> addressedGroups)
    {
        _addressedGroups = addressedGroups.ToArray();
    }

    /// <summary>The table so far, for a snapshot header.</summary>
    internal IReadOnlyList<SnapshotHeader.Group> Table { get; private set; } = Array.Empty<SnapshotHeader.Group>();

    /// <summary>The resolved directory, or null.</summary>
    internal GroupDirectory Directory { get; private set; }

    /// <summary>A <c>GroupsRegistered</c> off the queue; the latest is the table.</summary>
    /// <param name="buffer">holding the message</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">of the message</param>
    internal void OnGroupsRegistered(IDirectBuffer buffer, int offset, int length)
    {
        SbeBuffers.Wrap(_view, buffer, offset, length);
        _groupsRegistered.WrapForDecode(_view, 0, Frame.GroupsRegistered.BlockLength,
                                        Frame.MessageHeader.SbeSchemaVersion);
        ReadOnlySpan<byte> names = _groupsRegistered.GroupNames;
        var groups = new List<SnapshotHeader.Group>();
        for (int slot = 0; slot < FrameLayer.MaxGroups; slot++)
        {
            ReadOnlySpan<byte> name = names.Slice(slot * NameLength, NameLength);
            int nameLength = name.IndexOf((byte)0);
            nameLength = nameLength < 0 ? NameLength : nameLength;
            if (nameLength > 0)
            {
                groups.Add(new SnapshotHeader.Group(slot + 1, Encoding.ASCII.GetString(name.Slice(0, nameLength))));
            }
        }
        Table = groups.AsReadOnly();
    }

    /// <summary>A restore's header, standing in for the <c>GroupsRegistered</c> before its cut.</summary>
    internal void OnSnapshotHeader(IReadOnlyList<SnapshotHeader.Group> groups)
    {
        Table = groups;
    }

    /// <summary>Resolves the addressed names against the table.</summary>
    /// <param name="ownGroupId">the group whose queue the producer reads</param>
    /// <returns>the first addressed name the table lacks, which leaves this unresolved, or null</returns>
    internal string Resolve(int ownGroupId)
    {
        var resolved = new GroupDirectory(Table);
        foreach (string name in _addressedGroups)
        {
            if (resolved.Mask(name) == 0)
            {
                return name;
            }
        }
        Directory = resolved;
        _ownBit = GroupDirectory.Bit(ownGroupId);
        return null;
    }

    /// <summary>The mask a frame carries: the groups it addresses and the producer's own, since confirmed ingress
    /// watches the own queue. Only once resolved.</summary>
    /// <param name="groupMask">the groups addressed, 0 for the own group alone</param>
    /// <returns>the frame's mask, or <see cref="NotAddressable"/></returns>
    internal int FrameMask(ushort groupMask)
    {
        return (groupMask & ~Directory.ActiveMask) != 0 ? NotAddressable : groupMask | _ownBit;
    }
}
