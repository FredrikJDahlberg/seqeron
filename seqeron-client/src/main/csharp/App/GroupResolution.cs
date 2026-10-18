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

    /// <summary>What <see cref="FrameMask"/> returns before the groups are resolved: nothing may be published
    /// (A-9).</summary>
    internal const int Unresolved = -2;

    /// <summary>What a catch-up did to the groups.</summary>
    internal enum Outcome
    {
        /// <summary>Resolved before: nothing to do.</summary>
        Unchanged,
        /// <summary>Resolved now: hand <see cref="Directory"/> to the listener before the publish gate
        /// opens.</summary>
        Resolved,
        /// <summary>An addressed name the table lacks, <see cref="UnknownGroup"/>: the producer fences.</summary>
        GroupUnknown
    }

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

    /// <summary>The addressed name the table lacked, once <see cref="OnCaughtUp"/> has said so.</summary>
    internal string UnknownGroup { get; private set; }

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

    /// <summary>Resolves the addressed names against the table at the first catch-up, and never again: the groups
    /// are fixed (S-8).</summary>
    /// <param name="ownGroupId">the group whose queue the producer reads</param>
    /// <returns>what this catch-up did</returns>
    internal Outcome OnCaughtUp(int ownGroupId)
    {
        if (Directory != null)
        {
            return Outcome.Unchanged;
        }
        var resolved = new GroupDirectory(Table);
        foreach (string name in _addressedGroups)
        {
            if (resolved.Mask(name) == 0)
            {
                UnknownGroup = name;
                return Outcome.GroupUnknown;
            }
        }
        Directory = resolved;
        _ownBit = GroupDirectory.Bit(ownGroupId);
        return Outcome.Resolved;
    }

    /// <summary>The mask a frame carries: the groups it addresses and the producer's own, since confirmed ingress
    /// watches the own queue.</summary>
    /// <param name="groupMask">the groups addressed, 0 for the own group alone</param>
    /// <returns>the frame's mask, <see cref="NotAddressable"/> or <see cref="Unresolved"/></returns>
    internal int FrameMask(ushort groupMask)
    {
        if (Directory == null)
        {
            return Unresolved;
        }
        return (groupMask & ~Directory.ActiveMask) != 0 ? NotAddressable : groupMask | _ownBit;
    }
}
