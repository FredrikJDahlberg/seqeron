using System;
using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The log's group table, as a producer addresses it (spec A-9): a group's name to its bit of <c>groupMask</c>,
/// and a <c>groupId</c> to its name. Immutable: the groups are fixed by the first topology load (S-8), so one
/// directory holds for the life of the archive. Combine masks with <c>|</c>.
/// <para><c>GroupDirectory.java</c> and <c>app/GroupDirectory.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
public sealed class GroupDirectory
{
    private readonly string[] _names = new string[FrameLayer.MaxGroups];

    /// <param name="groups">the table, each group once</param>
    internal GroupDirectory(IReadOnlyList<SnapshotHeader.Group> groups)
    {
        foreach (SnapshotHeader.Group group in groups)
        {
            _names[group.GroupId - 1] = group.GroupName;
            ActiveMask |= Bit(group.GroupId);
        }
    }

    /// <summary>Every group's bit.</summary>
    internal ushort ActiveMask { get; }

    /// <summary>A group's bit.</summary>
    /// <param name="name">the group's name in the topology</param>
    /// <returns>its bit of <c>groupMask</c>, or 0 if the table names no such group</returns>
    /// <exception cref="ArgumentNullException">for a null name</exception>
    public ushort Mask(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        for (int slot = 0; slot < _names.Length; slot++)
        {
            if (name == _names[slot])
            {
                return (ushort)(1 << slot);
            }
        }
        return 0;
    }

    /// <summary>A group's name, to label it.</summary>
    /// <param name="groupId">1 to 16</param>
    /// <returns>its name, or null if the table names no such group</returns>
    public string Name(int groupId)
    {
        return groupId < 1 || groupId > _names.Length ? null : _names[groupId - 1];
    }

    internal static ushort Bit(int groupId)
    {
        return (ushort)(1 << (groupId - 1));
    }
}
