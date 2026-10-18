using System;
using System.Collections.Generic;
using System.Text;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.SbeTool.Sbe.Dll;
using Xunit;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The groups a producer addresses (spec A-9), resolved from its queue's table. Case for case with
/// <c>GroupResolutionTest.java</c> and <c>GroupResolutionTest.cpp</c>, in the same order.
/// </summary>
public class GroupResolutionTest
{
    private static readonly SnapshotHeader.Group[] Table = { new SnapshotHeader.Group(1, "orders"),
                                                             new SnapshotHeader.Group(3, "risk") };

    [Fact]
    public void TheTableIsTheLatestGroupsRegistered()
    {
        var resolution = new GroupResolution(Array.Empty<string>());
        GroupsRegistered(resolution, new SnapshotHeader.Group(2, "audit"));
        GroupsRegistered(resolution, Table);

        Assert.Equal(Table, resolution.Table);
    }

    [Fact]
    public void ARestoredHeaderStandsInForTheGroupsRegistered()
    {
        var resolution = new GroupResolution(Array.Empty<string>());
        resolution.OnSnapshotHeader(Table);
        Assert.Equal(Table, resolution.Table);

        GroupsRegistered(resolution, new SnapshotHeader.Group(1, "orders")); // one after the cut
        Assert.Equal(new[] { new SnapshotHeader.Group(1, "orders") }, resolution.Table);
    }

    [Fact]
    public void TheDirectoryNamesEveryGroupOfTheTable()
    {
        var resolution = new GroupResolution(new[] { "risk" });
        resolution.OnSnapshotHeader(Table);

        Assert.Null(resolution.Resolve(1));
        GroupDirectory directory = resolution.Directory;
        Assert.Equal(1, directory.Mask("orders"));
        Assert.Equal(4, directory.Mask("risk"));
        Assert.Equal(0, directory.Mask("audit"));
        Assert.Equal("risk", directory.Name(3));
        Assert.Null(directory.Name(2));
        Assert.Null(directory.Name(0));
        Assert.Null(directory.Name(17));
    }

    [Fact]
    public void AnAddressedNameTheTableLacksLeavesTheGroupsUnresolved()
    {
        var resolution = new GroupResolution(new[] { "risk", "audit" });
        resolution.OnSnapshotHeader(Table);

        Assert.Equal("audit", resolution.Resolve(1));
        Assert.Null(resolution.Directory);
    }

    [Fact]
    public void EveryFrameCarriesTheOwnGroupsBit()
    {
        var resolution = new GroupResolution(Array.Empty<string>());
        resolution.OnSnapshotHeader(Table);
        resolution.Resolve(3);

        Assert.Equal(4, resolution.FrameMask(0));
        Assert.Equal(5, resolution.FrameMask(1));
        Assert.Equal(4, resolution.FrameMask(4));
    }

    [Fact]
    public void AMaskNamingAGroupTheTableLacksIsNotAddressable()
    {
        var resolution = new GroupResolution(Array.Empty<string>());
        resolution.OnSnapshotHeader(Table);
        resolution.Resolve(1);

        Assert.Equal(GroupResolution.NotAddressable, resolution.FrameMask(2));
        Assert.Equal(GroupResolution.NotAddressable, resolution.FrameMask(1 << 15 | 1));
    }

    private static void GroupsRegistered(GroupResolution resolution, params SnapshotHeader.Group[] groups)
    {
        var names = new byte[Frame.GroupsRegistered.GroupNamesLength];
        int slotLength = names.Length / FrameLayer.MaxGroups;
        foreach (SnapshotHeader.Group group in groups)
        {
            Encoding.ASCII.GetBytes(group.GroupName).CopyTo(names, (group.GroupId - 1) * slotLength);
        }
        var body = new byte[Frame.GroupsRegistered.BlockLength];
        var encoder = new Frame.GroupsRegistered();
        encoder.WrapForEncode(new DirectBuffer(body), 0);
        encoder.GroupNames = names;
        using var wire = new UnsafeBuffer(body);
        resolution.OnGroupsRegistered(wire, 0, body.Length);
    }
}
