using System;
using Org.Limitless.Seqeron.Util;
using Xunit;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// Unit test for the receive stamp the adapter puts on every frame, <see cref="Clocks.EpochNanos"/>. No Aeron
/// runtime: the clock is all this touches. Case for case with <c>ReplayerStreamReceiverTest.java</c>.
/// </summary>
public class ReplayerStreamReceiverTest
{
    [Fact(DisplayName = "the receive stamp has sub-millisecond resolution")]
    public void ReceiveStampIsNotMillisecondQuantised()
    {
        // A millisecond clock's stamp is always an exact multiple of 1_000_000ns, so it cannot resolve the latency
        // it is sampled for. One reading off a whole millisecond disproves that; 1000 of them make a finer clock
        // landing on the boundary every time impossible in practice.
        bool subMillisecond = false;
        for (int i = 0; i < 1000 && !subMillisecond; i++)
        {
            subMillisecond = Clocks.EpochNanos() % 1_000_000L != 0;
        }

        Assert.True(subMillisecond, "every stamp landed on a whole millisecond");
    }

    [Fact(DisplayName = "the receive stamp is on the epoch the cluster timestamp is measured in")]
    public void ReceiveStampIsEpochBased()
    {
        // It is subtracted from ClusterTimestampNs, so a monotonic-since-boot clock would be meaningless here
        // however fine its resolution.
        long epochMs = Clocks.EpochNanos() / 1_000_000L;

        Assert.True(Math.Abs(epochMs - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) < 1_000,
                    "stamp is not wall-clock epoch ms");
    }
}
