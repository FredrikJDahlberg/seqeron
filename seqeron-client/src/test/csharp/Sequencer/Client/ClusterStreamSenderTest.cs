using Adaptive.Cluster.Client;
using Xunit;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// The timeouts every session opens with, read back from the context it connects with. Aeron.NET's defaults are
/// Aeron's, and its new-leader timeout is shorter than an election, so a client on the defaults would close its own
/// session at every failover. C# only: the Java and C++ senders set the same values, and the harness proves them
/// across a leader kill.
/// </summary>
public class ClusterStreamSenderTest
{
    [Fact(DisplayName = "a session waits 5s for a new leader, and its connect timeout is the caller's")]
    public void SessionContextCarriesTheTimeouts()
    {
        AeronCluster.Context context = ClusterStreamSender.NewContext(null, "aeron:udp", "0=localhost:9302",
                                                                      "aeron:udp?endpoint=localhost:0", null, 1234);

        Assert.Equal(5_000_000_000L, context.NewLeaderTimeoutNs());
        Assert.Equal(1234, context.MessageTimeoutNs());
        Assert.Equal("aeron:udp", context.IngressChannel());
        Assert.Equal("0=localhost:9302", context.IngressEndpoints());
        Assert.Equal("aeron:udp?endpoint=localhost:0", context.EgressChannel());
    }
}
