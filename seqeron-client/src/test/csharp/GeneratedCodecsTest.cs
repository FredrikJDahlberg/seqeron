using Org.Limitless.Seqeron.Sbe.Frame;
using Org.Limitless.Seqeron.Sbe.Replay;
using Xunit;

namespace Org.Limitless.Seqeron;

// The library carries both schemas' committed codecs, under the ids the wire uses.
public class GeneratedCodecsTest
{
    [Fact]
    public void FrameCodecsAreSchema210()
    {
        Assert.Equal(210, Sequenced.SchemaId);
        Assert.Equal(0, Sequenced.SchemaVersion);
        Assert.Equal(100, Unsequenced.TemplateId);
        Assert.Equal(101, Sequenced.TemplateId);
    }

    [Fact]
    public void ReplayCodecsAreSchema212()
    {
        Assert.Equal(212, ReplayRequest.SchemaId);
        Assert.Equal(0, ReplayRequest.SchemaVersion);
    }
}
