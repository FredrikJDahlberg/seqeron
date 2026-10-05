using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The port base is a deployment knob (<c>SEQERON_PORT_BASE</c>), and this pins the rules that decide it. Driven
/// through the pure seam rather than the environment: the base is read once into a static field. The cases are
/// <c>PortLayoutTest.java</c>'s, then the pairs <c>SequencerServerTest</c> pins, then <c>ports.sh</c> itself.
/// </summary>
public class PortLayoutTest
{
    [Fact]
    public void UnsetOrBlankFallsBackToTheRegisteredBlock()
    {
        Assert.Equal(9300, PortLayout.ResolveClusterPortBase(null));
        Assert.Equal(9300, PortLayout.ResolveClusterPortBase(""));
        Assert.Equal(9300, PortLayout.ResolveClusterPortBase("   "));
        Assert.Equal(PortLayout.DefaultClusterPortBase, PortLayout.ResolveClusterPortBase(null));
    }

    [Fact]
    public void AValidBaseIsTakenAsGiven()
    {
        Assert.Equal(20000, PortLayout.ResolveClusterPortBase("20000"));
        Assert.Equal(20000, PortLayout.ResolveClusterPortBase("  20000  "));
    }

    [Fact]
    public void NonNumericIsRejectedRatherThanTreatedAsUnset()
    {
        Assert.Throws<ArgumentException>(() => PortLayout.ResolveClusterPortBase("9300x"));
        Assert.Throws<ArgumentException>(() => PortLayout.ResolveClusterPortBase("nine"));
    }

    // 1024 is the first unprivileged port, and the block is 70 wide, so 65466 is the last base that fits. Both
    // boundaries are pinned from either side.
    [Fact]
    public void TheBaseMustLeaveRoomForTheWholeBlockAndAvoidPrivilegedPorts()
    {
        Assert.Throws<ArgumentException>(() => PortLayout.ResolveClusterPortBase("1023"));
        Assert.Equal(1024, PortLayout.ResolveClusterPortBase("1024"));

        Assert.Equal(65466, PortLayout.ResolveClusterPortBase("65466"));
        Assert.Throws<ArgumentException>(() => PortLayout.ResolveClusterPortBase("65467"));
    }

    [Fact(DisplayName = "the endpoint set is built off the port formula, not a restatement of the numbers")]
    public void EndpointsComeFromTheFormula()
    {
        string expected =
            string.Join(",", Enumerable.Range(0, 3).Select(id => $"{id}={PortLayout.IngressEndpoint(id)}"));
        Assert.Equal(expected, PortLayout.IngressEndpoints(3));
    }

    [Fact(DisplayName = "a single-node cluster names one member")]
    public void SingleNodeEndpointSet()
    {
        Assert.Equal("0=" + PortLayout.IngressEndpoint(0), PortLayout.IngressEndpoints(1));
    }

    [Fact(DisplayName = "with SEQERON_HOSTS unset, the default set names the default cluster's members")]
    public void TheDefaultSetIsTheDefaultCluster()
    {
        Assert.Empty(PortLayout.Hosts);
        Assert.Equal(PortLayout.IngressEndpoints(PortLayout.DefaultMemberCount), PortLayout.IngressEndpoints());
    }

    [Fact(DisplayName = "every endpoint the default set names is inside core's reserved block")]
    public void EveryEndpointIsACoreClusterPort()
    {
        foreach (string entry in PortLayout.IngressEndpoints().Split(','))
        {
            int port = int.Parse(entry[(entry.LastIndexOf(':') + 1)..]);
            Assert.True(PortLayout.IsClusterPort(port), entry);
        }
    }

    [Fact(DisplayName = "member i of a host list runs on entry i")]
    public void HostListEndpointSet()
    {
        Assert.Equal(
            $"0=h0:{PortLayout.IngressPort(0)},1=h1:{PortLayout.IngressPort(1)},2=h2:{PortLayout.IngressPort(2)}",
            PortLayout.IngressEndpoints(PortLayout.ParseHosts("h0, h1 ,h2")));
        Assert.Equal(PortLayout.IngressEndpoints(1), PortLayout.IngressEndpoints(new[] { PortLayout.DefaultHost }));
    }

    [Fact(DisplayName = "SEQERON_HOSTS unset or blank names no host; set, it is parsed like any host list")]
    public void HostsFromTheEnvironment()
    {
        Assert.Empty(PortLayout.ResolveHosts(null));
        Assert.Empty(PortLayout.ResolveHosts("  "));
        Assert.Equal(new[] { "h0", "h1" }, PortLayout.ResolveHosts("h0,h1"));
        Assert.Throws<ArgumentException>(() => PortLayout.ResolveHosts("h0,,h1"));
    }

    [Fact(DisplayName = "a host list with a blank entry, or more members than the block holds, is refused")]
    public void HostListRules()
    {
        Assert.Equal(new[] { "h0" }, PortLayout.ParseHosts("h0"));
        Assert.Throws<ArgumentException>(() => PortLayout.ParseHosts(""));
        Assert.Throws<ArgumentException>(() => PortLayout.ParseHosts("h0,,h2"));
        Assert.Throws<ArgumentException>(() => PortLayout.ParseHosts("h0,h1,"));
        Assert.Equal(7, PortLayout.ParseHosts("h0,h1,h2,h3,h4,h5,h6").Count);
        Assert.Throws<ArgumentException>(() => PortLayout.ParseHosts("h0,h1,h2,h3,h4,h5,h6,h7"));
    }

    // The documented layout, as SequencerServerTest pins it.
    [Fact(DisplayName = "member ports and the reserved block match the documented layout")]
    public void MemberPortsMatchTheDocumentedLayout()
    {
        Assert.Equal("localhost:9302", PortLayout.IngressEndpoint(0));
        Assert.Equal("localhost:9312", PortLayout.IngressEndpoint(1));
        Assert.Equal(new[] { 9321, 9322, 9323, 9324, 9325 },
                     new[] { PortLayout.ArchivePort(2), PortLayout.IngressPort(2), PortLayout.ConsensusPort(2),
                             PortLayout.LogPort(2), PortLayout.TransferPort(2) });

        Assert.Equal(9300, PortLayout.ClusterPortBlockFirst);
        Assert.Equal(9369, PortLayout.ClusterPortBlockLast);
        Assert.True(PortLayout.IsClusterPort(9360), "member 6's base, reserved though unbound");
        Assert.False(PortLayout.IsClusterPort(9299));
        Assert.False(PortLayout.IsClusterPort(9370));
    }

    // ports.sh is the fourth mirror. Its formula is compared at a moved base as well as the default, as offsets
    // from the base, since this process's own base is fixed at 9300.
    [Theory(DisplayName = "the formula is ports.sh's")]
    [InlineData(null)]
    [InlineData("20000")]
    public void TheFormulaIsPortsSh(string portBase)
    {
        // Forward slashes, which Git Bash on Windows reads as well as bash elsewhere does.
        string script =
            Path.Combine(RepositoryRoot(), "seqeron-service", "src", "main", "scripts", "ports.sh").Replace('\\', '/');
        string[] lines = Bash($"source '{script}'; for m in 0 1 2 3 4 5 6; do " +
                                  "echo $(( $(cluster_member_port_base $m) - CLUSTER_PORT_BASE ))" +
                                  " $(( $(archive_port $m) - CLUSTER_PORT_BASE ))" +
                                  " $(( $(ingress_port $m) - CLUSTER_PORT_BASE )); done; ingress_endpoints_string 3",
                              portBase);

        int shift =
            (portBase == null ? PortLayout.DefaultClusterPortBase : int.Parse(portBase)) - PortLayout.ClusterPortBase;
        for (int member = 0; member < PortLayout.ClusterMemberCount; member++)
        {
            int[] expected = { PortLayout.MemberPortBase(member), PortLayout.ArchivePort(member),
                               PortLayout.IngressPort(member) };
            Assert.Equal(string.Join(' ', expected.Select(port => port - PortLayout.ClusterPortBase)), lines[member]);
        }
        string endpoints = string.Join(
            ",", Enumerable.Range(0, 3).Select(id => $"{id}=localhost:{PortLayout.IngressPort(id) + shift}"));
        Assert.Equal(endpoints, lines[PortLayout.ClusterMemberCount]);
    }

    private static string[] Bash(string command, string portBase)
    {
        var start = new ProcessStartInfo(BashPath(), new[] { "-c", command }) { RedirectStandardOutput = true };
        start.Environment.Remove(PortLayout.EnvPortBase);
        if (portBase != null)
        {
            start.Environment[PortLayout.EnvPortBase] = portBase;
        }
        Process bash;
        try
        {
            bash = Process.Start(start);
        }
        catch (System.ComponentModel.Win32Exception)
        {
            Assert.Skip("no bash on this host");
            throw;
        }
        string output = bash.StandardOutput.ReadToEnd();
        bash.WaitForExit();
        Assert.Equal(0, bash.ExitCode);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    // Windows starts System32's bash, WSL's launcher, ahead of the PATH's; take the PATH's, Git Bash on a runner.
    private static string BashPath()
    {
        if (!OperatingSystem.IsWindows())
        {
            return "bash";
        }
        foreach (string dir in (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator))
        {
            string candidate = Path.Combine(dir, "bash.exe");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }
        return "bash";
    }

    private static string RepositoryRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "versions.properties")))
            {
                return dir.FullName;
            }
        }
        throw new InvalidOperationException("no versions.properties above " + AppContext.BaseDirectory);
    }
}
