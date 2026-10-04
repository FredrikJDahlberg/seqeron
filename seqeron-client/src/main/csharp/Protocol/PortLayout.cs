using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The cluster port-layout formula: the C# mirror of <c>PortLayout.java</c>, <c>PortLayout.hpp</c> and
/// <c>ports.sh</c>; change all four together. Cluster member ports only (doc/ops.md, "Ports").
/// </summary>
public static class PortLayout
{
    /// <summary>Every default endpoint below is on one host; a real deployment overrides them wholesale.</summary>
    public const string DefaultHost = "localhost";

    /// <summary>Members of the default cluster on <see cref="DefaultHost"/>, what the default endpoint sets
    /// name.</summary>
    public const int DefaultMemberCount = 3;

    /// <summary>
    /// Deployment-wide port-base override, read by every mirror. Set it identically for every seqeron process: a
    /// disagreement shows as a connection that never completes, not as an error.
    /// </summary>
    public const string EnvPortBase = "SEQERON_PORT_BASE";

    /// <summary>
    /// Deployment-wide host list, <c>"h0,h1,h2"</c>: member <c>i</c> runs on entry <c>i</c>. It names the members
    /// wherever a default would otherwise say <see cref="DefaultHost"/>.
    /// </summary>
    public const string EnvHosts = "SEQERON_HOSTS";

    /// <summary>The base when <see cref="EnvPortBase"/> is unset: core's registered block.</summary>
    public const int DefaultClusterPortBase = 9300;

    /// <summary>Ports one member takes, so member <c>m</c>'s block starts at <c>base + m * stride</c>.</summary>
    public const int ClusterPortStride = 10;

    /// <summary>Members a cluster is bounded at, since the reserved block is this many strides wide: Raft's 3, 5
    /// or 7.</summary>
    public const int ClusterMemberCount = 7;

    private const int ClusterPortBlockWidth = ClusterMemberCount * ClusterPortStride;
    private const int MinPortBase = 1024;
    private const int MaxPort = 65535;

    /// <summary>The base this process runs on, <see cref="EnvPortBase"/>'s value or
    /// <see cref="DefaultClusterPortBase"/>.</summary>
    public static readonly int ClusterPortBase =
        ResolveClusterPortBase(Environment.GetEnvironmentVariable(EnvPortBase));

    /// <summary>The members' hosts from <see cref="EnvHosts"/>, empty when it is unset.</summary>
    public static readonly IReadOnlyList<string> Hosts = ResolveHosts(Environment.GetEnvironmentVariable(EnvHosts));

    /// <summary>First port of core's reserved block (doc/ops.md, "Ports"): seven members wide, one stride each,
    /// wider than the base+1..base+65 seven members bind.</summary>
    public static readonly int ClusterPortBlockFirst = ClusterPortBase;

    /// <summary>Last port of core's reserved block.</summary>
    public static readonly int ClusterPortBlockLast = ClusterPortBase + ClusterPortBlockWidth - 1;

    /// <summary>How a process reaches the archive in its own Aeron directory: no endpoint, so no port to
    /// allocate.</summary>
    public const string ArchiveControlChannel = "aeron:ipc";

    /// <summary>
    /// Control stream of that link. The archive's own local control and every archive client must name the same
    /// id, so it is stated here once rather than per process.
    /// </summary>
    public const int ArchiveControlStreamId = 100;

    /// <summary>
    /// Parses <see cref="EnvPortBase"/>; a pure function so it is testable without the environment. A bad value
    /// fails here rather than as a bind error later.
    /// </summary>
    /// <param name="raw">the raw environment value, or null when unset</param>
    /// <returns>the configured base, or <see cref="DefaultClusterPortBase"/></returns>
    /// <exception cref="ArgumentException">if the value is not a number, or the block does not fit above the
    /// privileged ports and below 65535</exception>
    internal static int ResolveClusterPortBase(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultClusterPortBase;
        }
        if (!int.TryParse(raw.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int portBase))
        {
            throw new ArgumentException($"{EnvPortBase} is not a number: '{raw}'");
        }
        if (portBase < MinPortBase)
        {
            throw new ArgumentException($"{EnvPortBase}={portBase} is below {MinPortBase} (privileged ports)");
        }
        if (portBase + ClusterPortBlockWidth - 1 > MaxPort)
        {
            throw new ArgumentException($"{EnvPortBase}={portBase} leaves no room for the {ClusterPortBlockWidth}" +
                                        $"-port cluster block below {MaxPort}");
        }
        return portBase;
    }

    /// <summary>Parses <see cref="EnvHosts"/>; a pure function so it is testable without the environment.</summary>
    /// <param name="raw">the raw environment value, or null when unset</param>
    /// <returns>the host list, empty when <paramref name="raw"/> is unset or blank</returns>
    internal static IReadOnlyList<string> ResolveHosts(string raw)
    {
        return string.IsNullOrWhiteSpace(raw) ? Array.Empty<string>() : ParseHosts(raw);
    }

    /// <summary>Whether a port falls inside core's reservation, for a product checking its own bases.</summary>
    /// <param name="port">the port to check</param>
    public static bool IsClusterPort(int port)
    {
        return port >= ClusterPortBlockFirst && port <= ClusterPortBlockLast;
    }

    /// <summary>First port of one member's stride; the five below sit at fixed offsets from it.</summary>
    /// <param name="memberId">the member</param>
    public static int MemberPortBase(int memberId)
    {
        return ClusterPortBase + memberId * ClusterPortStride;
    }

    /// <summary>That member's Aeron Archive control port.</summary>
    /// <param name="memberId">the member</param>
    public static int ArchivePort(int memberId)
    {
        return MemberPortBase(memberId) + 1;
    }

    /// <summary>That member's cluster-ingress port, where a producer submits over UDP.</summary>
    /// <param name="memberId">the member</param>
    public static int IngressPort(int memberId)
    {
        return MemberPortBase(memberId) + 2;
    }

    /// <summary>That member's consensus port, which the cluster's members use among themselves.</summary>
    /// <param name="memberId">the member</param>
    public static int ConsensusPort(int memberId)
    {
        return MemberPortBase(memberId) + 3;
    }

    /// <summary>That member's log port, where it replicates the Raft log.</summary>
    /// <param name="memberId">the member</param>
    public static int LogPort(int memberId)
    {
        return MemberPortBase(memberId) + 4;
    }

    /// <summary>That member's log-transfer port, used to catch a member up.</summary>
    /// <param name="memberId">the member</param>
    public static int TransferPort(int memberId)
    {
        return MemberPortBase(memberId) + 5;
    }

    /// <summary>A member's cluster-ingress endpoint, <c>"host:port"</c>.</summary>
    /// <param name="memberId">the member</param>
    public static string IngressEndpoint(int memberId)
    {
        return DefaultHost + ":" + IngressPort(memberId);
    }

    /// <summary>Parses a cluster's host list, <c>"h0,h1,h2"</c>: member <c>i</c> runs on entry <c>i</c>.</summary>
    /// <param name="csv">the members' host names, comma-separated</param>
    /// <returns>the host names, trimmed</returns>
    /// <exception cref="ArgumentException">if an entry is blank, or the list names more than
    /// <see cref="ClusterMemberCount"/> members</exception>
    public static IReadOnlyList<string> ParseHosts(string csv)
    {
        string[] hosts = csv.Split(',').Select(host => host.Trim()).ToArray();
        if (hosts.Contains(""))
        {
            throw new ArgumentException($"host list has a blank entry: '{csv}'");
        }
        if (hosts.Length > ClusterMemberCount)
        {
            throw new ArgumentException($"host list names {hosts.Length} members, more than the " +
                                        $"{ClusterMemberCount} the port block holds: '{csv}'");
        }
        return Array.AsReadOnly(hosts);
    }

    /// <summary>
    /// The ingress endpoint set of a <paramref name="nodeCount"/>-member cluster on <see cref="DefaultHost"/>,
    /// <c>"0=host:9302,1=host:9312,…"</c>: the form <c>AeronCluster</c> and <c>clusterctl</c> take, and the mirror
    /// of <c>ports.sh</c>'s <c>ingress_endpoints_string</c>.
    /// </summary>
    /// <param name="nodeCount">members in the cluster</param>
    public static string IngressEndpoints(int nodeCount)
    {
        return IngressEndpoints(Enumerable.Repeat(DefaultHost, nodeCount).ToArray());
    }

    /// <summary>The ingress endpoint set of a cluster whose member <c>i</c> runs on <c>hosts[i]</c>.</summary>
    /// <param name="hosts">the members' host names, as <see cref="ParseHosts"/> returns them</param>
    public static string IngressEndpoints(IReadOnlyList<string> hosts)
    {
        var endpoints = new StringBuilder();
        for (int id = 0; id < hosts.Count; id++)
        {
            if (id > 0)
            {
                endpoints.Append(',');
            }
            endpoints.Append(id).Append('=').Append(hosts[id]).Append(':').Append(IngressPort(id));
        }
        return endpoints.ToString();
    }

    /// <summary>The default endpoint set: the members <see cref="Hosts"/> names, else a
    /// <see cref="DefaultMemberCount"/>-member cluster on <see cref="DefaultHost"/>.</summary>
    public static string IngressEndpoints()
    {
        return Hosts.Count == 0 ? IngressEndpoints(DefaultMemberCount) : IngressEndpoints(Hosts);
    }
}
