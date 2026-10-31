namespace Org.Limitless.Seqeron.App;

/// <summary>What takes a façade into snapshot rounds: the listener, and the directory where the instance keeps its
/// snapshots, one file per round. The directory is the instance's own (no other instance, its pair's included, may
/// write it), and a restart restores from what it holds, so it must outlive the process.</summary>
public sealed class SnapshotOptions
{
    /// <summary>Serializes the instance's state for a round and restores it on start.</summary>
    public required ISnapshotListener Listener { get; init; }

    /// <summary>Where the instance keeps its snapshot files.</summary>
    public required string Directory { get; init; }
}
