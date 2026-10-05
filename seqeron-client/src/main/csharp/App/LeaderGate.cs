namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Whether this replica may do leader-only work: caught up, and its node is the leader — or, off the cluster,
/// caught up alone. Gives edges rather than a boolean, because <c>OutstandingWork.OnNotLeader</c> belongs on the
/// closing edge.
/// <para><b>Every leadership change closes an open gate</b>, not only one that ends with another leader.
/// <see cref="Update"/> runs once per duty cycle, so a flip away and back applied within one cycle reads the same
/// member before and after. A reply this node sent during that election may have been lost with it, so the gate
/// closes for one cycle and the tracker re-dispatches. Call <see cref="OnLeadershipChanged"/> from the receiver's
/// leadership callback.</para>
/// <para><c>LeaderGate.java</c> and <c>app/detail/LeaderGate.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
internal sealed class LeaderGate
{
    /// <summary>An edge crossed by one <see cref="Update"/>.</summary>
    public enum Transition
    {
        /// <summary>The gate stands where it stood.</summary>
        None,

        /// <summary>This replica may now do leader-only work.</summary>
        Opened,

        /// <summary>It may not: re-dispatch on the next open rather than assume anything completed.</summary>
        Closed
    }

    private readonly int _memberId;
    private readonly bool _offCluster;

    private bool _open;
    private bool _leadershipChanged;

    /// <summary>A gate that starts shut.</summary>
    /// <param name="memberId">this node's cluster member id</param>
    /// <param name="offCluster">whether this node runs no member, so that no leadership is its own and the gate
    /// opens whoever leads</param>
    public LeaderGate(int memberId, bool offCluster = false)
    {
        _memberId = memberId;
        _offCluster = offCluster;
    }

    /// <summary>A <c>LeadershipChanged</c> frame was applied; an open gate closes on the next
    /// <see cref="Update"/>.</summary>
    public void OnLeadershipChanged()
    {
        _leadershipChanged = true;
    }

    /// <summary>Evaluates the gate once per duty cycle.</summary>
    /// <param name="caughtUp">the receiver's <c>IsCaughtUp</c></param>
    /// <param name="currentLeaderMemberId">the receiver's <c>CurrentLeaderMemberId</c></param>
    /// <returns>the edge crossed, if any</returns>
    public Transition Update(bool caughtUp, int currentLeaderMemberId)
    {
        bool wasOpen = _open;
        bool restart = _leadershipChanged;
        _leadershipChanged = false;
        _open = !(wasOpen && restart) && caughtUp && (_offCluster || currentLeaderMemberId == _memberId);
        if (_open == wasOpen)
        {
            return Transition.None;
        }
        return _open ? Transition.Opened : Transition.Closed;
    }

    /// <summary>Whether leader-only work may run, without waiting for the next <see cref="Update"/>.</summary>
    public bool IsOpen => _open;
}
