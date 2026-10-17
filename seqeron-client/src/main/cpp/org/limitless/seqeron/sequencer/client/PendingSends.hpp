#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressTracker.hpp"

#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/SessionClosed.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedHeader.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedSystem.h"

namespace org::limitless::seqeron::sequencer::client {

// A producer's ingress frames that its own queue has not yet shown, which of them a leader change lost (every
// frame still pending with a term below the latest LeadershipChanged's) or a closed session lost (every frame
// still pending on a session behind its SessionClosed), and their resend ahead of anything new: while
// isHolding(), send nothing new, and give this to the sender with setIngressHold. Own frames are matched by
// session, and one that differs from the oldest pending copy latches isFaulted(). The Java twin is
// sequencer/client/PendingSends.java, which carries the rationale and the limits; keep the two in step.
class PendingSends : public IngressTracker
{
  public:
    /**
     * Creates an empty tracker.
     *
     * @param capacity frames that may be pending at once; each holds one MAX_INGRESS_LENGTH copy
     */
    explicit PendingSends(const std::size_t capacity) : m_frames(capacity * SLOT_LENGTH), m_entries(capacity)
    {}

    // Check before sending: a frame that cannot be tracked must not be sent.
    [[nodiscard]] bool isFull() const noexcept override
    {
        return m_size == m_entries.size();
    }

    /**
     * Records a frame the sender placed, read straight after its send() returned true. Tracking into a full
     * ring latches the fault.
     *
     * @param frame            the frame's first byte; copied
     * @param length           the frame's length
     * @param clusterSessionId the sender's clusterSessionId()
     * @param leadershipTermId the sender's leadershipTermId()
     */
    void track(const std::uint8_t* frame, const std::uint16_t length, const std::int64_t clusterSessionId,
               const std::int64_t leadershipTermId) override
    {
        if (isFull())
        {
            m_faulted = true;
            return;
        }
        const std::size_t slot = slotOf(m_size);
        std::memcpy(m_frames.data() + slot * SLOT_LENGTH, frame, length);
        m_entries[slot] = Entry{ length, clusterSessionId, leadershipTermId, clusterSessionId == m_closedSessionId };
        m_liveSessionId = clusterSessionId;
        ++m_size;
    }

    /**
     * Closes the count on every earlier term. Called from the stream client's leadership callback.
     *
     * @param leadershipTermId the LeadershipChanged frame's term
     */
    void onLeadershipChanged(const std::int64_t leadershipTermId)
    {
        m_closedTermId = std::max(m_closedTermId, leadershipTermId);
    }

    /**
     * Records that egress named a new leader. Called by the sender on every NewLeaderEvent.
     *
     * @param leadershipTermId the new leader's term
     */
    void onNewLeader(const std::int64_t leadershipTermId) override
    {
        m_newLeaderTermId = std::max(m_newLeaderTermId, leadershipTermId);
    }

    // Send nothing new while this holds: an older term's or session's frames are still unsettled or unresent.
    [[nodiscard]] bool isHolding() const override
    {
        const Entry& head = m_entries[m_head];
        return m_size > 0 && (head.termId < std::max(m_newLeaderTermId, m_closedTermId) || head.lost ||
                              head.sessionId != m_liveSessionId);
    }

    /**
     * Resends the missing frames oldest first, each going back to the end of the ring as pending under the
     * term and session it now carries. Stops at the first send that fails, so a later call picks up where this
     * left off. Call it every duty cycle: it is also where this learns which session the sender holds.
     *
     * @param sender anything with send(bytes, length), clusterSessionId() and leadershipTermId()
     * @return how many frames were resent
     */
    template<typename Sender>
    std::size_t resendMissing(Sender& sender)
    {
        m_liveSessionId = sender.clusterSessionId();
        if (sender.leadershipTermId() < m_closedTermId || m_liveSessionId == m_closedSessionId)
        {
            return 0; // the sender has no NewLeader, or no replacement session, yet: the leader would drop them again
        }
        std::size_t resent = 0;
        while (m_size > 0 && isMissing(m_head))
        {
            if (!sender.send(m_frames.data() + m_head * SLOT_LENGTH, m_entries[m_head].length))
            {
                break;
            }
            moveHeadToTail(sender.clusterSessionId(), sender.leadershipTermId());
            ++resent;
        }
        return resent;
    }

    /**
     * Matches a frame off the queue against the pending ones. Only this producer's own frames and sessions change
     * anything.
     *
     * @param event every frame off the queue, in order
     */
    void onSequenced(const protocol::SequencedEvent& event)
    {
        if (event.system && event.systemEventType == protocol::SESSION_CLOSED)
        {
            onSessionClosed(protocol::decodeSystem<sbe::frame::SessionClosed>(event).clusterSessionId());
            return;
        }
        const std::size_t index = missing();
        if (index == m_size)
        {
            return;
        }
        const std::size_t slot = slotOf(index);
        if (event.sourceSessionId != m_entries[slot].sessionId)
        {
            return;
        }
        if (!matches(slot, event))
        {
            m_faulted = true;
            return;
        }
        remove(index);
    }

    /**
     * Forgets every pending frame, for a producer that knows none of them will be sequenced. Any that are still
     * arrive and match nothing.
     *
     * @return how many were dropped
     */
    std::size_t discardUnconfirmed() noexcept
    {
        const std::size_t dropped = m_size;
        m_head = 0;
        m_size = 0;
        return dropped;
    }

    // Pending frames a leader change or a closed session has lost, oldest first at the front.
    [[nodiscard]] std::size_t missing() const noexcept
    {
        std::size_t count = 0;
        while (count < m_size && isMissing(slotOf(count)))
        {
            ++count;
        }
        return count;
    }

    /**
     * Whether the queue has shown the sender's own session closed.
     *
     * @param clusterSessionId the sender's clusterSessionId()
     */
    [[nodiscard]] bool isClosed(const std::int64_t clusterSessionId) const noexcept
    {
        return clusterSessionId != NO_SESSION && clusterSessionId == m_closedSessionId;
    }

    // Frames tracked and not yet seen on the queue, missing ones included.
    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_size;
    }

    // Latched: the count can no longer be trusted, so fence the producer.
    [[nodiscard]] bool isFaulted() const noexcept
    {
        return m_faulted;
    }

  private:
    static constexpr std::size_t SLOT_LENGTH = protocol::MAX_INGRESS_LENGTH;
    static constexpr std::int64_t NO_SESSION = -1;

    struct Entry
    {
        std::uint16_t length = 0;
        std::int64_t sessionId = -1;
        std::int64_t termId = -1;
        bool lost = false; ///< its session closed on the queue before the frame appeared there
    };

    [[nodiscard]] std::size_t slotOf(const std::size_t index) const noexcept
    {
        return (m_head + index) % m_entries.size();
    }

    [[nodiscard]] bool isMissing(const std::size_t slot) const noexcept
    {
        return m_entries[slot].termId < m_closedTermId || m_entries[slot].lost;
    }

    // Every pending frame on that session is lost; one tracked on it later is lost already.
    void onSessionClosed(const std::int64_t clusterSessionId) noexcept
    {
        bool own = clusterSessionId == m_liveSessionId;
        for (std::size_t i = 0; i < m_size; ++i)
        {
            Entry& entry = m_entries[slotOf(i)];
            if (entry.sessionId == clusterSessionId)
            {
                entry.lost = true;
                own = true;
            }
        }
        if (own)
        {
            m_closedSessionId = clusterSessionId;
        }
    }

    // The two families' headers share their layout (F-3), so one codec reads either one's id at offset 16.
    [[nodiscard]] bool matches(const std::size_t slot, const protocol::SequencedEvent& event)
    {
        namespace frm = sbe::frame;
        char* base = reinterpret_cast<char*>(m_frames.data() + slot * SLOT_LENGTH);
        const std::uint16_t length = m_entries[slot].length;
        frm::MessageHeader messageHeader(base, 0, length, frm::MessageHeader::sbeSchemaVersion());
        const bool system = messageHeader.templateId() == frm::UnsequencedSystem::sbeTemplateId();
        frm::UnsequencedHeader frameHeader(base, frm::MessageHeader::encodedLength(), length,
                                           frm::MessageHeader::sbeSchemaVersion());
        const std::size_t bodyLength = length - protocol::MIN_INGRESS_LENGTH;
        return event.system == system &&
               (system ? event.systemEventType : event.payloadId) == frameHeader.payloadId() &&
               event.payloadLength == bodyLength &&
               std::memcmp(base + protocol::MIN_INGRESS_LENGTH, event.payload, bodyLength) == 0;
    }

    void moveHeadToTail(const std::int64_t clusterSessionId, const std::int64_t leadershipTermId)
    {
        const std::size_t from = m_head;
        const std::size_t to = slotOf(m_size);
        if (to != from)
        {
            std::memcpy(m_frames.data() + to * SLOT_LENGTH, m_frames.data() + from * SLOT_LENGTH,
                        m_entries[from].length);
        }
        m_entries[to] = Entry{ m_entries[from].length, clusterSessionId, leadershipTermId };
        m_head = slotOf(1);
    }

    // Drops the entry at index, moving the missing ones ahead of it up one slot to keep them in order.
    void remove(const std::size_t index)
    {
        for (std::size_t i = index; i > 0; --i)
        {
            const std::size_t to = slotOf(i);
            const std::size_t from = slotOf(i - 1);
            std::memcpy(m_frames.data() + to * SLOT_LENGTH, m_frames.data() + from * SLOT_LENGTH,
                        m_entries[from].length);
            m_entries[to] = m_entries[from];
        }
        m_head = slotOf(1);
        --m_size;
    }

    std::vector<std::uint8_t> m_frames;
    std::vector<Entry> m_entries;
    std::size_t m_head = 0;
    std::size_t m_size = 0;
    std::int64_t m_closedTermId = -1;
    std::int64_t m_newLeaderTermId = -1;
    std::int64_t m_liveSessionId = NO_SESSION;   ///< the sender's session, as of the last track or resend
    std::int64_t m_closedSessionId = NO_SESSION; ///< the last own session the queue showed closed
    bool m_faulted = false;
};

} // namespace org::limitless::seqeron::sequencer::client
