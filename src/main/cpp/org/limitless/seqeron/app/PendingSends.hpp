#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "org/limitless/seqeron/sequencer/IngressTracker.hpp"
#include "org/limitless/seqeron/sequencer/SequencedFrame.hpp"

#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedHeader.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedSystem.h"

namespace org::limitless::seqeron::app {

// A producer's ingress frames that its own tap has not yet shown, which of them a leader change lost (every
// frame still pending with a term below the latest LeadershipChanged's), and their resend ahead of anything
// new: while isHolding(), send nothing new, and give this to the sender with setIngressHold. Own frames are
// matched by session, and one that differs from the oldest pending copy latches isFaulted(). The Java twin is
// app/PendingSends.java, which carries the rationale and the limits; keep the two in step.
class PendingSends : public sequencer::IngressTracker
{
  public:
    // capacity: frames that may be pending at once; each holds one MAX_INGRESS_LENGTH copy.
    explicit PendingSends(const std::size_t capacity) : m_frames(capacity * SLOT_LENGTH), m_entries(capacity)
    {}

    // Check before sending: a frame that cannot be tracked must not be sent.
    [[nodiscard]] bool isFull() const noexcept override
    {
        return m_size == m_entries.size();
    }

    // A frame the sender placed, read straight after its send() returned true, with the sender's
    // clusterSessionId() and leadershipTermId(). Tracking into a full ring latches the fault.
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
        m_entries[slot] = Entry{ length, clusterSessionId, leadershipTermId };
        ++m_size;
    }

    // From the stream client's leadership callback: closes the count on every earlier term.
    void onLeadershipChanged(const std::int64_t leadershipTermId)
    {
        m_closedTermId = std::max(m_closedTermId, leadershipTermId);
    }

    // From the sender, which calls it on every NewLeaderEvent.
    void onNewLeader(const std::int64_t leadershipTermId) override
    {
        m_newLeaderTermId = std::max(m_newLeaderTermId, leadershipTermId);
    }

    // Send nothing new while this holds: an older term's frames are still unseen or unresent.
    [[nodiscard]] bool isHolding() const override
    {
        return m_size > 0 && m_entries[m_head].termId < std::max(m_newLeaderTermId, m_closedTermId);
    }

    // Resends the missing frames oldest first through sender (send(bytes, length), clusterSessionId(),
    // leadershipTermId()), each going back to the end of the ring as pending under the term it now carries.
    // Stops at the first send that fails, so a later call picks up where this left off. Returns how many.
    template<typename Sender>
    std::size_t resendMissing(Sender& sender)
    {
        if (sender.leadershipTermId() < m_closedTermId)
        {
            return 0; // the sender has no NewLeader yet, and the leader would drop them again
        }
        std::size_t resent = 0;
        while (m_size > 0 && m_entries[m_head].termId < m_closedTermId)
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

    // Every frame off the tap, in order. Only this producer's own frames change anything.
    void onSequenced(const sequencer::SequencedEvent& event)
    {
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

    // Pending frames a leader change has lost, oldest first at the front.
    [[nodiscard]] std::size_t missing() const noexcept
    {
        std::size_t count = 0;
        while (count < m_size && m_entries[slotOf(count)].termId < m_closedTermId)
        {
            ++count;
        }
        return count;
    }

    // Frames tracked and not yet seen on the tap, missing ones included.
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
    static constexpr std::size_t SLOT_LENGTH = sequencer::MAX_INGRESS_LENGTH;

    struct Entry
    {
        std::uint16_t length = 0;
        std::int64_t sessionId = -1;
        std::int64_t termId = -1;
    };

    [[nodiscard]] std::size_t slotOf(const std::size_t index) const noexcept
    {
        return (m_head + index) % m_entries.size();
    }

    // The two families' headers share their layout (F-3), so one codec reads either one's id at offset 16.
    [[nodiscard]] bool matches(const std::size_t slot, const sequencer::SequencedEvent& event)
    {
        namespace frm = sbe::frame;
        char* base = reinterpret_cast<char*>(m_frames.data() + slot * SLOT_LENGTH);
        const std::uint16_t length = m_entries[slot].length;
        frm::MessageHeader messageHeader(base, 0, length, frm::MessageHeader::sbeSchemaVersion());
        const bool system = messageHeader.templateId() == frm::UnsequencedSystem::sbeTemplateId();
        frm::UnsequencedHeader frameHeader(base, frm::MessageHeader::encodedLength(), length,
                                           frm::MessageHeader::sbeSchemaVersion());
        const std::size_t bodyLength = length - sequencer::MIN_INGRESS_LENGTH;
        return event.system == system &&
               (system ? event.systemEventType : event.payloadId) == frameHeader.payloadId() &&
               event.payloadLength == bodyLength &&
               std::memcmp(base + sequencer::MIN_INGRESS_LENGTH, event.payload, bodyLength) == 0;
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
    bool m_faulted = false;
};

} // namespace org::limitless::seqeron::app
