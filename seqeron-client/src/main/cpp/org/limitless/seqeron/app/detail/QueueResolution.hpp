#pragma once

#include <cstdint>

#include "org_limitless_seqeron_sbe_replay/MessageHeader.h"
#include "org_limitless_seqeron_sbe_replay/QueueResolved.h"
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"

namespace org::limitless::seqeron::app::detail {

/**
 * Which queue this instance reads (spec R-6): a QueueRequest to the node's Replayer, resent until it answers with a
 * QueueResolved. Until then the Replayer answers ReplayPending, which holds the resend off as it does a replay
 * request's. Holds no Aeron runtime; Session is its transport. The Java twin is app/QueueResolution.java; keep the
 * two in step.
 */
class QueueResolution
{
  public:
    // groupId() before the Replayer has answered.
    static constexpr std::int32_t UNRESOLVED = 0;
    static constexpr std::int64_t RESEND_INTERVAL_MS = 500;

    // What this cannot do itself: the send, the id replies are addressed to, and the clock.
    class Actions
    {
      public:
        // Offers one QueueRequest; best-effort, since the resend timer retries.
        virtual void sendQueueRequest(std::int64_t requestId) = 0;
        // The id the Replayer addresses this instance's replies to (R-4).
        virtual std::int64_t clientId() = 0;
        // Monotonic millis.
        virtual std::int64_t nowMs() = 0;

      protected:
        ~Actions() = default;
    };

    explicit QueueResolution(Actions& actions) : m_actions{ actions }
    {}

    // Sends the first request.
    void start()
    {
        m_started = true;
        send();
    }

    /**
     * The resend, once per duty cycle.
     *
     * @param publicationPending the request publication has not connected yet: retried every cycle rather than
     *                           waiting out a resend interval for a race much shorter than one
     */
    void doTimers(const bool publicationPending)
    {
        if (m_started && m_groupId == UNRESOLVED &&
            (publicationPending || m_actions.nowMs() - m_lastRequestMs > RESEND_INTERVAL_MS))
        {
            send();
        }
    }

    /**
     * Takes one control message; only a reply to the current request counts (R-1, R-4).
     *
     * @param message the message's first byte
     * @param length  its length
     */
    void onControl(char* const message, const std::uint64_t length)
    {
        if (m_groupId != UNRESOLVED || length < sbe::replay::MessageHeader::encodedLength())
        {
            return;
        }
        sbe::replay::MessageHeader header;
        header.wrap(message, 0, 0, length);
        if (header.schemaId() != sbe::replay::MessageHeader::sbeSchemaId())
        {
            return;
        }
        const std::uint64_t bodyOffset = sbe::replay::MessageHeader::encodedLength();
        if (header.templateId() == sbe::replay::QueueResolved::sbeTemplateId())
        {
            sbe::replay::QueueResolved resolved;
            resolved.wrapForDecode(message, bodyOffset, header.blockLength(), header.version(), length);
            if (resolved.clientId() == m_actions.clientId() && resolved.requestId() == m_requestId)
            {
                m_groupId = resolved.queueId();
            }
        }
        else if (header.templateId() == sbe::replay::ReplayPending::sbeTemplateId())
        {
            sbe::replay::ReplayPending pending;
            pending.wrapForDecode(message, bodyOffset, header.blockLength(), header.version(), length);
            if (pending.clientId() == m_actions.clientId() && pending.requestId() == m_requestId)
            {
                m_lastRequestMs = m_actions.nowMs();
            }
        }
    }

    // The group whose queue this instance reads, or UNRESOLVED.
    [[nodiscard]] std::int32_t groupId() const
    {
        return m_groupId;
    }

    // The id the next reply must carry to count.
    [[nodiscard]] std::int64_t requestId() const
    {
        return m_requestId;
    }

  private:
    void send()
    {
        m_lastRequestMs = m_actions.nowMs();
        m_actions.sendQueueRequest(++m_requestId);
    }

    Actions& m_actions;
    bool m_started = false;
    std::int64_t m_requestId = 0;
    std::int64_t m_lastRequestMs = 0;
    std::int32_t m_groupId = UNRESOLVED;
};

} // namespace org::limitless::seqeron::app::detail
