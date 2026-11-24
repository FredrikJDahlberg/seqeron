#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"

#include "org/limitless/seqeron/protocol/ReplayProtocol.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"

#include "org_limitless_seqeron_sbe_replay/MessageHeader.h"
#include "org_limitless_seqeron_sbe_replay/QueueRequest.h"
#include "org_limitless_seqeron_sbe_replay/QueueResolved.h"
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"
#include "org_limitless_seqeron_sbe_replay/ReplayRequest.h"
#include "org_limitless_seqeron_sbe_replay/Replaying.h"

namespace org::limitless::seqeron::replayer::client {

/**
 * One bounded read of the node's recorded queue through its Replayer (schema 212, spec §9.2): it resolves a
 * queue, asks for a replay from a recorded position, and hands each frame of that replay to a handler until it
 * reaches the position the Replayer names, which is where the queue was when it answered. Destroying the scan
 * releases the Replayer's slot (R-3).
 *
 * It is the reader for what needs history at a position of its own, such as serving resends, beside the hosted
 * stubs, whose host follows the queue for them. It restores no snapshot and follows no live queue.
 * The protocol changes in place without a version bump, so a scan must be built from the seqeron it runs beside.
 *
 * Single-threaded: every method belongs to the caller's duty-cycle thread.
 */
class ReplayScan
{
  public:
    enum class State
    {
        ResolvingQueue,
        Requesting,
        Reading,
        Done,
        Failed
    };

    using OnFrame = std::function<void(const protocol::FrameView&, std::int64_t position)>;

    /** fromPosition value asking for the active recording from its start. */
    static constexpr std::int64_t FROM_START = protocol::REPLAYER_FROM_START;

    /**
     * @param aeron        the co-located media driver's client
     * @param gatewayName  the name of the gateway instance whose queue to read, as its topology row gives it; empty
     *                     for the default queue (R-6)
     * @param fromPosition a frame's position() on this node's recording, or FROM_START
     * @param onFrame      called for each frame of the replay with its position() on the recording, which a later
     *                     scan resumes from; the view is valid for the call only
     */
    ReplayScan(std::shared_ptr<aeron::Aeron> aeron, const std::string& gatewayName, const std::int64_t fromPosition,
               OnFrame onFrame) :
      m_aeron{ std::move(aeron) },
      m_clientId{ m_aeron->nextCorrelationId() },
      m_fromPosition{ fromPosition },
      m_onFrame{ std::move(onFrame) },
      m_controlAssembler{ [this](auto& buffer, auto offset, auto length, auto&) {
          onControl(frameAt(buffer, offset), static_cast<std::uint64_t>(length));
      } },
      m_replayAssembler{ [this](auto& buffer, auto offset, auto length, auto& header) {
          onReplayFrame(frameAt(buffer, offset), static_cast<std::uint64_t>(length),
                        protocol::frameStartPosition(header));
      } }
    {
        std::strncpy(m_gatewayName.data(), gatewayName.c_str(), m_gatewayName.size());
        m_requestPubId = m_aeron->addPublication(IPC_CHANNEL, protocol::REPLAYER_REQUEST_STREAM_ID);
        m_controlSubId =
            m_aeron->addSubscription(std::string(IPC_CHANNEL) + "|tether=false", protocol::REPLAYER_CONTROL_STREAM_ID);
    }

    ReplayScan(const ReplayScan&) = delete;
    ReplayScan& operator=(const ReplayScan&) = delete;

    [[nodiscard]] State state() const noexcept
    {
        return m_state;
    }

    [[nodiscard]] bool finished() const noexcept
    {
        return m_state == State::Done || m_state == State::Failed;
    }

    /** The replay's read position, which advances while the scan makes progress; 0 before the replay starts. */
    [[nodiscard]] std::int64_t position() const noexcept
    {
        return m_position;
    }

    /** One duty-cycle iteration: the control stream, the request's resend timer and the replay. */
    int poll()
    {
        if (finished())
        {
            return 0;
        }
        resolveResources();
        if (!m_controlSub || !m_requestPub)
        {
            return 0;
        }

        int work = m_controlSub->poll(m_controlAssembler.handler(), FRAGMENT_LIMIT);
        if (m_state == State::ResolvingQueue || m_state == State::Requesting)
        {
            resendRequest();
        }
        if (m_state == State::Reading)
        {
            work += readReplay();
        }
        return work;
    }

  private:
    static constexpr int FRAGMENT_LIMIT = 16;
    static constexpr std::size_t REQUEST_BUFFER_LENGTH = 96;
    static constexpr std::chrono::milliseconds RESEND_INTERVAL{ 100 };

    static constexpr const char* IPC_CHANNEL = protocol::REPLAYER_IPC_CHANNEL;

    void resolveResources()
    {
        if (!m_controlSub)
        {
            m_controlSub = m_aeron->findSubscription(m_controlSubId);
        }
        if (!m_requestPub)
        {
            m_requestPub = m_aeron->findPublication(m_requestPubId);
        }
        if (!m_replaySub && m_replaySubId >= 0)
        {
            m_replaySub = m_aeron->findSubscription(m_replaySubId);
        }
    }

    // A request is resent until answered, with the requestId unchanged (R-1); the offer's result is discarded
    // because the timer is the retry.
    void resendRequest()
    {
        const auto now = std::chrono::steady_clock::now();
        if (m_requestSent && now - m_lastRequest < RESEND_INTERVAL)
        {
            return;
        }
        if (!m_requestPub->isConnected())
        {
            return;
        }
        std::array<std::uint8_t, REQUEST_BUFFER_LENGTH> buffer{};
        char* const bytes = reinterpret_cast<char*>(buffer.data());
        std::size_t length = 0;
        if (m_state == State::ResolvingQueue)
        {
            sbe::replay::QueueRequest request;
            request.wrapAndApplyHeader(bytes, 0, buffer.size())
                .clientId(m_clientId)
                .requestId(m_requestId)
                .sourceId(protocol::REPLAYER_NO_SOURCE_ID)
                .putGatewayName(m_gatewayName.data());
            length = sbe::replay::MessageHeader::encodedLength() + request.encodedLength();
        }
        else
        {
            sbe::replay::ReplayRequest request;
            request.wrapAndApplyHeader(bytes, 0, buffer.size())
                .clientId(m_clientId)
                .requestId(m_requestId)
                .fromPosition(m_fromPosition)
                .queueId(m_queueId)
                .cutClusterSeqNo(protocol::REPLAYER_NO_CUT);
            length = sbe::replay::MessageHeader::encodedLength() + request.encodedLength();
        }
        aeron::concurrent::AtomicBuffer atomic(buffer.data(), buffer.size());
        m_requestPub->offer(atomic, 0, static_cast<aeron::util::index_t>(length));
        m_requestSent = true;
        m_lastRequest = now;
    }

    void onControl(char* const frame, const std::uint64_t length)
    {
        if (length < sbe::replay::MessageHeader::encodedLength())
        {
            return;
        }
        sbe::replay::MessageHeader header;
        header.wrap(frame, 0, 0, length);
        const std::uint64_t body = sbe::replay::MessageHeader::encodedLength();
        switch (header.templateId())
        {
            case sbe::replay::QueueResolved::sbeTemplateId(): {
                sbe::replay::QueueResolved resolved;
                resolved.wrapForDecode(frame, body, header.blockLength(), header.version(), length);
                if (isCurrent(resolved.clientId(), resolved.requestId()) && m_state == State::ResolvingQueue)
                {
                    m_queueId = resolved.queueId();
                    m_state = State::Requesting;
                    nextRequest();
                }
                return;
            }
            case sbe::replay::Replaying::sbeTemplateId(): {
                sbe::replay::Replaying replaying;
                replaying.wrapForDecode(frame, body, header.blockLength(), header.version(), length);
                if (isCurrent(replaying.clientId(), replaying.requestId()) && m_state == State::Requesting)
                {
                    onReplaying(replaying.replaySessionId(), replaying.catchUpPosition());
                }
                return;
            }
            default:
                // ReplayPending is the resend timer's cue to wait, and the rest are other clients' or the
                // snapshot rounds', none of them ours.
                return;
        }
    }

    [[nodiscard]] bool isCurrent(const std::int64_t clientId, const std::int64_t requestId) const noexcept
    {
        return clientId == m_clientId && requestId == m_requestId;
    }

    // A new request supersedes the earlier one, so its requestId changes even when its content does not (R-1).
    void nextRequest()
    {
        ++m_requestId;
        m_requestSent = false;
    }

    void onReplaying(const std::int64_t sessionId, const std::int64_t catchUpPosition)
    {
        if (sessionId == protocol::REPLAYER_NO_REPLAY_NEEDED)
        {
            // From the start it means an empty recording. From a position the Replayer refuses it, which is
            // fatal (R-2): the position is before the recording's start or not a frame boundary in it.
            m_state = m_fromPosition == FROM_START ? State::Done : State::Failed;
            return;
        }
        m_sessionId = static_cast<std::int32_t>(sessionId);
        m_catchUpPosition = catchUpPosition;
        m_replaySubId =
            m_aeron->addSubscription(std::string(IPC_CHANNEL) + "|session-id=" + std::to_string(m_sessionId),
                                     protocol::REPLAYER_REPLAY_STREAM_ID);
        m_state = State::Reading;
    }

    int readReplay()
    {
        if (!m_replaySub)
        {
            return 0;
        }
        if (!m_replayImage)
        {
            m_replayImage = m_replaySub->imageBySessionId(m_sessionId);
            if (!m_replayImage)
            {
                return 0;
            }
        }
        const std::shared_ptr<aeron::Image> image = m_replayImage;
        if (image->isClosed())
        {
            finish(image->position() >= m_catchUpPosition ? State::Done : State::Failed);
            return 0;
        }
        const int work = image->poll(m_replayAssembler.handler(), FRAGMENT_LIMIT);
        if (m_state == State::Reading)
        {
            m_position = image->position();
            if (m_position >= m_catchUpPosition)
            {
                finish(State::Done);
            }
        }
        return work;
    }

    // The first frame of a resumed replay must be the one asked for (R-2); any other is fatal.
    void onReplayFrame(char* const frame, const std::uint64_t length, const std::int64_t position)
    {
        if (m_state != State::Reading)
        {
            return;
        }
        if (!m_firstFrameSeen)
        {
            m_firstFrameSeen = true;
            if (m_fromPosition != FROM_START && position != m_fromPosition)
            {
                finish(State::Failed);
                return;
            }
        }
        const protocol::FrameView view = protocol::unwrapFrame(frame, length);
        if (view.valid)
        {
            m_onFrame(view, position);
        }
    }

    // Releases the replay subscription, and with it the Replayer's slot.
    void finish(const State state)
    {
        m_state = state;
        m_replayImage.reset();
        m_replaySub.reset();
    }

    static char* frameAt(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset)
    {
        return reinterpret_cast<char*>(buffer.buffer()) + offset;
    }

    std::shared_ptr<aeron::Aeron> m_aeron;
    const std::int64_t m_clientId;
    const std::int64_t m_fromPosition;
    OnFrame m_onFrame;
    std::array<char, 32> m_gatewayName{};

    State m_state{ State::ResolvingQueue };
    std::int64_t m_requestId{ 1 };
    bool m_requestSent{ false };
    std::chrono::steady_clock::time_point m_lastRequest{};
    std::int32_t m_queueId{ 0 };
    std::int32_t m_sessionId{ 0 };
    std::int64_t m_catchUpPosition{ 0 };
    std::int64_t m_position{ 0 };
    bool m_firstFrameSeen{ false };

    std::int64_t m_requestPubId{ -1 };
    std::int64_t m_controlSubId{ -1 };
    std::int64_t m_replaySubId{ -1 };
    std::shared_ptr<aeron::Publication> m_requestPub;
    std::shared_ptr<aeron::Subscription> m_controlSub;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Image> m_replayImage;

    aeron::FragmentAssembler m_controlAssembler;
    aeron::FragmentAssembler m_replayAssembler;
};

} // namespace org::limitless::seqeron::replayer::client
