#pragma once

#include <cstdint>

#include "org/limitless/seqeron/app/Consumer.hpp"
#include "org/limitless/seqeron/hosted/detail/Attachment.hpp"

namespace org::limitless::seqeron::hosted {

/**
 * The client stub of a hosted Consumer (doc/hosted-facade-draft.md §17.1): the surface of app::Consumer, with the
 * queue, the replay and the fences running in a host process that this talks to over schema 218. Its listener is the
 * direct Consumer's, and a Payload reaches it as it would there.
 *
 * It sends only Attach and Detach: no stream 304, no Gate, no commands, and the host opens no cluster session for it.
 * Fenced, the host sends Fenced and exits (H-9); the listener's onFenced runs once.
 *
 * Single-threaded: every method belongs to the caller's one duty-cycle thread.
 */
template<typename Listener>
class Consumer : public detail::Attachment<Consumer<Listener>, Listener>
{
    using Base = detail::Attachment<Consumer<Listener>, Listener>;
    friend Base;

  public:
    struct Config
    {
        // The host's heartbeat counter may be silent this long before the stub gives it up (H-9, section 17.6).
        std::int64_t hostTimeoutMs = 3000;
        std::int64_t attachTimeoutMs = 90'000;
        // Names the host this attaches to, the one its proxy.hostName gives; the attach is refused otherwise.
        std::string hostName = protocol::PROXY_DEFAULT_HOST_NAME;
    };

    Consumer(Config config, Listener& listener) :
      Base{ listener, wire::Kind::CONSUMER, -1, nullptr, config.hostTimeoutMs, config.attachTimeoutMs, config.hostName,
            false }
    {
        static_assert(app::ConsumerListener<Listener>);
    }

  private:
    // The hooks of Attachment: a consumer is sent neither.
    void onGate(bool)
    {}

    void onOther(std::uint16_t, char*, aeron::util::index_t, std::uint16_t, std::uint16_t)
    {}
};

} // namespace org::limitless::seqeron::hosted
