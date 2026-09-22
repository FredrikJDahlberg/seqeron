#pragma once

// The seam ClusterStreamSender talks to the cluster through, and its Aeron implementations. Not API: a test
// drives the sender with in-memory fakes through these.

#include <cstdint>
#include <functional>
#include <memory>
#include <span>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"

namespace org::limitless::seqeron::sequencer::client::detail {

// ── Transport interfaces ──────────────────────────────────────────────────────

// Outbound half: offers raw bytes to the cluster ingress; `false` means not yet accepted.
class IngressTransport
{
  public:
    virtual ~IngressTransport() = default;
    virtual bool offer(std::span<const std::uint8_t> bytes) = 0;
};

// Inbound half: polls whole (reassembled) egress messages; returns fragments processed.
class EgressTransport
{
  public:
    using FragmentHandler = std::function<void(std::span<const std::uint8_t>)>;

    virtual ~EgressTransport() = default;
    virtual int poll(const FragmentHandler& handler) = 0;
};

// ── Real Aeron-backed transports ──────────────────────────────────────────────

class AeronIngressTransport : public IngressTransport
{
  public:
    explicit AeronIngressTransport(std::shared_ptr<aeron::Publication> pub) : m_pub(std::move(pub))
    {}

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        const aeron::concurrent::AtomicBuffer buffer(const_cast<std::uint8_t*>(bytes.data()),
                                                     static_cast<aeron::util::index_t>(bytes.size()));
        return m_pub->offer(buffer, 0, static_cast<aeron::util::index_t>(bytes.size())) >= 0;
    }

  private:
    std::shared_ptr<aeron::Publication> m_pub;
};

class AeronEgressTransport : public EgressTransport
{
  public:
    explicit AeronEgressTransport(std::shared_ptr<aeron::Subscription> sub) : m_sub(std::move(sub))
    {}

    int poll(const FragmentHandler& handler) override
    {
        m_handler = &handler;
        const int n = m_sub->poll(m_poll, 10);
        m_handler = nullptr;
        return n;
    }

  private:
    std::shared_ptr<aeron::Subscription> m_sub;

    // Per-call callback set in poll(); null outside of that call.
    const FragmentHandler* m_handler{ nullptr };

    // Persistent across poll() calls so multi-fragment messages reassemble correctly.
    aeron::FragmentAssembler m_fa{ [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                                          aeron::util::index_t len, aeron::Header&) {
        if (m_handler)
        {
            (*m_handler)(std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(buf.buffer()) + off,
                                                       static_cast<std::size_t>(len)));
        }
    } };

    // Composed once rather than per poll(): FragmentAssembler::handler() returns a fresh std::function.
    // Declared after m_fa, which it is built from.
    aeron::fragment_handler_t m_poll{ m_fa.handler() };
};

} // namespace org::limitless::seqeron::sequencer::client::detail
