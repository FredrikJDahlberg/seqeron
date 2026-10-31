#pragma once

// What a ClientProxy and its client share (doc/hosted-facade-draft.md §17.2, §17.7). The Java twin is
// org.limitless.seqeron.protocol.ProxyProtocol, which also holds the channels; the stub builds its own.

#include <cstdint>

namespace org::limitless::seqeron::protocol {

// The hostName a client names in its Attach.
inline constexpr const char* PROXY_HOST_NAME = "hosted";
// Attach.protocolVersion.
inline constexpr std::uint16_t PROXY_PROTOCOL_VERSION = 1;
// The schema id of the MessageHeader on all four streams.
inline constexpr std::uint16_t PROXY_SCHEMA_ID = 218;

inline constexpr std::int32_t PROXY_ATTACH_REQUEST_STREAM = 301;
inline constexpr std::int32_t PROXY_ATTACH_REPLY_STREAM = 302;
inline constexpr std::int32_t PROXY_EVENTS_STREAM = 303;
inline constexpr std::int32_t PROXY_COMMANDS_STREAM = 304;

} // namespace org::limitless::seqeron::protocol
