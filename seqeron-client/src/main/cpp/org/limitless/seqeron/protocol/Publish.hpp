#pragma once

// What a publish did. `Refused` is local and permanent (T-3): the body is above MAX_PAYLOAD_LENGTH, or the
// frame breaks §9.2 conditions 6 to 9, and nothing was offered, so retrying cannot succeed. `Declined` —
// transport back-pressure, a lost session, or the tracker holding or full — is the one a caller may retry.
//
// Here rather than in sequencer::client because it is what the app façades return, and a consumer that takes
// one should not have to reach into the layer below it. The Java twin is org.limitless.seqeron.protocol.Publish.

namespace org::limitless::seqeron::protocol {

enum class Publish
{
    Published,
    Refused,
    Declined
};

} // namespace org::limitless::seqeron::protocol
