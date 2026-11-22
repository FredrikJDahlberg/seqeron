#pragma once

// What a publish did. `Refused` is local and permanent (T-3): the payload is above MAX_PAYLOAD_LENGTH, or the
// frame breaks §9.2 conditions 6 to 9, and nothing was offered, so retrying cannot succeed. `Declined` is the one a
// caller may retry: the gate shut, ingress held or the tracker full, the session lost or being replaced, or a hosted
// stub's link to its host full. Cluster ingress back-pressure is not a decline: a publish spins through it, for up to
// 10 s.
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
