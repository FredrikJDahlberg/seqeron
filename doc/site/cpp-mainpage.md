# seqeron C++ client {#mainpage}

The client tier of seqeron: header-only, linked as `seqeron::seqeron_core`. It speaks the same wire protocol
as the Java client, `%org.limitless:seqeron`, and its namespaces mirror that jar's packages.

## Start here

| To | Take |
|---|---|
| Run one instance of an elected active/standby gateway pair | \ref org::limitless::seqeron::app::Gateway "app::Gateway" |
| Run a co-located application: one replica per node, publishing while its node leads | \ref org::limitless::seqeron::app::Application "app::Application" |
| Follow the ordered stream: history off the recording, then the live tap | \ref org::limitless::seqeron::replayer::client::ReplayerStreamReceiver "replayer::client::ReplayerStreamReceiver" |

A façade assembles the whole duty cycle. Its listener sees \ref org::limitless::seqeron::app::Payload "Payload"
and \ref org::limitless::seqeron::app::ClusterError "ClusterError", and a publish returns
\ref org::limitless::seqeron::protocol::Publish "Publish".

## Namespaces

All under `org::limitless::seqeron`.

| Namespace | Holds |
|---|---|
| \ref org::limitless::seqeron::app "app" | The front door: the two façades above and the blocks they are assembled from |
| \ref org::limitless::seqeron::sequencer::client "sequencer::client" | Producing: the cluster session (\ref org::limitless::seqeron::sequencer::client::ClusterStreamSender "ClusterStreamSender"), the encode-and-offer (\ref IngressPublisher.hpp "IngressPublisher.hpp") and confirmed ingress (\ref org::limitless::seqeron::sequencer::client::PendingSends "PendingSends") |
| \ref org::limitless::seqeron::replayer::client "replayer::client" | Consuming: \ref org::limitless::seqeron::replayer::client::ReplayerStreamReceiver "ReplayerStreamReceiver" |
| \ref org::limitless::seqeron::protocol "protocol" | The wire contract: the frame envelope and `SequencedEvent`, the port layout, the replay protocol's addresses, the counter ids and `Publish` |
| \ref org::limitless::seqeron::util "util" | Support code: infrastructure rather than API |

The `detail` namespaces and the generated SBE codecs are not API and are left out.

## Getting it

```cmake
include(FetchContent)
FetchContent_Declare(seqeron
    GIT_REPOSITORY https://github.com/FredrikJDahlberg/seqeron.git
    GIT_TAG        v<version>)
FetchContent_MakeAvailable(seqeron)
target_link_libraries(my_app PRIVATE seqeron::seqeron_core)
```

Or `find_package(seqeron)` against an installed prefix, which takes your own installed Aeron.

## Read next

- [Client API](https://github.com/FredrikJDahlberg/seqeron/blob/main/doc/client-api.md): what a client
  programs against, in both languages, and what is not API
- [Protocol specification](https://github.com/FredrikJDahlberg/seqeron/blob/main/doc/seqeron-protocol-spec.md):
  the frames, the two families and the system vocabulary
- [Examples](https://github.com/FredrikJDahlberg/seqeron/tree/main/seqeron-examples): `FollowStream`, the
  smallest complete client, and the two façades in use
