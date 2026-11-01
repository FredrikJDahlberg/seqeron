# seqeron C++ client {#mainpage}

The C++ client of seqeron: header-only, linked as `seqeron::seqeron_core`. A C++ client is a thin stub that talks over
IPC to a host process, the Java `ClientProxy`, which runs the cluster session, the queue and the fences
(`doc/hosted-facade-draft.md`). The direct C++ client is retired.

## Start here

| To | Take |
|---|---|
| Run one instance of an elected active/standby gateway pair | \ref org::limitless::seqeron::hosted::Gateway "hosted::Gateway" |
| Run a co-located application: one replica per node, publishing while its node leads | \ref org::limitless::seqeron::hosted::Application "hosted::Application" |
| Follow the ordered stream, publishing nothing | \ref org::limitless::seqeron::hosted::Consumer "hosted::Consumer" |

A stub's listener sees \ref org::limitless::seqeron::app::Payload "Payload" and
\ref org::limitless::seqeron::app::ClusterError "ClusterError", and a publish returns
\ref org::limitless::seqeron::protocol::Publish "Publish". The listener concepts are in `app/Listeners.hpp`.

## Namespaces

All under `org::limitless::seqeron`.

| Namespace | Holds |
|---|---|
| \ref org::limitless::seqeron::hosted "hosted" | The stubs above |
| \ref org::limitless::seqeron::app "app" | The types the stubs share with the Java client tier: `Payload`, `ClusterError`, `GroupDirectory`, `OutstandingWork`, `SnapshotListener` and the listener concepts |
| \ref org::limitless::seqeron::protocol "protocol" | The wire contract: the frame envelope and `SequencedEvent`, the counter ids, the host protocol's ids and `Publish` |
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
