# seqeron C# client {#mainpage}

The client tier of seqeron: the `Org.Limitless.Seqeron` package, on Aeron.NET. It speaks the same wire
protocol as the Java client, `%org.limitless:seqeron`, and its namespaces are that jar's packages in
PascalCase.

## Start here

| To | Take |
|---|---|
| Run one instance of an elected active/standby gateway pair | \ref Org.Limitless.Seqeron.App.Gateway "App.Gateway" |
| Run a co-located application: one replica per node, publishing while its node leads | \ref Org.Limitless.Seqeron.App.Application "App.Application" |
| Follow the ordered stream: history off the recording, then the live tap | \ref Org.Limitless.Seqeron.Replayer.Client.ReplayerStreamReceiver "Replayer.Client.ReplayerStreamReceiver" |

A façade takes an options class (\ref Org.Limitless.Seqeron.App.GatewayOptions "GatewayOptions",
\ref Org.Limitless.Seqeron.App.ApplicationOptions "ApplicationOptions") and assembles the whole duty cycle. Its
listener sees \ref Org.Limitless.Seqeron.App.Payload "Payload" and
\ref Org.Limitless.Seqeron.App.ClusterError "ClusterError", and a publish returns
\ref Org.Limitless.Seqeron.Protocol.Publish "Publish".

## Namespaces

All under `Org.Limitless.Seqeron`.

| Namespace | Holds |
|---|---|
| \ref Org.Limitless.Seqeron.App "App" | The front door: the two façades above and the blocks they are assembled from |
| \ref Org.Limitless.Seqeron.Sequencer.Client "Sequencer.Client" | Producing: the cluster session (\ref Org.Limitless.Seqeron.Sequencer.Client.ClusterStreamSender "ClusterStreamSender"), the encode-and-offer (\ref Org.Limitless.Seqeron.Sequencer.Client.IngressPublisher "IngressPublisher") and confirmed ingress (\ref Org.Limitless.Seqeron.Sequencer.Client.PendingSends "PendingSends") |
| \ref Org.Limitless.Seqeron.Replayer.Client "Replayer.Client" | Consuming: \ref Org.Limitless.Seqeron.Replayer.Client.ReplayerStreamReceiver "ReplayerStreamReceiver" |
| \ref Org.Limitless.Seqeron.Protocol "Protocol" | The wire contract: the frame envelope and `SequencedEvent`, the port layout, the replay protocol's addresses, the counter ids and `Publish` |
| \ref Org.Limitless.Seqeron.Util "Util" | Support code: infrastructure rather than API |

`internal` types and the generated SBE codecs are not API and are left out.

## Getting it

```
dotnet add package Org.Limitless.Seqeron
```

The package carries its XML docs, so an IDE shows these pages, and a symbols package beside it.

## Read next

- [Client API](https://github.com/FredrikJDahlberg/seqeron/blob/main/doc/client-api.md): what a client
  programs against, in each language, and what is not API
- [Protocol specification](https://github.com/FredrikJDahlberg/seqeron/blob/main/doc/seqeron-protocol-spec.md):
  the frames, the two families and the system vocabulary
- [Examples](https://github.com/FredrikJDahlberg/seqeron/tree/main/seqeron-examples): `FollowStream`, the
  smallest complete client, and the two façades in use
