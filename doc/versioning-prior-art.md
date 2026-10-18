# Protocol versioning: prior art

How financial and log-based systems version their protocols, as background to
[`upgrades.md`](upgrades.md). Nothing here is seqeron's design; §7 says what bears on it. The external facts
are as published in October 2026, and those specifications change; the sources are listed at the end.

The common thread: **who reads a stream decides how it can be versioned.** A one-to-one session can
negotiate a version per client. A one-to-many stream cannot, and every exchange versions its market data
differently from its order entry for that reason.

## 1. Where the version is carried

| carried in | examples | consequence |
| --- | --- | --- |
| the message name | ISO 20022 `pacs.008.001.08`: business area, message, variant, version | each version is a distinct document type; old and new coexist only through translation or bilateral agreement |
| every message header | SBE `schemaId` and `version` (CME, Euronext, Binance, B3) | messages describe themselves; a reader that skips by `blockLength` reads newer ones |
| the session | FIXT.1.1 `DefaultApplVerID` at logon, overridden per message type and per message; Eurex ETI's backward compatibility mode; Binance's per-request header | the server encodes for each client, which needs a one-to-one session |
| the port | Nasdaq OUCH 5.0 needs new ports; a 4.2 session is not upgraded in place | a breaking change is an infrastructure change |
| a separate document | FIX Orchestra (machine-readable rules of engagement), Binance's lifecycle JSON, SBE XML on exchange file servers | tools can consume them; only Binance's states when a version stops being accepted |

## 2. Compatibility models

1. **Additive change in place.** SBE as Euronext Optiq codifies it (§3): fields appended to a block or a
   group, always optional, enum values never deleted. Readers on an older version read newer messages.
2. **The server sends its latest; the client tolerates it.** CME iLink 3 accepts the prior schema version
   inbound and sends only the latest. Euronext sends the current template version whatever the client
   uses. CME's 2024 notice of the MDP 3.0 core schema moving from version 9 to 13 warns that clients not
   built for template extension will fail to decode.
3. **Parallel layouts.** Eurex T7 serves the previous release's ETI and FIX layouts in a backward
   compatibility mode, without the new release's functions. Market data, reference data and reports get no
   such mode, and some releases (13.0) offer none at all.
4. **Downgrade with a sentinel.** Binance encodes for the version a client requests and maps what that
   version cannot express to `NonRepresentable` (§4).
5. **One date for everyone.** SWIFT's MT standards release goes live each November for all users at once,
   announced 12 to 16 months ahead.
6. **Coexistence through translation.** SWIFT MT and ISO 20022 ran side by side for cross-border payments
   from March 2023 to November 2025.
7. **A new connection.** Nasdaq OUCH 4.2 to 5.0, which also moved from fixed-length messages to optional
   tagged appendages so that later fields do not change the layout.

## 3. Euronext Optiq SBE rules

The most complete published rule set for SBE evolution (Euronext technical notice, 2018).

**Versions.** `version` rises with every schema change, not necessarily by one, and order entry and market
data move together. `schemaId` is 0 and rises only when compatibility breaks. `semanticVersion` names the
functional specification and is not for clients.

**Writer rules.**

- New fields go at the end of the root block or of a repeating group, and are always optional; a field the
  specification calls mandatory is enforced by the matching engine, not the schema.
- Enum and bitset values are never deleted. A decommissioned value is deprecated.
- Encoding types do not change.

**Reader obligations.**

- Move from the header to the first group by the header's `blockLength`, and between groups by
  `numInGroup` × the group's `blockLength`, never by compiled offsets.
- Ignore unknown enum and bitset values, or discard the message.
- Accept a null in a deprecated field.
- Identify fields by name: field and group ids are sequential per message and shift between versions.

**Deprecation.**

| deprecated | inbound, if the client still sends it | outbound |
| --- | --- | --- |
| value | processed or rejected, by business rule | no longer sent |
| field | processed or rejected, by business rule | sent as null |
| message | rejected | no longer sent |

**Lifecycle.** Several versions are accepted in parallel above a published minimum; raising the minimum is
announced at least 3 months ahead, and inbound messages below it are rejected. A breaking change raises
`schemaId`, is announced at least 4 months ahead, removes the deprecated fields and requires conformance
testing. Templates reach production at least 2 days before go-live.

## 4. Binance spot SBE

**Versions.** `schemaId` rises on a breaking change and resets `version` to 0; `version` rises on any other.

**Lifecycle.** Every release deprecates its predecessor, breaking or not, and a deprecated version is
supported for at least 6 months before retirement. A request for a retired version gets HTTP 400, or a
reject on FIX. Deprecation is signalled in every response: a REST header, a WebSocket field, a FIX
`LogonAck` field. A lifecycle JSON per environment lists the latest, deprecated and retired versions with
their dates.

**Encoding.** A request for a deprecated version is answered in the highest version of the same
`schemaId`: `3:0` requests receive `3:1` responses. Every enum carries `NonRepresentable` (254 for a
`uint8`, whose null value is 255; `~` for a `char`), and `NonRepresentableMessage` stands in for a message
the requested version lacks. The documentation does not say whether values are masked by the requested
version or the encoded one; only the former protects a reader compiled against the older schema.

**Observed in the published schemas:**

- Minor versions add enum values: 3:1 added four `filterType` values, 3:2 `selfTradePreventionMode`
  `Transfer` (`sinceVersion="2"`).
- A minor version removed a message: `TPlusFilterLockEvent`, added in 3:2 and gone in 3:3. Decoders do not
  break; a client that relied on the event stops receiving it.
- Cadence: id 2 followed id 1 after 7 weeks; id 3 shipped 2025-04-24 and reached 3:5 on 2026-07-07. Ids 2
  and 3 were both live from 2025-04-24 to 2025-10-24.

## 5. Common practice

- **Inbound strict, outbound current.** Exchanges reject old or deprecated input below a minimum version
  and send their current version, relying on readers to tolerate it.
- **One-to-many streams are not negotiated.** Market data is one version on the wire everywhere surveyed;
  a breaking change is a new `schemaId` or channel with notice and testing. Negotiation exists only on
  order entry.
- **Breaking changes are a major version reserved for them**, with 3 to 16 months' notice and, at Euronext
  and CME, mandatory certification.
- **The usual failure is the reader.** CME's warning and Euronext's reader obligations exist because
  client decoders use compiled block lengths, fixed offsets, or throw on unknown enum values. SBE's
  generated C++ does the last by default: an enum's `get()` throws `unknown value for enum … [E103]`.
- **Version independence underdelivers.** FIXT.1.1 was meant to carry several application versions in one
  session; the FIX working group's own forum notes the `ApplVerID` enumeration did not keep pace with
  service packs.

## 6. Log-based systems

- **Kafka Schema Registry** names the upgrade order. `BACKWARD`, consumers first, is the default "so that
  you can rewind consumers to the beginning of the topic"; a consumer that reads every registered version
  needs `BACKWARD_TRANSITIVE`. The registry checks a schema against the rule when it is registered.
- **Event sourcing** (Young, *Versioning in an Event Sourced System*): versioning by event type works only
  with the system stopped; a weak schema (no renames, no change of meaning), upcasting on read, and for
  what neither handles, copying to a new stream and switching consumers to it.
- **LMAX** kept about a day of messages to replay, which made replay-based upgrades workable; Young notes it
  does not work with years of history.
- **Aeron Cluster** checks the application version when it replays the log (1.39.0) and versions its
  consensus protocol (1.40.0).

## 7. Bearing on seqeron

- **The queue is a one-to-many stream.** One encoding serves every consumer, live and replayed, so §5's
  conclusion applies: one version on the wire, readers that tolerate extension, a breaking change as a new
  identity. [`upgrades.md`](upgrades.md) §7.1 already makes v2 a new `payloadId`. Per-consumer encoding and
  Binance's masking have no place to run (`upgrades.md` §5.1).
- **Reader obligations, in part.** `upgrades.md` §4.1 lists the changes SBE allows but not how a reader
  moves through a newer message (§3), and the conformance suite (spec §14) has no case decoding a payload
  one version newer than the reader's codec. An older reader ignores appended fields, unknown
  templates and unknown values, as Euronext's do; a value it could not safely ignore would change the
  field's meaning, which `upgrades.md` §4.1 already rules out. SBE's generated C++ throws on an unknown enum
  value, so such a reader takes the raw value. What seqeron adds is that every instance of one source treats
  a message alike: one that acts on a new field while another of its source ignores it diverges, and A-7
  fences one of them.
- **The compatibility class is `BACKWARD_TRANSITIVE`**, consumers first (`upgrades.md` §5.3), back to
  `clusterSeqNo` 1 today and back to the floor `F` under cluster snapshots ([`snapshot.md`](snapshot.md)
  §10.6). A check of a schema's IR against every version since `F` would enforce it as a registry does.
- **Retirement follows `F`, not a calendar.** A payload version can be retired once `F` passes its last
  frame, about 256 rounds after its cutover marker. `LOG_VERSION` (`snapshot.md` §10.2) is the `schemaId` of
  seqeron's own log.

## Sources

- FIX: [FIXT.1.1 session protocol](https://onixs.biz/fix-dictionary/FIXT1.1/section_session_protocol.html),
  [ApplVerID and extension packs](https://forum.fixtrading.org/t/applverid-and-extension-packs/5951),
  [requiring ApplVerID on all FIXT messages](https://forum.fixtrading.org/t/require-applverid-on-all-fixt-messages/5022),
  [Orchestra technical specification](https://www.fixtrading.org/?p=57539)
- Euronext: [SBE template introduction and compatibility rules](https://eua-connect.euronext.com/sites/default/files/it-documentation/sbe_template_introduction_and_compatibility_rules.pdf)
- CME: [notice, August 2024](https://www.cmegroup.com/notices/electronic-trading/2024/08/20240826.html),
  [notice, January 2021](https://www.cmegroup.com/en/notices/electronic-trading/2021/01/20210104.html),
  [MDP 3.0 message schema](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457225459),
  [iLink 3 SBE](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/46439949)
- Eurex: [T7 14.1 release notes](https://www.eurex.com/resource/blob/5071042/e6ba27751ee0671a6572538dcc1387d3/data/T7_Release_14.1_-_Release_Notes_Eurex_Version_3.pdf),
  [circular 5098798](https://www.eurex.com/ex-en/find/circulars/circular-5098798),
  [ETI backward compatibility mode](https://www.eurex.com/ex-en/find/circulars/Update-of-the-T7-Release-9.0-Release-Notes-containing-important-information-regarding-the-ETI-Backward-Compatibility-Mode-2307506)
- Nasdaq: [OUCH 5.0 FAQ](https://m.nasdaqtrader.com/content/productsservices/trading/OUCH_5.0_FAQ.pdf),
  [OUCH 5 overview](https://nasdaq.com/docs/ouch-5-overview)
- Binance: [SBE FAQ](https://developers.binance.com/legacy-docs/binance-spot-api-docs/faqs/sbe_faq),
  [schemas and lifecycle JSON](https://github.com/binance/binance-spot-api-docs/tree/master/sbe/schemas)
- ISO 20022 and SWIFT: [BNY, Demystifying ISO 20022](https://www.bnymellon.com/content/dam/bnymellon/documents/pdf/iso-20022/Module%201_September%202020_Demystifying%20ISO20022.pdf),
  [MT and ISO 20022 coexistence](https://www.theglobaltreasurer.com/2024/08/20/iso20022-co-existence-will-come-to-an-end-next-year-are-you-ready/),
  [SWIFT standards release](https://www.tangentia.com/?p=5823)
- Logs: [Confluent, schema evolution and compatibility](https://docs.confluent.io/platform/7.2/schema-registry/avro.html),
  [Young, Versioning in an Event Sourced System](https://leanpub.com/esversioning/read),
  [LMAX replay window](https://groups.google.com/g/dddcqrs/c/vNAwkmW055g),
  [Aeron 1.39.0](https://newreleases.io/project/github/aeron-io/aeron/release/1.39.0),
  [Aeron 1.40.0](https://newreleases.io/project/github/aeron-io/aeron/release/1.40.0)
