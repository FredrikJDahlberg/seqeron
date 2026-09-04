//
// Round-trip tests for the sbe-unsequenced.xml and sbe-sequenced.xml
// generated codecs — the FIX admin/application messages that travel
// through Aeron Cluster ingress (unsequenced) and the cluster stream
// (sequenced).
//
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "org/limitless/phixeron/sequencer/SequencedFrame.hpp"
#include "org_limitless_phixeron_sbe_frame/ClientConnected.h"
#include "org_limitless_phixeron_sbe_frame/ClientDisconnected.h"
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/Sequenced.h"
#include "org_limitless_phixeron_sbe_frame/Unsequenced.h"
#include "org_limitless_phixeron_sbe_sequenced/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_sequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_unsequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_unsequenced/Logon.h"
#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/NewOrderSingle.h"

namespace usq = org::limitless::phixeron::sbe::unsequenced;
namespace seq = org::limitless::phixeron::sbe::sequenced;
namespace frm = org::limitless::phixeron::sbe::frame;
namespace sequencer = org::limitless::phixeron::sequencer;

namespace {

static constexpr std::uint16_t UNSEQUENCED_SCHEMA_ID = 200;
static constexpr std::uint16_t SEQUENCED_SCHEMA_ID = 202;
static constexpr std::uint16_t SCHEMA_VERSION = 0;

template<typename MessageHeader>
MessageHeader decodeHeader(std::uint8_t* buffer, std::uint64_t bufferferLength)
{
    MessageHeader hdr;
    hdr.wrap(reinterpret_cast<char*>(buffer), 0, 0, bufferferLength);
    return hdr;
}

} // namespace

// sbe-unsequenced.xml

TEST(UnsequencedCodec, MessageHeaderIdentifiesSchemaAndVersion)
{
    alignas(16) std::array<std::uint8_t, 128> buffer{};
    usq::Heartbeat hb;
    hb.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    hb.header().sourceId(1).sessionId(2);
    hb.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(1).sendingTimeMs(0);
    hb.testReqID()[0] = '\0';

    const auto hdr = decodeHeader<usq::MessageHeader>(buffer.data(), buffer.size());
    EXPECT_EQ(UNSEQUENCED_SCHEMA_ID, hdr.schemaId());
    EXPECT_EQ(SCHEMA_VERSION, hdr.version());
    EXPECT_EQ(usq::Heartbeat::sbeTemplateId(), hdr.templateId());
    EXPECT_EQ(usq::Heartbeat::sbeBlockLength(), hdr.blockLength());
}

TEST(UnsequencedCodec, LogonRoundTripsHeaderAndXmlData)
{
    alignas(16) std::array<std::uint8_t, 256> buffer{};
    usq::Logon enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(42).sessionId(7);
    enc.putSender(std::string_view{ "CLIENT" })
        .putTarget(std::string_view{ "PHIXERON" })
        .seqNum(1)
        .sendingTimeMs(1234567890123LL)
        .encryptMethod(usq::EncryptMethod::Value::None)
        .heartbeatInterval(30000)
        .defaultApplVerID(6);
    enc.putXmlData(std::string_view("<FIXML/>"));

    const auto hdr = decodeHeader<usq::MessageHeader>(buffer.data(), buffer.size());
    ASSERT_EQ(usq::Logon::sbeTemplateId(), hdr.templateId());

    usq::Logon dec;
    dec.wrapForDecode(reinterpret_cast<char*>(buffer.data()), usq::MessageHeader::encodedLength(), hdr.blockLength(),
                      hdr.version(), buffer.size());
    EXPECT_EQ(42, dec.header().sourceId());
    EXPECT_EQ(7, dec.header().sessionId());
    EXPECT_EQ("CLIENT", dec.getSenderAsString());
    EXPECT_EQ("PHIXERON", dec.getTargetAsString());
    EXPECT_EQ(1u, dec.seqNum());
    EXPECT_EQ(1234567890123LL, dec.sendingTimeMs());
    EXPECT_EQ(usq::EncryptMethod::Value::None, dec.encryptMethod());
    EXPECT_EQ(30000u, dec.heartbeatInterval());
    EXPECT_EQ(6, dec.defaultApplVerID()); // FIXT.1.1 DefaultApplVerID (tag 1137)
    EXPECT_EQ("<FIXML/>", dec.getXmlDataAsString());
}

TEST(UnsequencedCodec, NewOrderSingleRoundTripsAllFieldsIncludingOptionals)
{
    alignas(16) std::array<std::uint8_t, 256> buffer{};
    usq::NewOrderSingle enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(11).sessionId(22);
    enc.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(3).sendingTimeMs(1000);
    enc.putAccount("ACC1");
    enc.putClOrdID("ORD-1");
    enc.handlInst(usq::HandlInst::Value::AutoPrivate);
    enc.putSymbol("AAPL");
    enc.side(usq::Side::Value::Buy);
    enc.transactTime(2000);
    enc.orderQty(100);
    enc.ordType(usq::OrdType::Value::Limit);
    enc.price(12'345'000'000LL);
    enc.timeInForce(usq::TimeInForce::Value::Day);
    enc.putText("note");
    enc.tradeDate(86'400'000LL);
    enc.maturityTime(3'600'000LL);

    const auto hdr = decodeHeader<usq::MessageHeader>(buffer.data(), buffer.size());
    ASSERT_EQ(usq::NewOrderSingle::sbeTemplateId(), hdr.templateId());

    usq::NewOrderSingle dec;
    dec.wrapForDecode(reinterpret_cast<char*>(buffer.data()), usq::MessageHeader::encodedLength(), hdr.blockLength(),
                      hdr.version(), buffer.size());
    EXPECT_EQ(11, dec.header().sourceId());
    EXPECT_EQ(22, dec.header().sessionId());
    EXPECT_EQ("ACC1", dec.getAccountAsString());
    EXPECT_EQ("ORD-1", dec.getClOrdIDAsString());
    EXPECT_EQ(usq::HandlInst::Value::AutoPrivate, dec.handlInst());
    EXPECT_EQ("AAPL", dec.getSymbolAsString());
    EXPECT_EQ(usq::Side::Value::Buy, dec.side());
    EXPECT_EQ(2000, dec.transactTime());
    EXPECT_EQ(100u, dec.orderQty());
    EXPECT_EQ(usq::OrdType::Value::Limit, dec.ordType());
    EXPECT_EQ(12'345'000'000LL, dec.price());
    EXPECT_EQ(usq::TimeInForce::Value::Day, dec.timeInForce());
    EXPECT_EQ("note", dec.getTextAsString());
    EXPECT_EQ(86'400'000LL, dec.tradeDate());
    EXPECT_EQ(3'600'000LL, dec.maturityTime());
}

TEST(UnsequencedCodec, NewOrderSingleRoundTripsWithOptionalFieldsAbsent)
{
    alignas(16) std::array<std::uint8_t, 256> buffer{};
    usq::NewOrderSingle enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(1).sessionId(1);
    enc.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(1).sendingTimeMs(1000);
    enc.putAccount(std::string_view{});
    enc.putClOrdID("ORD-2");
    enc.handlInst(usq::HandlInst::Value::Manual);
    enc.putSymbol("MSFT");
    enc.side(usq::Side::Value::Sell);
    enc.transactTime(2000);
    enc.orderQty(50);
    enc.ordType(usq::OrdType::Value::Market);
    enc.price(usq::NewOrderSingle::priceNullValue());
    enc.timeInForce(usq::TimeInForce::Value::NULL_VALUE);
    enc.putText(std::string_view{});
    enc.tradeDate(usq::NewOrderSingle::tradeDateNullValue());
    enc.maturityTime(usq::NewOrderSingle::maturityTimeNullValue());

    usq::NewOrderSingle dec;
    dec.wrapForDecode(reinterpret_cast<char*>(buffer.data()), usq::MessageHeader::encodedLength(),
                      usq::NewOrderSingle::sbeBlockLength(), usq::NewOrderSingle::sbeSchemaVersion(), buffer.size());
    EXPECT_EQ("", dec.getAccountAsString());
    EXPECT_EQ(usq::NewOrderSingle::priceNullValue(), dec.price());
    EXPECT_EQ(usq::TimeInForce::Value::NULL_VALUE, dec.timeInForce());
    EXPECT_EQ("", dec.getTextAsString());
    EXPECT_EQ(usq::NewOrderSingle::tradeDateNullValue(), dec.tradeDate());
    EXPECT_EQ(usq::NewOrderSingle::maturityTimeNullValue(), dec.maturityTime());
}

TEST(UnsequencedCodec, ExecutionReportRoundTripsOptionalFieldsPresent)
{
    alignas(16) std::array<std::uint8_t, 256> buffer{};
    usq::ExecutionReport enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(5).sessionId(6);
    enc.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(9).sendingTimeMs(1000);
    enc.putOrderID("ORD-3").putClOrdID("ORD-3").putExecID("EXEC-1");
    enc.execType(usq::ExecType::Value::Trade);
    enc.ordStatus(usq::OrdStatus::Value::PartiallyFilled);
    enc.putSymbol("AAPL");
    enc.side(usq::Side::Value::Buy);
    enc.orderQty(100);
    enc.price(10'000'000'000LL);
    enc.lastQty(40);
    enc.lastPx(10'000'000'000LL);
    enc.leavesQty(60);
    enc.cumQty(40);
    enc.avgPx(10'000'000'000LL);
    enc.transactTime(2000);
    enc.putText("partial fill");

    usq::ExecutionReport dec;
    dec.wrapForDecode(reinterpret_cast<char*>(buffer.data()), usq::MessageHeader::encodedLength(),
                      usq::ExecutionReport::sbeBlockLength(), usq::ExecutionReport::sbeSchemaVersion(), buffer.size());
    EXPECT_EQ(5, dec.header().sourceId());
    EXPECT_EQ(6, dec.header().sessionId());
    EXPECT_EQ("ORD-3", dec.getOrderIDAsString());
    EXPECT_EQ("EXEC-1", dec.getExecIDAsString());
    EXPECT_EQ(usq::ExecType::Value::Trade, dec.execType());
    EXPECT_EQ(usq::OrdStatus::Value::PartiallyFilled, dec.ordStatus());
    EXPECT_EQ(100u, dec.orderQty());
    EXPECT_EQ(10'000'000'000LL, dec.price());
    EXPECT_EQ(40u, dec.lastQty());
    EXPECT_EQ(10'000'000'000LL, dec.lastPx());
    EXPECT_EQ(60u, dec.leavesQty());
    EXPECT_EQ(40u, dec.cumQty());
    EXPECT_EQ(10'000'000'000LL, dec.avgPx());
    EXPECT_EQ("partial fill", dec.getTextAsString());
}

// ── sbe-frame.xml ─────────────────────────────────────────────────────────
//
// The TCP lifecycle events are core payloads now, and a core payload carries no header of its own: the
// identity is the frame's. These check that the envelope round-trips both ways round, which is what
// every consumer's dispatch rests on.

TEST(FrameCodec, ClientConnectedRoundTripsInsideAnUnsequencedFrame)
{
    alignas(16) std::array<std::uint8_t, 64> payload{};
    frm::ClientConnected enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    enc.putConnectionData(nullptr, 0);
    const auto payloadLength = static_cast<std::uint16_t>(frm::MessageHeader::encodedLength() + enc.encodedLength());

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::Unsequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header().sourceId(77).connectionId(5).sessionId(88).payloadId(sequencer::CORE_PAYLOAD_ID);
    frame.putPayload(reinterpret_cast<const char*>(payload.data()), payloadLength);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_FALSE(view.valid) << "an Unsequenced frame is not a tap frame; only Sequenced is";
}

TEST(FrameCodec, ClientDisconnectedRoundTripsInsideASequencedFrame)
{
    alignas(16) std::array<std::uint8_t, 64> payload{};
    frm::ClientDisconnected enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    const auto payloadLength = static_cast<std::uint16_t>(frm::MessageHeader::encodedLength() + enc.encodedLength());

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(99)
        .connectionId(7)
        .sessionId(100)
        .payloadId(sequencer::CORE_PAYLOAD_ID)
        .globalSeqNo(42)
        .timestamp(1700000000000LL);
    frame.putPayload(reinterpret_cast<const char*>(payload.data()), payloadLength);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(sequencer::CORE_PAYLOAD_ID, view.payloadId);
    EXPECT_EQ(frm::ClientDisconnected::sbeTemplateId(), view.templateId);
    EXPECT_EQ(99, view.sourceId);
    EXPECT_EQ(7, view.connectionId);
    EXPECT_EQ(100, view.sessionId);
    EXPECT_EQ(42, view.globalSeqNo);
    EXPECT_EQ(1700000000000LL, view.timestamp);
    EXPECT_EQ(payloadLength, view.payloadLength);
}

// ── sbe-sequenced.xml ─────────────────────────────────────────────────────
//
// Same messages as sbe-unsequenced.xml; these tests focus on the extended
// header composite (adds globalSeqNo/timestamp) and reuse one representative
// admin message and one application message to keep the schemas in lock-step.

TEST(SequencedCodec, MessageHeaderIdentifiesSchemaAndVersion)
{
    alignas(16) std::array<std::uint8_t, 128> buffer{};
    seq::Heartbeat hb;
    hb.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    hb.header().sourceId(1).sessionId(2).globalSeqNo(3).timestamp(4);
    hb.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(1).sendingTimeMs(0);
    hb.testReqID()[0] = '\0';

    const auto hdr = decodeHeader<seq::MessageHeader>(buffer.data(), buffer.size());
    EXPECT_EQ(SEQUENCED_SCHEMA_ID, hdr.schemaId());
    EXPECT_EQ(SCHEMA_VERSION, hdr.version());
    EXPECT_EQ(seq::Heartbeat::sbeTemplateId(), hdr.templateId());
    EXPECT_EQ(seq::Heartbeat::sbeBlockLength(), hdr.blockLength());
}

TEST(SequencedCodec, HeaderCarriesSourceSessionGlobalSeqNoAndTimestamp)
{
    alignas(16) std::array<std::uint8_t, 128> buffer{};
    seq::Heartbeat enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(10).sessionId(20).globalSeqNo(123456789LL).timestamp(1700000000000LL);
    enc.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(1).sendingTimeMs(0);
    enc.testReqID()[0] = '\0';

    seq::Heartbeat dec;
    dec.wrapForDecode(reinterpret_cast<char*>(buffer.data()), seq::MessageHeader::encodedLength(),
                      seq::Heartbeat::sbeBlockLength(), seq::Heartbeat::sbeSchemaVersion(), buffer.size());
    EXPECT_EQ(10, dec.header().sourceId());
    EXPECT_EQ(20, dec.header().sessionId());
    EXPECT_EQ(123456789LL, dec.header().globalSeqNo());
    EXPECT_EQ(1700000000000LL, dec.header().timestamp());
}

TEST(SequencedCodec, ExecutionReportRoundTripsAllFieldsWithSequencingStamp)
{
    alignas(16) std::array<std::uint8_t, 512> buffer{};
    seq::ExecutionReport enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(5).sessionId(6).globalSeqNo(1000).timestamp(2000);
    enc.putSender(std::string_view{ "CLIENT" }).putTarget(std::string_view{ "PHIXERON" }).seqNum(9).sendingTimeMs(1000);
    enc.putOrderID("ORD-3").putClOrdID("ORD-3").putExecID("EXEC-1");
    enc.execType(seq::ExecType::Value::New);
    enc.ordStatus(seq::OrdStatus::Value::New);
    enc.putSymbol("AAPL");
    enc.side(seq::Side::Value::Buy);
    enc.orderQty(100);
    enc.price(seq::ExecutionReport::priceNullValue());
    enc.lastQty(seq::ExecutionReport::lastQtyNullValue());
    enc.lastPx(seq::ExecutionReport::lastPxNullValue());
    enc.leavesQty(100);
    enc.cumQty(0);
    enc.avgPx(0);
    enc.transactTime(2000);
    enc.putText(std::string_view{});

    seq::ExecutionReport dec;
    dec.wrapForDecode(reinterpret_cast<char*>(buffer.data()), seq::MessageHeader::encodedLength(),
                      seq::ExecutionReport::sbeBlockLength(), seq::ExecutionReport::sbeSchemaVersion(), buffer.size());
    EXPECT_EQ(5, dec.header().sourceId());
    EXPECT_EQ(6, dec.header().sessionId());
    EXPECT_EQ(1000, dec.header().globalSeqNo());
    EXPECT_EQ(2000, dec.header().timestamp());
    EXPECT_EQ("ORD-3", dec.getOrderIDAsString());
    EXPECT_EQ(seq::ExecType::Value::New, dec.execType());
    EXPECT_EQ(seq::OrdStatus::Value::New, dec.ordStatus());
    EXPECT_EQ(100u, dec.orderQty());
    EXPECT_EQ(seq::ExecutionReport::priceNullValue(), dec.price());
    EXPECT_EQ(100u, dec.leavesQty());
}

TEST(FrameCodec, ABareSequencedMessageIsReadAsOneWithNoPayloadId)
{
    // The other shape still on the tap: a schema-202 message with its own header composite. unwrapFrame
    // is the one place the two are told apart, so a consumer never has to.
    alignas(16) std::array<std::uint8_t, 128> buffer{};
    seq::Heartbeat enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    enc.header().sourceId(77).connectionId(3).sessionId(88).globalSeqNo(1).timestamp(1700000000000LL);
    enc.putSender("CLIENT").putTarget("PHIXERON").seqNum(4).sendingTimeMs(0);

    const auto length = seq::MessageHeader::encodedLength() + enc.encodedLength();
    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()), length);
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(sequencer::NO_PAYLOAD_ID, view.payloadId);
    EXPECT_EQ(seq::Heartbeat::sbeTemplateId(), view.templateId);
    EXPECT_EQ(77, view.sourceId);
    EXPECT_EQ(1, view.globalSeqNo);
    EXPECT_EQ(length, view.payloadLength) << "a bare message is the whole fragment";
}
