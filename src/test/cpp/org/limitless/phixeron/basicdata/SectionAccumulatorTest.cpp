// Unit tests for SectionAccumulator — the consumer-side atomic-commit contract for one basic-data
// section (doc/basicdata-design.md §5). Pure logic, no Aeron: the rules under test are exactly the ones
// that make an interrupted or re-sent load safe to consume, and until this was extracted from
// BasicDataServer they were only reachable through a connected process.

#include <cstdint>
#include <string>
#include <vector>

#include "org/limitless/phixeron/basicdata/SectionAccumulator.hpp"
#include "gtest/gtest.h"

namespace org::limitless::phixeron::basicdata {
namespace {

struct Row
{
    int id;
    std::string name;

    bool operator==(const Row& other) const = default;
};

// The ordinary case: rows accumulate invisibly and the whole section appears at once, in arrival order.
TEST(SectionAccumulator, CommitsTheWholeSectionOnTheFinalRow)
{
    SectionAccumulator<Row> accumulator;

    EXPECT_FALSE(accumulator.accept(2, Row{ 1, "a" }).has_value());
    EXPECT_FALSE(accumulator.accept(1, Row{ 2, "b" }).has_value());

    const auto committed = accumulator.accept(0, Row{ 3, "c" });
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ((std::vector<Row>{ { 1, "a" }, { 2, "b" }, { 3, "c" } }), *committed);
}

// A one-row section is complete on its first row — remainingItems is already 0.
TEST(SectionAccumulator, CommitsASingleRowSectionImmediately)
{
    SectionAccumulator<Row> accumulator;

    const auto committed = accumulator.accept(0, Row{ 1, "only" });
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ((std::vector<Row>{ { 1, "only" } }), *committed);
    EXPECT_FALSE(accumulator.isAccumulating());
}

// The §4/§5 pairing: a producer that died mid-section re-sends that section WHOLE, so a row that is not
// the strict successor of the last one discards the partial rather than splicing the two runs together.
TEST(SectionAccumulator, DiscardsAPartialSectionWhenAResendStartsOver)
{
    SectionAccumulator<Row> accumulator;

    EXPECT_FALSE(accumulator.accept(2, Row{ 1, "partial-a" }).has_value());
    EXPECT_FALSE(accumulator.accept(1, Row{ 2, "partial-b" }).has_value());

    // Leader change: the whole section arrives again from its first row.
    EXPECT_FALSE(accumulator.accept(2, Row{ 1, "resent-a" }).has_value());
    EXPECT_FALSE(accumulator.accept(1, Row{ 2, "resent-b" }).has_value());

    const auto committed = accumulator.accept(0, Row{ 3, "resent-c" });
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ((std::vector<Row>{ { 1, "resent-a" }, { 2, "resent-b" }, { 3, "resent-c" } }), *committed);
}

// A repeated row is not a successor either, so it restarts accumulation rather than being appended
// twice — a duplicated row must never inflate the committed table.
TEST(SectionAccumulator, ARepeatedRowRestartsAccumulation)
{
    SectionAccumulator<Row> accumulator;

    EXPECT_FALSE(accumulator.accept(2, Row{ 1, "first" }).has_value());
    EXPECT_FALSE(accumulator.accept(2, Row{ 1, "again" }).has_value());
    EXPECT_FALSE(accumulator.accept(1, Row{ 2, "b" }).has_value());

    const auto committed = accumulator.accept(0, Row{ 3, "c" });
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ((std::vector<Row>{ { 1, "again" }, { 2, "b" }, { 3, "c" } }), *committed);
}

// StartBasicData: a load is beginning, so any lingering partial from a previous attempt is dropped.
TEST(SectionAccumulator, ResetDiscardsAnInFlightSection)
{
    SectionAccumulator<Row> accumulator;

    EXPECT_FALSE(accumulator.accept(2, Row{ 1, "stale-a" }).has_value());
    EXPECT_FALSE(accumulator.accept(1, Row{ 2, "stale-b" }).has_value());
    EXPECT_TRUE(accumulator.isAccumulating());

    accumulator.reset();
    EXPECT_FALSE(accumulator.isAccumulating());

    EXPECT_FALSE(accumulator.accept(1, Row{ 1, "fresh-a" }).has_value());
    const auto committed = accumulator.accept(0, Row{ 2, "fresh-b" });
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ((std::vector<Row>{ { 1, "fresh-a" }, { 2, "fresh-b" } }), *committed);
}

// Recovery replays the same code path as a first load, so one accumulator serves load after load: a
// commit must leave no residue behind for the next section.
TEST(SectionAccumulator, IsReusableAcrossLoads)
{
    SectionAccumulator<Row> accumulator;

    EXPECT_FALSE(accumulator.accept(1, Row{ 1, "load1-a" }).has_value());
    ASSERT_TRUE(accumulator.accept(0, Row{ 2, "load1-b" }).has_value());

    EXPECT_FALSE(accumulator.accept(1, Row{ 3, "load2-a" }).has_value());
    const auto second = accumulator.accept(0, Row{ 4, "load2-b" });
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ((std::vector<Row>{ { 3, "load2-a" }, { 4, "load2-b" } }), *second);
}

// The partial is never observable: only the accept() that closes a section yields anything at all.
TEST(SectionAccumulator, YieldsNothingWhileASectionIsIncomplete)
{
    SectionAccumulator<Row> accumulator;

    for (std::uint16_t remaining = 4; remaining > 0; --remaining)
    {
        EXPECT_FALSE(accumulator.accept(remaining, Row{ remaining, "row" }).has_value())
            << "committed early at remainingItems=" << remaining;
        EXPECT_TRUE(accumulator.isAccumulating());
    }
    ASSERT_TRUE(accumulator.accept(0, Row{ 0, "last" }).has_value());
}

} // namespace
} // namespace org::limitless::phixeron::basicdata
