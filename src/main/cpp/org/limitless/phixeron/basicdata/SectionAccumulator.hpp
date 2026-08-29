#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace org::limitless::phixeron::basicdata {

// The consumer-side atomic-commit contract for ONE basic-data section.
template<typename Row>
class SectionAccumulator
{
  public:
    std::optional<std::vector<Row>> accept(const std::uint16_t remainingItems, Row row)
    {
        if (!m_accumulating || remainingItems + 1 != m_lastRemaining)
        {
            m_scratch.clear();
            m_accumulating = true;
        }
        m_scratch.push_back(std::move(row));
        m_lastRemaining = remainingItems;

        if (remainingItems != 0)
        {
            return std::nullopt;
        }
        m_accumulating = false;
        std::vector<Row> committed = std::move(m_scratch);
        m_scratch.clear(); // a moved-from vector is valid but unspecified; the next section starts empty
        return committed;
    }

    void reset()
    {
        m_scratch.clear();
        m_accumulating = false;
    }

    [[nodiscard]] bool isAccumulating() const noexcept
    {
        return m_accumulating;
    }

  private:
    std::vector<Row> m_scratch;
    bool m_accumulating = false;
    std::uint16_t m_lastRemaining = 0;
};

} // namespace org::limitless::phixeron::basicdata
