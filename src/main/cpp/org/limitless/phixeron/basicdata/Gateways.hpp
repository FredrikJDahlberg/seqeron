#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace org::limitless::phixeron::basicdata {

struct GatewayIdentity
{
    std::int32_t gatewayId = 0;
    std::int32_t gatewaySourceId = 0;
    std::uint8_t preferenceRank = 0;
};

// Accumulates the Gateway rows seen on the sequenced stream, keyed by gatewayName.
class Gateways
{
  public:
    void add(std::string_view gatewayName, GatewayIdentity identity)
    {
        m_byName.insert_or_assign(std::string{ gatewayName }, identity);
    }

    [[nodiscard]] std::optional<GatewayIdentity> resolve(std::string_view gatewayName) const
    {
        const auto it = m_byName.find(std::string{ gatewayName });
        if (it == m_byName.end())
        {
            return std::nullopt;
        }
        return it->second;
    }

    [[nodiscard]] bool isInstanceOf(std::int32_t gatewayId, std::int32_t gatewaySourceId) const
    {
        for (const auto& [name, identity] : m_byName)
        {
            if (identity.gatewayId == gatewayId && identity.gatewaySourceId == gatewaySourceId)
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_byName.size();
    }

  private:
    std::unordered_map<std::string, GatewayIdentity> m_byName;
};
} // namespace org::limitless::phixeron::basicdata
