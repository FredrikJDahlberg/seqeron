/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _ORG_LIMITLESS_SEQERON_CLUSTER_SBE_CLOSEREASON_CXX_H_
#define _ORG_LIMITLESS_SEQERON_CLUSTER_SBE_CLOSEREASON_CXX_H_

#if !defined(__STDC_LIMIT_MACROS)
#  define __STDC_LIMIT_MACROS 1
#endif

#include <cstdint>
#include <iomanip>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <sstream>
#include <string>

#define SBE_NULLVALUE_INT8 (std::numeric_limits<std::int8_t>::min)()
#define SBE_NULLVALUE_INT16 (std::numeric_limits<std::int16_t>::min)()
#define SBE_NULLVALUE_INT32 (std::numeric_limits<std::int32_t>::min)()
#define SBE_NULLVALUE_INT64 (std::numeric_limits<std::int64_t>::min)()
#define SBE_NULLVALUE_UINT8 (std::numeric_limits<std::uint8_t>::max)()
#define SBE_NULLVALUE_UINT16 (std::numeric_limits<std::uint16_t>::max)()
#define SBE_NULLVALUE_UINT32 (std::numeric_limits<std::uint32_t>::max)()
#define SBE_NULLVALUE_UINT64 (std::numeric_limits<std::uint64_t>::max)()

namespace org {
namespace limitless {
namespace seqeron {
namespace cluster {
namespace sbe {

class CloseReason
{
public:
    enum Value
    {
        CLIENT_ACTION = INT32_C(0),
        SERVICE_ACTION = INT32_C(1),
        TIMEOUT = INT32_C(2),
        NULL_VALUE = INT32_MIN
    };

    static CloseReason::Value get(const std::int32_t value)
    {
        switch (value)
        {
            case INT32_C(0): return CLIENT_ACTION;
            case INT32_C(1): return SERVICE_ACTION;
            case INT32_C(2): return TIMEOUT;
            case INT32_MIN: return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum CloseReason [E103]");
    }

    static const char *c_str(const CloseReason::Value value)
    {
        switch (value)
        {
            case CLIENT_ACTION: return "CLIENT_ACTION";
            case SERVICE_ACTION: return "SERVICE_ACTION";
            case TIMEOUT: return "TIMEOUT";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum CloseReason [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, CloseReason::Value m)
    {
        return os << CloseReason::c_str(m);
    }
};

}
}
}
}
}

#endif
