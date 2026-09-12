/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _ORG_LIMITLESS_SEQERON_SBE_FRAME_UNSEQUENCEDSYSTEMHEADER_CXX_H_
#define _ORG_LIMITLESS_SEQERON_SBE_FRAME_UNSEQUENCEDSYSTEMHEADER_CXX_H_

#if __cplusplus >= 201103L
#  define SBE_CONSTEXPR constexpr
#  define SBE_NOEXCEPT noexcept
#else
#  define SBE_CONSTEXPR
#  define SBE_NOEXCEPT
#endif

#if __cplusplus >= 201703L
#  include <string_view>
#  define SBE_NODISCARD [[nodiscard]]
#  if !defined(SBE_USE_STRING_VIEW)
#    define SBE_USE_STRING_VIEW 1
#  endif
#else
#  define SBE_NODISCARD
#endif

#if __cplusplus >= 202002L
#  include <span>
#  if !defined(SBE_USE_SPAN)
#    define SBE_USE_SPAN 1
#  endif
#endif

#if !defined(__STDC_LIMIT_MACROS)
#  define __STDC_LIMIT_MACROS 1
#endif

#include <cstdint>
#include <limits>
#include <cstring>
#include <iomanip>
#include <ostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>
#include <tuple>

#if defined(WIN32) || defined(_WIN32)
#  define SBE_BIG_ENDIAN_ENCODE_16(v) _byteswap_ushort(v)
#  define SBE_BIG_ENDIAN_ENCODE_32(v) _byteswap_ulong(v)
#  define SBE_BIG_ENDIAN_ENCODE_64(v) _byteswap_uint64(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_16(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_32(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_64(v) (v)
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#  define SBE_BIG_ENDIAN_ENCODE_16(v) __builtin_bswap16(v)
#  define SBE_BIG_ENDIAN_ENCODE_32(v) __builtin_bswap32(v)
#  define SBE_BIG_ENDIAN_ENCODE_64(v) __builtin_bswap64(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_16(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_32(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_64(v) (v)
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define SBE_LITTLE_ENDIAN_ENCODE_16(v) __builtin_bswap16(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_32(v) __builtin_bswap32(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_64(v) __builtin_bswap64(v)
#  define SBE_BIG_ENDIAN_ENCODE_16(v) (v)
#  define SBE_BIG_ENDIAN_ENCODE_32(v) (v)
#  define SBE_BIG_ENDIAN_ENCODE_64(v) (v)
#else
#  error "Byte Ordering of platform not determined. Set __BYTE_ORDER__ manually before including this file."
#endif

#if !defined(SBE_BOUNDS_CHECK_EXPECT)
#  if defined(SBE_NO_BOUNDS_CHECK)
#    define SBE_BOUNDS_CHECK_EXPECT(exp, c) (false)
#  elif defined(_MSC_VER)
#    define SBE_BOUNDS_CHECK_EXPECT(exp, c) (exp)
#  else 
#    define SBE_BOUNDS_CHECK_EXPECT(exp, c) (__builtin_expect(exp, c))
#  endif

#endif

#define SBE_FLOAT_NAN std::numeric_limits<float>::quiet_NaN()
#define SBE_DOUBLE_NAN std::numeric_limits<double>::quiet_NaN()
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
namespace sbe {
namespace frame {

class UnsequencedSystemHeader
{
private:
    char *m_buffer = nullptr;
    std::uint64_t m_bufferLength = 0;
    std::uint64_t m_offset = 0;
    std::uint64_t m_actingVersion = 0;

public:
    enum MetaAttribute
    {
        EPOCH, TIME_UNIT, SEMANTIC_TYPE, PRESENCE
    };

    union sbe_float_as_uint_u
    {
        float fp_value;
        std::uint32_t uint_value;
    };

    union sbe_double_as_uint_u
    {
        double fp_value;
        std::uint64_t uint_value;
    };

    UnsequencedSystemHeader() = default;

    UnsequencedSystemHeader(
        char *buffer,
        const std::uint64_t offset,
        const std::uint64_t bufferLength,
        const std::uint64_t actingVersion) :
        m_buffer(buffer),
        m_bufferLength(bufferLength),
        m_offset(offset),
        m_actingVersion(actingVersion)
    {
        if (SBE_BOUNDS_CHECK_EXPECT(((m_offset + 18) > m_bufferLength), false))
        {
            throw std::runtime_error("buffer too short for flyweight [E107]");
        }
    }

    UnsequencedSystemHeader(
        char *buffer,
        const std::uint64_t bufferLength,
        const std::uint64_t actingVersion) :
        UnsequencedSystemHeader(buffer, 0, bufferLength, actingVersion)
    {
    }

    UnsequencedSystemHeader(
        char *buffer,
        const std::uint64_t bufferLength) :
        UnsequencedSystemHeader(buffer, 0, bufferLength, sbeSchemaVersion())
    {
    }

    UnsequencedSystemHeader &wrap(
        char *buffer,
        const std::uint64_t offset,
        const std::uint64_t actingVersion,
        const std::uint64_t bufferLength)
    {
        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset;
        m_actingVersion = actingVersion;

        if (SBE_BOUNDS_CHECK_EXPECT(((m_offset + 18) > m_bufferLength), false))
        {
            throw std::runtime_error("buffer too short for flyweight [E107]");
        }

        return *this;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t encodedLength() SBE_NOEXCEPT
    {
        return 18;
    }

    SBE_NODISCARD std::uint64_t offset() const SBE_NOEXCEPT
    {
        return m_offset;
    }

    SBE_NODISCARD const char *buffer() const SBE_NOEXCEPT
    {
        return m_buffer;
    }

    SBE_NODISCARD char *buffer() SBE_NOEXCEPT
    {
        return m_buffer;
    }

    SBE_NODISCARD std::uint64_t bufferLength() const SBE_NOEXCEPT
    {
        return m_bufferLength;
    }

    SBE_NODISCARD std::uint64_t actingVersion() const SBE_NOEXCEPT
    {
        return m_actingVersion;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeSchemaId() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(210);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeSchemaVersion() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    SBE_NODISCARD static const char *sourceIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t sourceIdId() SBE_NOEXCEPT
    {
        return -1;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t sourceIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool sourceIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t sourceIdEncodingOffset() SBE_NOEXCEPT
    {
        return 0;
    }

    static SBE_CONSTEXPR std::int32_t sourceIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t sourceIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t sourceIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t sourceIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t sourceId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 0, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    UnsequencedSystemHeader &sourceId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 0, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *connectionIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t connectionIdId() SBE_NOEXCEPT
    {
        return -1;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t connectionIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool connectionIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t connectionIdEncodingOffset() SBE_NOEXCEPT
    {
        return 4;
    }

    static SBE_CONSTEXPR std::int32_t connectionIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t connectionIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t connectionIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t connectionIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t connectionId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 4, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    UnsequencedSystemHeader &connectionId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 4, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *sessionIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t sessionIdId() SBE_NOEXCEPT
    {
        return -1;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t sessionIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool sessionIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t sessionIdEncodingOffset() SBE_NOEXCEPT
    {
        return 8;
    }

    static SBE_CONSTEXPR std::int64_t sessionIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t sessionIdMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t sessionIdMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t sessionIdEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t sessionId() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 8, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    UnsequencedSystemHeader &sessionId(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 8, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *systemEventTypeMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t systemEventTypeId() SBE_NOEXCEPT
    {
        return -1;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t systemEventTypeSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool systemEventTypeInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t systemEventTypeEncodingOffset() SBE_NOEXCEPT
    {
        return 16;
    }

    static SBE_CONSTEXPR std::uint16_t systemEventTypeNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT16;
    }

    static SBE_CONSTEXPR std::uint16_t systemEventTypeMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    static SBE_CONSTEXPR std::uint16_t systemEventTypeMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(65534);
    }

    static SBE_CONSTEXPR std::size_t systemEventTypeEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::uint16_t systemEventType() const SBE_NOEXCEPT
    {
        std::uint16_t val;
        std::memcpy(&val, m_buffer + m_offset + 16, sizeof(std::uint16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    UnsequencedSystemHeader &systemEventType(const std::uint16_t value) SBE_NOEXCEPT
    {
        std::uint16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 16, &val, sizeof(std::uint16_t));
        return *this;
    }

template<typename CharT, typename Traits>
friend std::basic_ostream<CharT, Traits> & operator << (
    std::basic_ostream<CharT, Traits> &builder, UnsequencedSystemHeader &writer)
{
    builder << '{';
    builder << R"("sourceId": )";
    builder << +writer.sourceId();

    builder << ", ";
    builder << R"("connectionId": )";
    builder << +writer.connectionId();

    builder << ", ";
    builder << R"("sessionId": )";
    builder << +writer.sessionId();

    builder << ", ";
    builder << R"("systemEventType": )";
    builder << +writer.systemEventType();

    builder << '}';

    return builder;
}

};

}
}
}
}
}

#endif
