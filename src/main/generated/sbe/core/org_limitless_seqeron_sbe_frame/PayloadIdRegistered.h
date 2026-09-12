/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _ORG_LIMITLESS_SEQERON_SBE_FRAME_PAYLOADIDREGISTERED_CXX_H_
#define _ORG_LIMITLESS_SEQERON_SBE_FRAME_PAYLOADIDREGISTERED_CXX_H_

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


#include "UnsequencedHeader.h"
#include "UnsequencedSystemHeader.h"
#include "MessageHeader.h"
#include "SequencedHeader.h"
#include "SequencedSystemHeader.h"
#include "VarDataEncoding.h"

namespace org {
namespace limitless {
namespace seqeron {
namespace sbe {
namespace frame {

class PayloadIdRegistered
{
private:
    char *m_buffer = nullptr;
    std::uint64_t m_bufferLength = 0;
    std::uint64_t m_offset = 0;
    std::uint64_t m_position = 0;
    std::uint64_t m_actingBlockLength = 0;
    std::uint64_t m_actingVersion = 0;

    inline std::uint64_t *sbePositionPtr() SBE_NOEXCEPT
    {
        return &m_position;
    }

public:
    static constexpr std::uint16_t SBE_BLOCK_LENGTH = static_cast<std::uint16_t>(36);
    static constexpr std::uint16_t SBE_TEMPLATE_ID = static_cast<std::uint16_t>(23);
    static constexpr std::uint16_t SBE_SCHEMA_ID = static_cast<std::uint16_t>(210);
    static constexpr std::uint16_t SBE_SCHEMA_VERSION = static_cast<std::uint16_t>(0);
    static constexpr const char* SBE_SEMANTIC_VERSION = "1.0";

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

    using messageHeader = MessageHeader;

    PayloadIdRegistered() = default;

    PayloadIdRegistered(
        char *buffer,
        const std::uint64_t offset,
        const std::uint64_t bufferLength,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion) :
        m_buffer(buffer),
        m_bufferLength(bufferLength),
        m_offset(offset),
        m_position(sbeCheckPosition(offset + actingBlockLength)),
        m_actingBlockLength(actingBlockLength),
        m_actingVersion(actingVersion)
    {
    }

    PayloadIdRegistered(char *buffer, const std::uint64_t bufferLength) :
        PayloadIdRegistered(buffer, 0, bufferLength, sbeBlockLength(), sbeSchemaVersion())
    {
    }

    PayloadIdRegistered(
        char *buffer,
        const std::uint64_t bufferLength,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion) :
        PayloadIdRegistered(buffer, 0, bufferLength, actingBlockLength, actingVersion)
    {
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeBlockLength() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(36);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t sbeBlockAndHeaderLength() SBE_NOEXCEPT
    {
        return messageHeader::encodedLength() + sbeBlockLength();
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeTemplateId() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(23);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeSchemaId() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(210);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeSchemaVersion() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    SBE_NODISCARD static const char *sbeSemanticVersion() SBE_NOEXCEPT
    {
        return "1.0";
    }

    SBE_NODISCARD static SBE_CONSTEXPR const char *sbeSemanticType() SBE_NOEXCEPT
    {
        return "";
    }

    SBE_NODISCARD std::uint64_t offset() const SBE_NOEXCEPT
    {
        return m_offset;
    }

    PayloadIdRegistered &wrapForEncode(char *buffer, const std::uint64_t offset, const std::uint64_t bufferLength)
    {
        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset;
        m_actingBlockLength = sbeBlockLength();
        m_actingVersion = sbeSchemaVersion();
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    PayloadIdRegistered &wrapAndApplyHeader(char *buffer, const std::uint64_t offset, const std::uint64_t bufferLength)
    {
        messageHeader hdr(buffer, offset, bufferLength, sbeSchemaVersion());

        hdr
            .blockLength(sbeBlockLength())
            .templateId(sbeTemplateId())
            .schemaId(sbeSchemaId())
            .version(sbeSchemaVersion());

        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset + messageHeader::encodedLength();
        m_actingBlockLength = sbeBlockLength();
        m_actingVersion = sbeSchemaVersion();
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    PayloadIdRegistered &wrapForDecode(
        char *buffer,
        const std::uint64_t offset,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion,
        const std::uint64_t bufferLength)
    {
        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset;
        m_actingBlockLength = actingBlockLength;
        m_actingVersion = actingVersion;
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    PayloadIdRegistered &sbeRewind()
    {
        return wrapForDecode(m_buffer, m_offset, m_actingBlockLength, m_actingVersion, m_bufferLength);
    }

    SBE_NODISCARD std::uint64_t sbePosition() const SBE_NOEXCEPT
    {
        return m_position;
    }

    // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    std::uint64_t sbeCheckPosition(const std::uint64_t position)
    {
        if (SBE_BOUNDS_CHECK_EXPECT((position > m_bufferLength), false))
        {
            throw std::runtime_error("buffer too short [E100]");
        }
        return position;
    }

    void sbePosition(const std::uint64_t position)
    {
        m_position = sbeCheckPosition(position);
    }

    SBE_NODISCARD std::uint64_t encodedLength() const SBE_NOEXCEPT
    {
        return sbePosition() - m_offset;
    }

    SBE_NODISCARD std::uint64_t decodeLength() const
    {
        PayloadIdRegistered skipper(m_buffer, m_offset, m_bufferLength, m_actingBlockLength, m_actingVersion);
        skipper.skip();
        return skipper.encodedLength();
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

    SBE_NODISCARD static const char *payloadIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t payloadIdId() SBE_NOEXCEPT
    {
        return 20042;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t payloadIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool payloadIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t payloadIdEncodingOffset() SBE_NOEXCEPT
    {
        return 0;
    }

    static SBE_CONSTEXPR std::uint16_t payloadIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT16;
    }

    static SBE_CONSTEXPR std::uint16_t payloadIdMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    static SBE_CONSTEXPR std::uint16_t payloadIdMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(65534);
    }

    static SBE_CONSTEXPR std::size_t payloadIdEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::uint16_t payloadId() const SBE_NOEXCEPT
    {
        std::uint16_t val;
        std::memcpy(&val, m_buffer + m_offset + 0, sizeof(std::uint16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    PayloadIdRegistered &payloadId(const std::uint16_t value) SBE_NOEXCEPT
    {
        std::uint16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 0, &val, sizeof(std::uint16_t));
        return *this;
    }

    SBE_NODISCARD static const char *protocolVersionMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t protocolVersionId() SBE_NOEXCEPT
    {
        return 20043;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t protocolVersionSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool protocolVersionInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t protocolVersionEncodingOffset() SBE_NOEXCEPT
    {
        return 2;
    }

    static SBE_CONSTEXPR std::uint16_t protocolVersionNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT16;
    }

    static SBE_CONSTEXPR std::uint16_t protocolVersionMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    static SBE_CONSTEXPR std::uint16_t protocolVersionMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(65534);
    }

    static SBE_CONSTEXPR std::size_t protocolVersionEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::uint16_t protocolVersion() const SBE_NOEXCEPT
    {
        std::uint16_t val;
        std::memcpy(&val, m_buffer + m_offset + 2, sizeof(std::uint16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    PayloadIdRegistered &protocolVersion(const std::uint16_t value) SBE_NOEXCEPT
    {
        std::uint16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 2, &val, sizeof(std::uint16_t));
        return *this;
    }

    SBE_NODISCARD static const char *protocolNameMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t protocolNameId() SBE_NOEXCEPT
    {
        return 20044;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t protocolNameSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool protocolNameInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t protocolNameEncodingOffset() SBE_NOEXCEPT
    {
        return 4;
    }

    static SBE_CONSTEXPR char protocolNameNullValue() SBE_NOEXCEPT
    {
        return static_cast<char>(0);
    }

    static SBE_CONSTEXPR char protocolNameMinValue() SBE_NOEXCEPT
    {
        return static_cast<char>(32);
    }

    static SBE_CONSTEXPR char protocolNameMaxValue() SBE_NOEXCEPT
    {
        return static_cast<char>(126);
    }

    static SBE_CONSTEXPR std::size_t protocolNameEncodingLength() SBE_NOEXCEPT
    {
        return 32;
    }

    static SBE_CONSTEXPR std::uint64_t protocolNameLength() SBE_NOEXCEPT
    {
        return 32;
    }

    SBE_NODISCARD const char *protocolName() const SBE_NOEXCEPT
    {
        return m_buffer + m_offset + 4;
    }

    SBE_NODISCARD char *protocolName() SBE_NOEXCEPT
    {
        return m_buffer + m_offset + 4;
    }

    SBE_NODISCARD char protocolName(const std::uint64_t index) const
    {
        if (index >= 32)
        {
            throw std::runtime_error("index out of range for protocolName [E104]");
        }

        char val;
        std::memcpy(&val, m_buffer + m_offset + 4 + (index * 1), sizeof(char));
        return (val);
    }

    PayloadIdRegistered &protocolName(const std::uint64_t index, const char value)
    {
        if (index >= 32)
        {
            throw std::runtime_error("index out of range for protocolName [E105]");
        }

        char val = (value);
        std::memcpy(m_buffer + m_offset + 4 + (index * 1), &val, sizeof(char));
        return *this;
    }

    std::uint64_t getProtocolName(char *const dst, const std::uint64_t length) const
    {
        if (length > 32)
        {
            throw std::runtime_error("length too large for getProtocolName [E106]");
        }

        std::memcpy(dst, m_buffer + m_offset + 4, sizeof(char) * static_cast<std::size_t>(length));
        return length;
    }

    #ifdef SBE_USE_SPAN
    SBE_NODISCARD std::span<const char> getProtocolNameAsSpan() const SBE_NOEXCEPT
    {
        const char *buffer = m_buffer + m_offset + 4;
        return std::span<const char>(reinterpret_cast<const char*>(buffer), 32);
    }
    #endif

    #ifdef SBE_USE_SPAN
    template <std::size_t N>
    PayloadIdRegistered &putProtocolName(std::span<const char, N> src) SBE_NOEXCEPT
    {
        static_assert(N <= 32, "array too large for putProtocolName");

        if (N > 0)
        {
            std::memcpy(m_buffer + m_offset + 4, src.data(), sizeof(char) * N);
        }

        for (std::size_t start = N; start < 32; ++start)
        {
            m_buffer[m_offset + 4 + start] = 0;
        }

        return *this;
    }
    #endif

    #ifdef SBE_USE_SPAN
    template <typename T>
    PayloadIdRegistered &putProtocolName(T&& src)  SBE_NOEXCEPT requires
        (std::is_pointer_v<std::remove_reference_t<T>> &&
         !std::is_array_v<std::remove_reference_t<T>>)
    #else
    PayloadIdRegistered &putProtocolName(const char *const src) SBE_NOEXCEPT
    #endif
    {
        std::memcpy(m_buffer + m_offset + 4, src, sizeof(char) * 32);
        return *this;
    }

    #ifdef SBE_USE_SPAN
    template <std::size_t N>
    PayloadIdRegistered &putProtocolName(const char (&src)[N]) SBE_NOEXCEPT
    {
        return putProtocolName(std::span<const char, N>(src));
    }
    #endif

    SBE_NODISCARD std::string getProtocolNameAsString() const
    {
        const char *buffer = m_buffer + m_offset + 4;
        std::size_t length = 0;

        for (; length < 32 && *(buffer + length) != '\0'; ++length);
        std::string result(buffer, length);

        return result;
    }

    std::string getProtocolNameAsJsonEscapedString()
    {
        std::ostringstream oss;
        std::string s = getProtocolNameAsString();

        for (const auto c : s)
        {
            switch (c)
            {
                case '"': oss << "\\\""; break;
                case '\\': oss << "\\\\"; break;
                case '\b': oss << "\\b"; break;
                case '\f': oss << "\\f"; break;
                case '\n': oss << "\\n"; break;
                case '\r': oss << "\\r"; break;
                case '\t': oss << "\\t"; break;

                default:
                    if ('\x00' <= c && c <= '\x1f')
                    {
                        oss << "\\u" << std::hex << std::setw(4)
                            << std::setfill('0') << (int)(c);
                    }
                    else
                    {
                        oss << c;
                    }
            }
        }

        return oss.str();
    }

    #ifdef SBE_USE_STRING_VIEW
    SBE_NODISCARD std::string_view getProtocolNameAsStringView() const SBE_NOEXCEPT
    {
        const char *buffer = m_buffer + m_offset + 4;
        std::size_t length = 0;

        for (; length < 32 && *(buffer + length) != '\0'; ++length);
        std::string_view result(buffer, length);

        return result;
    }
    #endif

    #ifdef SBE_USE_STRING_VIEW
    PayloadIdRegistered &putProtocolName(const std::string_view str)
    {
        const std::size_t srcLength = str.length();
        if (srcLength > 32)
        {
            throw std::runtime_error("string too large for putProtocolName [E106]");
        }

        if (srcLength > 0)
        {
            std::memcpy(m_buffer + m_offset + 4, str.data(), srcLength);
        }

        for (std::size_t start = srcLength; start < 32; ++start)
        {
            m_buffer[m_offset + 4 + start] = 0;
        }

        return *this;
    }
    #else
    PayloadIdRegistered &putProtocolName(const std::string &str)
    {
        const std::size_t srcLength = str.length();
        if (srcLength > 32)
        {
            throw std::runtime_error("string too large for putProtocolName [E106]");
        }

        std::memcpy(m_buffer + m_offset + 4, str.c_str(), srcLength);
        for (std::size_t start = srcLength; start < 32; ++start)
        {
            m_buffer[m_offset + 4 + start] = 0;
        }

        return *this;
    }
    #endif

template<typename CharT, typename Traits>
friend std::basic_ostream<CharT, Traits> & operator << (
    std::basic_ostream<CharT, Traits> &builder, const PayloadIdRegistered &_writer)
{
    PayloadIdRegistered writer(
        _writer.m_buffer,
        _writer.m_offset,
        _writer.m_bufferLength,
        _writer.m_actingBlockLength,
        _writer.m_actingVersion);

    builder << '{';
    builder << R"("Name": "PayloadIdRegistered", )";
    builder << R"("sbeTemplateId": )";
    builder << writer.sbeTemplateId();
    builder << ", ";

    builder << R"("payloadId": )";
    builder << +writer.payloadId();

    builder << ", ";
    builder << R"("protocolVersion": )";
    builder << +writer.protocolVersion();

    builder << ", ";
    builder << R"("protocolName": )";
    builder << '"' <<
        writer.getProtocolNameAsJsonEscapedString().c_str() << '"';

    builder << '}';

    return builder;
}

void skip()
{
}

SBE_NODISCARD static SBE_CONSTEXPR bool isConstLength() SBE_NOEXCEPT
{
    return true;
}

SBE_NODISCARD static std::size_t computeLength()
{
#if defined(__GNUG__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtype-limits"
#endif
    std::size_t length = sbeBlockLength();

    return length;
#if defined(__GNUG__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
}
};
}
}
}
}
}
#endif
