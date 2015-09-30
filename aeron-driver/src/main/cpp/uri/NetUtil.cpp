//
// Created by Michael Barker on 01/09/15.
//

#include "NetUtil.h"

#ifdef __APPLE__

#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#endif

using namespace aeron::driver::uri;

static std::uint64_t prefixLengthToIpV6Mask(std::uint64_t subnetPrefix)
{
    return 0 == subnetPrefix ? 0 : ~((1L << (64 - subnetPrefix)) - UINT64_C(1));
}


static std::uint32_t prefixLengthToIpV4Mask(std::uint32_t subnetPrefix)
{
    return 0 == subnetPrefix ? 0 : ~((1 << (32 - subnetPrefix)) - UINT32_C(1));
}

bool NetUtil::wildcardMatch(const struct in6_addr* data, const struct in6_addr* pattern, std::uint32_t prefixLength)
{
    union _u
    {
        const std::uint8_t* b;
        std::uint64_t* l;
    };

    union _u cvt;

    cvt.b = data->s6_addr;
    std::uint64_t dataUpper = *(cvt.l);

    cvt.b = &data->s6_addr[8];
    std::uint64_t dataLower = *(cvt.l);

    cvt.b = pattern->s6_addr;
    std::uint64_t patternUpper = *(cvt.l);

    cvt.b = &pattern->s6_addr[8];
    std::uint64_t patternLower = *(cvt.l);

    std::uint64_t maskUpper = htobe64(prefixLengthToIpV6Mask(prefixLength));
    std::uint64_t lowerPrefixLength = prefixLength > 64 ? prefixLength - 64 : 0;
    std::uint64_t maskLower = htobe64(prefixLengthToIpV6Mask(lowerPrefixLength));

    return
        (maskUpper & dataUpper) == (maskUpper & patternUpper) &&
        (maskLower & dataLower) == (maskLower & patternLower);
}

bool NetUtil::wildcardMatch(const in_addr* data, const in_addr* pattern, std::uint32_t prefixLength)
{
    std::uint32_t* data_p = (std::uint32_t*) data;
    std::uint32_t* pattern_p = (std::uint32_t*) pattern;
    std::uint32_t mask = htobe32(prefixLengthToIpV4Mask(prefixLength));

    return (*data_p & mask) == (*pattern_p & mask);
}

bool NetUtil::isEven(in_addr ipV4)
{
    return (be32toh(ipV4.s_addr) & 1) == 0;
}

bool NetUtil::isEven(in6_addr const & ipV6)
{
    return (ipV6.s6_addr[15] & 1) == 0;
}
