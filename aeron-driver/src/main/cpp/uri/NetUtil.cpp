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

//bool NetUtil::wildcardMatch(struct in6_addr* data, struct in6_addr* pattern)
//{
//    return false;
//}

uint32_t prefixLengthToIpV4Mask(uint32_t subnetPrefix)
{
    return 0 == subnetPrefix ? 0 : ~((1 << (32 - subnetPrefix)) - UINT32_C(1));
}


bool NetUtil::wildcardMatch(in_addr* data, in_addr* pattern, std::uint32_t prefixLength)
{
    std::uint32_t* data_p = (std::uint32_t*) data;
    std::uint32_t* pattern_p = (std::uint32_t*) pattern;
    std::uint32_t mask = htobe32(prefixLengthToIpV4Mask(prefixLength));

    return (*data_p & mask) == (*pattern_p & mask);
}
