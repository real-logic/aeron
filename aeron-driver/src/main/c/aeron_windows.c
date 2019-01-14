#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#include <WinSock2.h>
#include <Windows.h>
#endif

#include <time.h>

#include "aeron_windows.h"
#include "concurrent/aeron_thread.h"



#ifdef AERON_COMPILER_MSVC
void aeron_micro_sleep(size_t microseconds)
{
    aeron_nano_sleep(1000 * microseconds);
}
#endif

#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
    #include <intrin.h>
    #define __builtin_bswap32 _byteswap_ulong 
    #define __builtin_bswap64 _byteswap_uint64 
    #define __builtin_popcount __popcnt 
    #define __builtin_popcountll __popcnt64 


    typedef struct { UINT64 q[2]; } aeron_uint128_t;

    aeron_uint128_t make_aeron_uint128_t(UINT64 x)
    {
        aeron_uint128_t result;
        result.q[0] = x;
        result.q[1] = 0;
        return result;
    }

    aeron_uint128_t aeron_uint128_bitwise_negate(aeron_uint128_t x)
    {
        aeron_uint128_t r;
        r.q[0] = ~x.q[0];
        r.q[1] = ~x.q[1];
        return r;
    }

    BOOL aeron_uint128_equals(const aeron_uint128_t lhs, const aeron_uint128_t rhs)
    {
        return lhs.q[0] == rhs.q[0]
            && lhs.q[1] == rhs.q[1];
    }

    aeron_uint128_t aeron_uint128_bitwise_shift_left(const aeron_uint128_t lhs, size_t n)
    {
        aeron_uint128_t result = lhs;

        if (n >= 128)
        {
            result.q[1] = 0;
            result.q[0] = 0;
        }
        else
        {
            const unsigned int halfsize = 128 / 2;

            if (n >= halfsize) {
                n -= halfsize;
                result.q[1] = result.q[0];
                result.q[0] = 0;
            }

            if (n != 0) {
                // shift high half
                result.q[1] <<= n;

                const UINT64 mask = (~((UINT64)-1) >> n);

                // and add them to high half
                result.q[1] |= (result.q[0] & mask) >> (halfsize - n);

                // and finally shift also low half
                result.q[0] <<= n;
            }
        }

        return result;
    }

    aeron_uint128_t aeron_uint128_sub(const aeron_uint128_t lhs, const aeron_uint128_t rhs)
    {
        aeron_uint128_t result = lhs;

        result.q[1] -= rhs.q[1];
        result.q[0] -= rhs.q[0];

        if (rhs.q[0] >= lhs.q[0])
            result.q[1] -= 1;

        return result;
    }

    aeron_uint128_t aeron_uint128_bitwise_and(const aeron_uint128_t lhs, const aeron_uint128_t rhs)
    {
        aeron_uint128_t result;
        result.q[0] = lhs.q[0] & rhs.q[0];
        result.q[1] = lhs.q[1] & rhs.q[1];
        return result;
    }

    aeron_uint128_t aeron_ipv6_netmask_from_prefixlen(size_t prefixlen)
    {
        aeron_uint128_t netmask;

        if (0 == prefixlen)
        {
            netmask.q[1] = 0;
            netmask.q[0] = 0;
        }
        else
        {
            netmask = aeron_uint128_bitwise_negate(aeron_uint128_sub(aeron_uint128_bitwise_shift_left(make_aeron_uint128_t(1), (128 - prefixlen)), make_aeron_uint128_t(1)));
        }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        UINT64 lo = netmask.q[0];
        netmask.q[0] = __builtin_bswap64(netmask.q[1]);
        netmask.q[1] = __builtin_bswap64(lo);
#endif

        return netmask;
    }

    BOOL aeron_ipv6_does_prefix_match(struct in6_addr *in6_addr1, struct in6_addr *in6_addr2, size_t prefixlen)
    {
        aeron_uint128_t addr1;
        aeron_uint128_t addr2;
        const aeron_uint128_t netmask = aeron_ipv6_netmask_from_prefixlen(prefixlen);

        memcpy(&addr1, in6_addr1, sizeof(addr1));
        memcpy(&addr2, in6_addr2, sizeof(addr2));

        return aeron_uint128_equals(aeron_uint128_bitwise_and(addr1, netmask), aeron_uint128_bitwise_and(addr2, netmask));
    }

    void srand48(UINT64 aeron_nano_clock)
    {
        srand(aeron_nano_clock);
    }

    double drand48()
    {
        return rand();
    }

    double erand48(unsigned short xsubi[3])
    {
        return rand();
    }

    void localtime_r(const time_t *timep, struct tm *result)
    {
        localtime_s(result, timep);
    }
    
    #define MS_PER_SEC      1000ULL     // MS = milliseconds
    #define US_PER_MS       1000ULL     // US = microseconds
    #define HNS_PER_US      10ULL       // HNS = hundred-nanoseconds (e.g., 1 hns = 100 ns)
    #define NS_PER_US       1000ULL

    #define HNS_PER_SEC     (MS_PER_SEC * US_PER_MS * HNS_PER_US)
    #define NS_PER_HNS      (100ULL)    // NS = nanoseconds
    #define NS_PER_SEC      (MS_PER_SEC * US_PER_MS * NS_PER_US)

    int clock_gettime_monotonic(struct timespec *tv)
    {
        static LARGE_INTEGER ticksPerSec;
        LARGE_INTEGER ticks;

        if (!ticksPerSec.QuadPart) {
            QueryPerformanceFrequency(&ticksPerSec);
            if (!ticksPerSec.QuadPart) {
                errno = ENOTSUP;
                return -1;
            }
        }

        QueryPerformanceCounter(&ticks);

        double seconds = (double)ticks.QuadPart / (double)ticksPerSec.QuadPart;
        tv->tv_sec = (time_t)seconds;
        tv->tv_nsec = (long)((ULONGLONG)(seconds * NS_PER_SEC) % NS_PER_SEC);

        return 0;
    }

    int clock_gettime_realtime(struct timespec *tv)
    {
        FILETIME ft;
        ULARGE_INTEGER hnsTime;

        GetSystemTimeAsFileTime(&ft);

        hnsTime.LowPart = ft.dwLowDateTime;
        hnsTime.HighPart = ft.dwHighDateTime;

        hnsTime.QuadPart -= (11644473600ULL * HNS_PER_SEC);

        tv->tv_nsec = (long)((hnsTime.QuadPart % HNS_PER_SEC) * NS_PER_HNS);
        tv->tv_sec = (long)(hnsTime.QuadPart / HNS_PER_SEC);

        return 0;
    }

    int clock_gettime(clockid_t type, struct timespec *tp)
    {
        if (type == CLOCK_MONOTONIC_RAW)
            return clock_gettime_monotonic(tp);
        if (type == CLOCK_REALTIME)
            return clock_gettime_realtime(tp);

        errno = ENOTSUP;
        return -1;
    }

#endif
