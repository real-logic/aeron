#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#include <filesystem>
#endif

#include <chrono>
#include <regex>
#include <thread>
#include <time.h>


extern "C"
{
    #include "aeron_windows.h"

    void aeron_nano_sleep(size_t nanoseconds = 1)
    {
        std::this_thread::sleep_for(std::chrono::nanoseconds(nanoseconds));
    }

    void aeron_micro_sleep(size_t microseconds)
    {
        std::this_thread::sleep_for(std::chrono::microseconds(microseconds));
    }

    #if defined(AERON_COMPILER_MSVC) 
    int aeron_delete_directory(const char* directory)
    {
        std::error_code e;
        if (std::filesystem::remove_all(directory, e) == std::uintmax_t(-1))
            return -1;
        return 0;    
    }

    int aeron_is_directory(const char* path)
    {
        return std::filesystem::is_directory(path);        
    }
    #endif

#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
    #include <intrin.h>
    #define __builtin_bswap32 _byteswap_ulong 
    #define __builtin_bswap64 _byteswap_uint64 
    #define __builtin_popcount __popcnt 
    #define __builtin_popcountll __popcnt64 

    void localtime_r(const time_t *timep, struct tm *result)
    {
        localtime_s(result, timep);
    }

    double erand48(unsigned short xsubi[3])
    {
        return rand();
    }

    struct __uint128_t
    {
        uint64_t q[2];

        __uint128_t()
        {
            q[0] = 0;
            q[1] = 0;
        }

        __uint128_t(std::uint64_t input)
        {
            hi() = 0;
            lo() = input;
        }

        __uint128_t operator~()
        {
            q[0] = ~q[0];
            q[1] = ~q[1];
            return *this;
        }

        uint64_t& hi()
        {
            return q[1];

        }

        uint64_t& lo()
        {
            return q[0];
        }

        uint64_t hi() const
        {
            return q[1];

        }

        uint64_t lo() const
        {
            return q[0];
        }
    };

    __uint128_t operator<<(const __uint128_t& lhs, size_t n)
    {
        __uint128_t result = lhs;

        if (n >= 128)
        {
            result.hi() = 0;
            result.lo() = 0;
        }
        else
        {
            const unsigned int halfsize = 128 / 2;

            if (n >= halfsize) {
                n -= halfsize;
                result.hi() = result.lo();
                result.lo() = 0;
            }

            if (n != 0) {
                // shift high half
                result.hi() <<= n;

                const uint64_t mask(~(uint64_t(-1) >> n));

                // and add them to high half
                result.hi() |= (result.lo() & mask) >> (halfsize - n);

                // and finally shift also low half
                result.lo() <<= n;
            }
        }

        return result;
    }

    __uint128_t operator-(const __uint128_t& lhs, const __uint128_t& rhs)
    {
        __uint128_t result = lhs;

        result.hi() -= rhs.hi();
        result.lo() -= rhs.lo();

        if (rhs.lo() >= lhs.lo())
            result.hi() -= 1;

        return result;
    }

    __uint128_t aeron_ipv6_netmask_from_prefixlen(size_t prefixlen)
    {
        __uint128_t netmask;

        if (0 == prefixlen)
        {
            netmask.hi() = 0;
            netmask.lo() = 0;
        }
        else
        {
            netmask = ~((__uint128_t(1) << (128 - prefixlen)) - __uint128_t(1));
        }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        uint64_t lo = netmask.lo();
        netmask.lo() = __builtin_bswap64(netmask.hi());
        netmask.hi() = __builtin_bswap64(lo);
#endif

        return netmask;
    }

    bool equals(const __uint128_t& lhs, const __uint128_t& rhs)
    {
        return lhs.q[0] == rhs.q[0]
            && lhs.q[1] == rhs.q[1];
    }

    __uint128_t operator&(const __uint128_t& lhs, const __uint128_t& rhs)
    {
        __uint128_t result;
        result.q[0] = lhs.q[0] & rhs.q[0];
        result.q[1] = lhs.q[1] & rhs.q[1];
        return result;
    }

    bool aeron_ipv6_does_prefix_match(struct in6_addr *in6_addr1, struct in6_addr *in6_addr2, size_t prefixlen)
    {
        __uint128_t addr1;
        __uint128_t addr2;
        __uint128_t netmask = aeron_ipv6_netmask_from_prefixlen(prefixlen);

        memcpy(&addr1, in6_addr1, sizeof(addr1));
        memcpy(&addr2, in6_addr2, sizeof(addr2));

        return equals((addr1 & netmask), (addr2 & netmask));
    }

    void srand48(int64_t aeron_nano_clock)
    {
        srand(aeron_nano_clock);
    }

    double drand48()
    {
        return rand();
    }

    int clock_gettime_monotonic(struct timespec *tv)
    {        
        auto now = std::chrono::steady_clock{}.now().time_since_epoch();
        auto s = std::chrono::duration_cast<std::chrono::seconds>(now);
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - s);

        tv->tv_sec = s.count();
        tv->tv_nsec = ns.count();

        return 0;
    }

    int clock_gettime_realtime(struct timespec *tv)
    {
        auto now = std::chrono::system_clock{}.now().time_since_epoch();
        auto s = std::chrono::duration_cast<std::chrono::seconds>(now);
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - s);

        tv->tv_sec = s.count();
        tv->tv_nsec = ns.count();

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
}
