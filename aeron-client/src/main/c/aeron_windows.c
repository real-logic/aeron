/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#include "aeron_windows.h"
#include "util/aeron_error.h"

#include <WinSock2.h>
#include <Windows.h>
#include <time.h>
#include <stdio.h>
#include <intrin.h>

#include "concurrent/aeron_thread.h"
#include "aeron_alloc.h"

#define __builtin_bswap32 _byteswap_ulong
#define __builtin_bswap64 _byteswap_uint64
#define __builtin_popcount __popcnt
#define __builtin_popcountll __popcnt64

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    switch (fdwReason)
    {
        case DLL_PROCESS_ATTACH:
            if (!aeron_error_dll_process_attach())
            {
                return FALSE;
            }
            break;

        case DLL_THREAD_DETACH:
            aeron_error_dll_thread_detach();
            break;

        case DLL_PROCESS_DETACH:
            aeron_error_dll_process_detach();
            break;

        default:
            break;
    }

    return TRUE;
}

void aeron_micro_sleep(size_t microseconds)
{
    aeron_nano_sleep(1000 * microseconds);
}

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
    return lhs.q[0] == rhs.q[0] && lhs.q[1] == rhs.q[1];
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

        if (n >= halfsize)
        {
            n -= halfsize;
            result.q[1] = result.q[0];
            result.q[0] = 0;
        }

        if (n != 0)
        {
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
    {
        result.q[1] -= 1;
    }

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
        netmask = aeron_uint128_bitwise_negate(aeron_uint128_sub(aeron_uint128_bitwise_shift_left(
            make_aeron_uint128_t(1), (128 - prefixlen)), make_aeron_uint128_t(1)));
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

void aeron_srand48(UINT64 aeron_nano_clock)
{
    srand((unsigned int)aeron_nano_clock);
}

double aeron_drand48()
{
    return rand() / (double)(RAND_MAX + 1);
}

double aeron_erand48(unsigned short xsubi[3])
{
    return rand() / (double)(RAND_MAX + 1);
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

int aeron_clock_gettime_monotonic(struct timespec *tv)
{
    static LARGE_INTEGER ticksPerSec;
    LARGE_INTEGER ticks;

    if (!ticksPerSec.QuadPart)
    {
        QueryPerformanceFrequency(&ticksPerSec);
        if (!ticksPerSec.QuadPart)
        {
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

int aeron_clock_gettime_realtime(struct timespec *tv)
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

char *aeron_strndup(const char *value, size_t length)
{
    size_t str_length = strlen(value);
    char *dup = NULL;

    str_length = (str_length > length) ? length : str_length;
    if (aeron_alloc((void **)&dup, str_length + 1) < 0)
    {
        errno = ENOMEM;
        return NULL;
    }

    strncpy(dup, value, str_length);
    dup[str_length] = '\0';
    return dup;
}

static char *optarg = 0;
static int optind = 1;
static int opterr = 1;
static int optopt = 0;

static int postpone_count = 0;
static int nextchar = 0;

struct option
{
    const char *name;
    int has_arg;
    int *flag;
    int val;
};

#define no_argument 0
#define required_argument 1
#define optional_argument 2

char *aeron_optarg_get()
{
    return optarg;
}

int *aeron_optind_get()
{
    return &optind;
}

static void postpone(int argc, char *const argv[], int index)
{
    char **nc_argv = (char **)argv;
    char *p = nc_argv[index];
    int j = index;
    for (; j < argc - 1; j++)
    {
        nc_argv[j] = nc_argv[j + 1];
    }
    nc_argv[argc - 1] = p;
}

static int postpone_noopt(int argc, char *const argv[], int index)
{
    int i = index;
    for (; i < argc; i++)
    {
        if (*(argv[i]) == '-')
        {
            postpone(argc, argv, index);
            return 1;
        }
    }
    return 0;
}

static int _getopt_(int argc, char *const argv[],
                    const char *optstring,
                    const struct option *longopts, int *longindex)
{
    while (1)
    {
        int c;
        const char *optptr = 0;
        if (optind >= argc - postpone_count)
        {
            c = 0;
            optarg = 0;
            break;
        }
        c = *(argv[optind] + nextchar);
        if (c == '\0')
        {
            nextchar = 0;
            ++optind;
            continue;
        }
        if (nextchar == 0)
        {
            if (optstring[0] != '+' && optstring[0] != '-')
            {
                while (c != '-')
                {
                    /* postpone non-opt parameter */
                    if (!postpone_noopt(argc, argv, optind))
                    {
                        break; /* all args are non-opt param */
                    }
                    ++postpone_count;
                    c = *argv[optind];
                }
            }
            if (c != '-')
            {
                if (optstring[0] == '-')
                {
                    optarg = argv[optind];
                    nextchar = 0;
                    ++optind;
                    return 1;
                }
                break;
            }
            else
            {
                if (strcmp(argv[optind], "--") == 0)
                {
                    optind++;
                    break;
                }
                ++nextchar;
                if (longopts != 0 && *(argv[optind] + 1) == '-')
                {
                    char const *spec_long = argv[optind] + 2;
                    char const *pos_eq = strchr(spec_long, '=');
                    size_t spec_len = (pos_eq == NULL ? strlen(spec_long) : pos_eq - spec_long);
                    int index_search = 0;
                    int index_found = -1;
                    const struct option *optdef = 0;
                    while (longopts->name != 0)
                    {
                        if (strncmp(spec_long, longopts->name, spec_len) == 0)
                        {
                            if (optdef != 0)
                            {
                                if (opterr)
                                {
                                    fprintf(stderr, "ambiguous option: %s\n", spec_long);
                                }
                                return '?';
                            }
                            optdef = longopts;
                            index_found = index_search;
                        }
                        longopts++;
                        index_search++;
                    }
                    if (optdef == 0)
                    {
                        if (opterr)
                        {
                            fprintf(stderr, "no such a option: %s\n", spec_long);
                        }
                        return '?';
                    }
                    switch (optdef->has_arg)
                    {
                    case no_argument:
                        optarg = 0;
                        if (pos_eq != 0)
                        {
                            if (opterr)
                            {
                                fprintf(stderr, "no argument for %s\n", optdef->name);
                            }
                            return '?';
                        }
                        break;
                    case required_argument:
                        if (pos_eq == NULL)
                        {
                            ++optind;
                            optarg = argv[optind];
                        }
                        else
                        {
                            optarg = (char *)pos_eq + 1;
                        }
                        break;
                    }
                    ++optind;
                    nextchar = 0;
                    if (longindex != 0)
                    {
                        *longindex = index_found;
                    }
                    if (optdef->flag != 0)
                    {
                        *optdef->flag = optdef->val;
                        return 0;
                    }
                    return optdef->val;
                }
                continue;
            }
        }
        optptr = strchr(optstring, c);
        if (optptr == NULL)
        {
            optopt = c;
            if (opterr)
            {
                fprintf(stderr,
                        "%s: invalid option -- %c\n",
                        argv[0], c);
            }
            ++nextchar;
            return '?';
        }
        if (*(optptr + 1) != ':')
        {
            nextchar++;
            if (*(argv[optind] + nextchar) == '\0')
            {
                ++optind;
                nextchar = 0;
            }
            optarg = 0;
        }
        else
        {
            nextchar++;
            if (*(argv[optind] + nextchar) != '\0')
            {
                optarg = argv[optind] + nextchar;
            }
            else
            {
                ++optind;
                if (optind < argc - postpone_count)
                {
                    optarg = argv[optind];
                }
                else
                {
                    optopt = c;
                    if (opterr)
                    {
                        fprintf(stderr,
                                "%s: option requires an argument -- %c\n",
                                argv[0], c);
                    }
                    if (optstring[0] == ':' ||
                        (optstring[0] == '-' || optstring[0] == '+') &&
                            optstring[1] == ':')
                    {
                        c = ':';
                    }
                    else
                    {
                        c = '?';
                    }
                }
            }
            ++optind;
            nextchar = 0;
        }
        return c;
    }

    /* end of option analysis */

    /* fix the order of non-opt params to original */
    while ((argc - optind - postpone_count) > 0)
    {
        postpone(argc, argv, optind);
        ++postpone_count;
    }

    nextchar = 0;
    postpone_count = 0;
    return -1;
}

int aeron_getopt(int argc, char *const argv[], const char *optstring)
{
    return _getopt_(argc, argv, optstring, 0, 0);
}

#else

typedef int aeron_make_into_non_empty_translation_unit_t;

#endif
