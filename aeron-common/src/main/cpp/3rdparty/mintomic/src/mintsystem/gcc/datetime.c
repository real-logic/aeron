#include <mintsystem/datetime.h>
#if _POSIX_TIMERS
#include <time.h>
#else
#include <sys/time.h>
#endif

uint64_t mint_get_current_utc_time()
{
#if _POSIX_TIMERS
    struct timespec tick;
    clock_gettime(CLOCK_REALTIME, &tick);
    return (uint64_t) tick.tv_sec * 1000000ull + tick.tv_nsec / 1000 + 11644473600000000ull;
#else
    struct timeval tick;
    gettimeofday(&tick, NULL);
    return (uint64_t) tick.tv_sec * 1000000ull + tick.tv_usec + 11644473600000000ull;
#endif
}

