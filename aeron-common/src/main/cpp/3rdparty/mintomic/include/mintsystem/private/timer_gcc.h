#ifndef __MINTSYSTEM_PRIVATE_TIMER_GCC_H__
#define __MINTSYSTEM_PRIVATE_TIMER_GCC_H__

#include <stddef.h>
#include <time.h>
#if MINT_IS_APPLE
    #include <mach/mach_time.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Timers
//-------------------------------------
typedef uint64_t mint_timer_tick_t;

// These are initialized by mint_timer_initialize:
extern double mint_timer_ticksToSeconds;
extern double mint_timer_secondsToTicks;

void mint_timer_initialize();
double mint_timer_getSecondsToTicks();    // Can be called before mint_timer_initialize

MINT_C_INLINE int mint_timer_is_initialized()
{
    return mint_timer_secondsToTicks != 0;
}

MINT_C_INLINE mint_timer_tick_t mint_timer_get()
{
#if MINT_IS_APPLE
    return mach_absolute_time();
#else
    struct timespec tick;
    clock_gettime(CLOCK_MONOTONIC, &tick);
    return (uint64_t) tick.tv_sec * 1000000000ull + tick.tv_nsec;
#endif
}

MINT_C_INLINE int mint_timer_greater_or_equal(mint_timer_tick_t a, mint_timer_tick_t b)
{
    return (int64_t) (a - b) >= 0;
}


//-------------------------------------
//  Sleep
//-------------------------------------
MINT_C_INLINE void mint_yield_hw_thread()
{
    // Only implemented on x86/64
#if MINT_CPU_X86 || MINT_CPU_X64
    asm volatile("pause");
#endif
}

MINT_C_INLINE void mint_sleep_millis(int millis)
{
    struct timespec ts;
    ts.tv_sec = millis / 1000;
    ts.tv_nsec = (millis % 1000) * 1000;
    nanosleep(&ts, NULL);
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_TIMER_GCC_H__
