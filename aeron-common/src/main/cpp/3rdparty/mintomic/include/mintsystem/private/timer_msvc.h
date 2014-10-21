#ifndef __MINTSYSTEM_PRIVATE_TIMER_MSVC_H__
#define __MINTSYSTEM_PRIVATE_TIMER_MSVC_H__

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
    LARGE_INTEGER now;
    QueryPerformanceCounter(&now);
    return now.QuadPart;
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
    YieldProcessor();
}

MINT_C_INLINE void mint_sleep_millis(int millis)
{
    Sleep(millis);
}



#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_TIMER_MSVC_H__
