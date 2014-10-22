#include <mintsystem/timer.h>


double mint_timer_secondsToTicks = 0;
double mint_timer_ticksToSeconds = 0;

double mint_timer_getSecondsToTicks()
{
#if MINT_IS_APPLE
    mach_timebase_info_data_t info;
    mach_timebase_info(&info);
    return 1e9 * info.denom / info.numer;
#else
    return 1e9;
#endif
}

void mint_timer_initialize()
{
    mint_timer_secondsToTicks = mint_timer_getSecondsToTicks();
    mint_timer_ticksToSeconds = 1.0 / mint_timer_secondsToTicks;
}
