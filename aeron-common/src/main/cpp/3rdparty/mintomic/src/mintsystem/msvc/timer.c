#include <mintsystem/timer.h>


double mint_timer_ticksToSeconds = 0;
double mint_timer_secondsToTicks = 0;

double mint_timer_getSecondsToTicks()
{
    LARGE_INTEGER freq;
    QueryPerformanceFrequency(&freq);
    return (double) freq.QuadPart;
}

void mint_timer_initialize()
{
    mint_timer_secondsToTicks = mint_timer_getSecondsToTicks();
    mint_timer_ticksToSeconds = 1.0 / mint_timer_secondsToTicks;
}
