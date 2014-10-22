#include <mintomic/mintomic.h>
#include <mintpack/timewaster.h>
#include <stdlib.h>
#include <assert.h>


uint32_t TimeWaster::g_randomValues[kArraySize];

void TimeWaster::Initialize()
{
    for (uint32_t i = 0; i < kArraySize; i++)
        g_randomValues[i] = rand();
}

TimeWaster::TimeWaster(int32_t seed) : m_pos(0)
{
    uint32_t s = seed;
    if (seed < 0)
    {
        static mint_atomic32_t sharedSeed = { 0 };
        s = mint_fetch_add_32_relaxed(&sharedSeed, 1);
    }
    m_step = s % (kArraySize - 1) + 1;
}

void TimeWaster::wasteRandomCycles()
{
    uint32_t i;
    do
    {
        i = g_randomValues[m_pos];
        m_pos += m_step;
        if (m_pos >= kArraySize)
            m_pos -= kArraySize;
    }
    while (i & 7);
}
