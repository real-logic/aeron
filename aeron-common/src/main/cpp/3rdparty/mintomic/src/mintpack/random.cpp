#include <mintpack/random.h>
#include <mintsystem/datetime.h>
#include <mintsystem/timer.h>
#include <mintsystem/tid.h>
#include <assert.h>


mint_atomic32_t Random::m_sharedCounter = { 0 };

//-------------------------------------------------------------------------
static uint32_t getNoiseFromTimer()
{
    // How many ticks are in 10 microseconds?
    mint_timer_tick_t interval = (mint_timer_tick_t) (0.00001 * mint_timer_getSecondsToTicks());
    if (interval == 0)
        interval = 1;

    // When is the end of the current 10 microsecond interval?
    mint_timer_tick_t start = mint_timer_get();
    mint_timer_tick_t remaining = interval - start % interval;
    mint_timer_tick_t end = start + remaining;

    // Now let's accumulate timer values into a counter until we get there.
    uint32_t accum = 0;
    mint_timer_tick_t current;
    do
    {
        current = mint_timer_get();
        accum += (uint32_t) current;
    }
    while (!mint_timer_greater_or_equal(current, end));
    return accum;
}

//-------------------------------------------------------------------------
static inline uint32_t permuteQuadraticResidue(uint32_t x)
{
    static const uint32_t prime = 4294967291u;
    if (x >= prime)
        return x;  // The 5 integers out of range are mapped to themselves.
    uint32_t residue = (uint32_t) (((uint64_t) x * x) % prime);
    return (x <= prime / 2) ? residue : prime - residue;
}

//-------------------------------------------------------------------------
static inline uint32_t permuteWithOffset(uint32_t x, uint32_t offset)
{
    return (permuteQuadraticResidue(x) + offset) ^ 0x5bf03635;
}

//-------------------------------------------------------------------------
Random::Random()
{
    m_value = 0;

    // Try to initialize a set of values that are unique each time Random::Random() is called:
    m_offsets[0] = getNoiseFromTimer();
    m_offsets[1] = mint_fetch_add_32_relaxed(&m_sharedCounter, 1);
    uint64_t time = mint_get_current_utc_time();
    m_offsets[2] = (uint32_t) (time >> 32);
    m_offsets[3] = (uint32_t) time;
    m_offsets[4] = (uint32_t) mint_get_current_thread_id();
    m_offsets[5] = (uint32_t) mint_get_current_process_id();
    m_offsets[6] = getNoiseFromTimer();
    uint64_t tick = (uint64_t) mint_timer_get();
    m_offsets[7] = (uint32_t) (tick ^ (tick >> 32));
    assert(kNumOffsets == 8);

    // Now shuffle them up a bit
    uint32_t ofs = 0;
    for (int i = 0; i < 4; i++)
    {
        for (int j = 0; j < kNumOffsets; j++)
        {
            ofs = permuteWithOffset(ofs, m_offsets[j]);
            m_offsets[j] = ofs;
        }
    }
}

//-------------------------------------------------------------------------
uint32_t Random::generate32()
{
    uint32_t x = m_value;
    uint32_t carry = 1; // Used to alter the internal state.

    for (int i = 0; i < kNumOffsets; i++)
    {
        x = permuteWithOffset(x, m_offsets[i]);
        if (carry > 0)
            carry = ++m_offsets[i] ? 0 : 1;
    }

    m_value = x;
    return x;
}

//-------------------------------------------------------------------------
uint32_t Random::generateUnique32()
{
    uint32_t x = m_value;
    m_value++;

    for (int i = 0; i < kNumOffsets; i++)
    {
        x = permuteWithOffset(x, m_offsets[i]);
    }

    return x;
}
