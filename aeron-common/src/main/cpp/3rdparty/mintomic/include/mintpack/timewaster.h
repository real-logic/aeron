#ifndef __MINTPACK_TIME_WASTER_H__
#define __MINTPACK_TIME_WASTER_H__

#include <mintomic/core.h>


//-------------------------------------
//  TimeWaster
//-------------------------------------
class TimeWaster
{
private:
    static const uint32_t kArraySize = 65521;
    static uint32_t g_randomValues[kArraySize];

    uint32_t m_pos;
    uint32_t m_step;

public:
    static void Initialize();

    TimeWaster(int32_t seed = -1);
    void wasteRandomCycles();
};


#endif // __MINTPACK_TIME_WASTER_H__
