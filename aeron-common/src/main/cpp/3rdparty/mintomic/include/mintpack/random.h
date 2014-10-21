#ifndef __MINTPACK_RANDOM_H__
#define __MINTPACK_RANDOM_H__

#include <mintomic/mintomic.h>


//-------------------------------------
//  PRNG that seeds itself using various information from the environment.
//  generate32() is uniformly distributed across all 32-bit integer values.
//  generateUnique32() returns unique integers 2^32 times in a row, then repeats the sequence.
//-------------------------------------
class Random
{
private:
    static const int kNumOffsets = 8;
    static mint_atomic32_t m_sharedCounter;
    uint32_t m_value;
    uint32_t m_offsets[kNumOffsets];

public:
    Random();
    uint32_t generate32();
    uint32_t generateUnique32();
    uint64_t generate64()
    {
        return (((uint64_t) generate32()) << 32) | generate32();
    }
};


#endif // __MINTPACK_RANDOM_H__
