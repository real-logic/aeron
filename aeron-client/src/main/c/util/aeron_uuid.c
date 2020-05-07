#include <stdint.h>
#include <stdlib.h> /* srand, rand */
#include <time.h>
#include <string.h>

#include "aeron_uuid.h"

#include "randombytes.h"

/*
 * Note that RFC4122 defines UUID in more details:
 *
 *     Field               Data Type     Octet  Note
 * -------------------------------------------------
 *  time_low               unsigned 32   0-3    The low field of the
 *                         bit integer          timestamp
 *
 *  time_mid               unsigned 16   4-5    The middle field of the
 *                         bit integer          timestamp
 *
 *  time_hi_and_version    unsigned 16   6-7    The high field of the
 *                         bit integer          timestamp multiplexed
 *                                              with the version number
 *
 *  clock_seq_hi_and_rese  unsigned 8    8      The high field of the
 *  rved                   bit integer          clock sequence
 *                                              multiplexed with the
 *                                              variant
 *
 *  clock_seq_low          unsigned 8    9      The low field of the
 *                         bit integer          clock sequence
 *
 *  node                   unsigned 48   10-15  The spatially unique
 *                         bit integer          node identifier
 *
 * We have clock_seq_hi_and_reserved (8bit) and clock_seq_low (8bit)
 * merged into clock_seq (16bit).
 */
struct uuid
{
    uint32_t time_low;
    uint16_t time_mid;
    uint16_t time_hi_and_version;
    uint16_t clock_seq;
    uint8_t node[6];
};

void uuid_unpack(const aeron_uuid_t in, struct uuid *uu)
{
    const uint8_t *ptr = in;
    uint32_t tmp;

    tmp = *ptr++;
    tmp = (tmp << 8) | *ptr++;
    tmp = (tmp << 8) | *ptr++;
    tmp = (tmp << 8) | *ptr++;
    uu->time_low = tmp;

    tmp = *ptr++;
    tmp = (tmp << 8) | *ptr++;
    uu->time_mid = tmp;

    tmp = *ptr++;
    tmp = (tmp << 8) | *ptr++;
    uu->time_hi_and_version = tmp;

    tmp = *ptr++;
    tmp = (tmp << 8) | *ptr++;
    uu->clock_seq = tmp;

    memcpy(uu->node, ptr, 6);
}

void uuid_pack(const struct uuid *uu, aeron_uuid_t ptr)
{
    uint32_t tmp;
    unsigned char *out = ptr;

    tmp = uu->time_low;
    out[3] = (unsigned char)tmp;
    tmp >>= 8;
    out[2] = (unsigned char)tmp;
    tmp >>= 8;
    out[1] = (unsigned char)tmp;
    tmp >>= 8;
    out[0] = (unsigned char)tmp;

    tmp = uu->time_mid;
    out[5] = (unsigned char)tmp;
    tmp >>= 8;
    out[4] = (unsigned char)tmp;

    tmp = uu->time_hi_and_version;
    out[7] = (unsigned char)tmp;
    tmp >>= 8;
    out[6] = (unsigned char)tmp;

    tmp = uu->clock_seq;
    out[9] = (unsigned char)tmp;
    tmp >>= 8;
    out[8] = (unsigned char)tmp;

    memcpy(out + 10, uu->node, 6);
}

static uint64_t xorshift128plus(uint64_t *s)
{
    /* http://xorshift.di.unimi.it/xorshift128plus.c */
    uint64_t s1 = s[0];
    const uint64_t s0 = s[1];
    s[0] = s0;
    s1 ^= s1 << 23;
    s[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
    return s[1] + s0;
}

int aeron_random_init()
{
    unsigned int seed;
    if (randombytes(&seed, sizeof(seed)) != 0)
    {
        return -1;
    }
    srand(seed);
    return 0;
}

void aeron_randombytes(void *buf, size_t n)
{
    if (randombytes(buf, n) == 0)
    {
        return;
    }

    for (size_t i = 0; i < n; i += n)
    {
        int32_t val = rand();
        int8_t *vals = (int8_t *)&val;
        int8_t shortVal = 0;
        for (int j = 0; j < 4; j += 1)
        {
            shortVal = shortVal ^ vals[i];
        }
        ((int8_t *)buf)[i] = shortVal;
    }
}

void aeron_random_128bit(void *vals)
{
    aeron_randombytes(vals, 16);
}

int32_t aeron_randomised_int32()
{
    int32_t vals[4];
    aeron_random_128bit((int64_t *)vals);
    int32_t result = 0;
    int i;
    for (i = 0; i < 4; i += 1)
    {
        result = result ^ vals[i];
    }
    return result;
}

int aeron_uuid_generate(aeron_uuid_t out)
{
    aeron_uuid_t buf;
    struct uuid uu;

    uint64_t vals[2];
    if (randombytes(vals, sizeof(vals)) != 0)
    {
        return -1;
    }
    memcpy(buf, vals, sizeof(buf));

    uuid_unpack(buf, &uu);

    uu.clock_seq = (uu.clock_seq & 0x3FFF) | 0x8000;
    uu.time_hi_and_version = (uu.time_hi_and_version & 0x0FFF) | 0x4000;
    uuid_pack(&uu, out);

    return 0;
}
