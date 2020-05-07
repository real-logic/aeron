#ifndef AERON_UUID_H
#define AERON_UUID_H

#include <stdint.h>

typedef unsigned char aeron_uuid_t[16];

int aeron_random_init();
int32_t aeron_randomised_int32();
int aeron_uuid_generate(aeron_uuid_t out);

#endif