#ifndef __MINTOMIC_PRIVATE_STDINT_H__
#define __MINTOMIC_PRIVATE_STDINT_H__


typedef signed char int8_t;
typedef unsigned char uint8_t;
#define INT8_MIN (-0x7f - 1)
#define INT8_MAX 0x7f
#define UINT8_MAX 0xff

typedef short int16_t;
typedef unsigned short uint16_t;
#define INT16_MIN (-0x7fff - 1)
#define INT16_MAX 0x7fff
#define UINT16_MAX 0xffff

typedef int int32_t;
typedef unsigned int uint32_t;
#define INT32_MIN (-0x7fffffff - 1)
#define INT32_MAX 0x7fffffff
#define UINT32_MAX 0xffffffff

typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#define INT64_MIN (-0x7fffffffffffffff - 1)
#define INT64_MAX 0x7fffffffffffffff
#define UINT64_MAX 0xffffffffffffffffu


#endif // __MINTOMIC_PRIVATE_STDINT_H__
