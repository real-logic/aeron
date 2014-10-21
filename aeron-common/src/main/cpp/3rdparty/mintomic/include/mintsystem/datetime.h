#ifndef __MINTSYSTEM_PRIVATE_DATETIME_MSVC_H__
#define __MINTSYSTEM_PRIVATE_DATETIME_MSVC_H__

#include <mintomic/core.h>

#ifdef __cplusplus
extern "C" {
#endif


// Number of microseconds since January 1, 1601 in Coordinated Universal Time.
uint64_t mint_get_current_utc_time();


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_DATETIME_MSVC_H__
