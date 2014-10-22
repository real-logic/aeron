#ifndef __MINTSYSTEM_PRIVATE_TID_GCC_H__
#define __MINTSYSTEM_PRIVATE_TID_GCC_H__

#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Thread IDs
//-------------------------------------
typedef size_t mint_tid_t;

MINT_C_INLINE mint_tid_t mint_get_current_thread_id()
{
    return (mint_tid_t) pthread_self();
}


//-------------------------------------
//  Process IDs
//-------------------------------------
typedef pid_t mint_pid_t;

MINT_C_INLINE mint_pid_t mint_get_current_process_id()
{
    return getpid();
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_TID_GCC_H__
