#ifndef __MINTSYSTEM_PRIVATE_THREAD_GCC_H__
#define __MINTSYSTEM_PRIVATE_THREAD_GCC_H__

#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif


typedef pthread_t mint_thread_t;

MINT_C_INLINE int mint_thread_create(mint_thread_t *thread, void *(*start_routine) (void *), void *arg)
{
    return pthread_create(thread, NULL, start_routine, arg);
}

MINT_C_INLINE int mint_thread_join(mint_thread_t thread, void **retval)
{
    return pthread_join(thread, retval);
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_THREAD_GCC_H__
