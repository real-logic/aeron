#ifndef __MINTSYSTEM_PRIVATE_MUTEX_GCC_H__
#define __MINTSYSTEM_PRIVATE_MUTEX_GCC_H__

#include <pthread.h>
#include <errno.h>

#ifdef __cplusplus
extern "C" {
#endif


typedef pthread_mutex_t mint_mutex_t;

#define MINT_MUTEX_EBUSY EBUSY

int mint_mutex_init(mint_mutex_t *mutex);

MINT_C_INLINE int mint_mutex_destroy(mint_mutex_t *mutex)
{
    return pthread_mutex_destroy(mutex);
}

MINT_C_INLINE int mint_mutex_lock(mint_mutex_t *mutex)
{
    return pthread_mutex_lock(mutex);
}

MINT_C_INLINE int mint_mutex_trylock(mint_mutex_t *mutex)
{
    return pthread_mutex_trylock(mutex);
}

MINT_C_INLINE int mint_mutex_unlock(mint_mutex_t *mutex)
{
    return pthread_mutex_unlock(mutex);
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_MUTEX_GCC_H__
