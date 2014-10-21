#ifndef __MINTSYSTEM_PRIVATE_MUTEX_MSVC_H__
#define __MINTSYSTEM_PRIVATE_MUTEX_MSVC_H__

#ifdef __cplusplus
extern "C" {
#endif


typedef CRITICAL_SECTION mint_mutex_t;

#define MINT_MUTEX_EBUSY 16

MINT_C_INLINE int mint_mutex_init(mint_mutex_t *mutex)
{
    InitializeCriticalSection(mutex);
    return 0;
}

MINT_C_INLINE int mint_mutex_destroy(mint_mutex_t *mutex)
{
    DeleteCriticalSection(mutex);
    return 0;
}

MINT_C_INLINE int mint_mutex_lock(mint_mutex_t *mutex)
{
    EnterCriticalSection(mutex);
    return 0;
}

MINT_C_INLINE int mint_mutex_trylock(mint_mutex_t *mutex)
{
    return TryEnterCriticalSection(mutex) ? 0 : MINT_MUTEX_EBUSY;
}

MINT_C_INLINE int mint_mutex_unlock(mint_mutex_t *mutex)
{
    LeaveCriticalSection(mutex);
    return 0;
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_MUTEX_MSVC_H__
