#ifndef __MINTSYSTEM_PRIVATE_SEMAPHORE_MSVC_H__
#define __MINTSYSTEM_PRIVATE_SEMAPHORE_MSVC_H__

#ifdef __cplusplus
extern "C" {
#endif


typedef void mint_sem_t;

MINT_C_INLINE mint_sem_t *mint_sem_create()
{
    HANDLE hSema = CreateSemaphore(NULL, 0, INT32_MAX, NULL);
    return (mint_sem_t *) hSema;
}

MINT_C_INLINE int mint_sem_destroy(mint_sem_t *sem)
{
    CloseHandle((HANDLE) sem);
    return 0;
}

MINT_C_INLINE int mint_sem_post(mint_sem_t *sem)
{
    BOOL rc = ReleaseSemaphore((HANDLE) sem, 1, NULL);
    return (rc != 0) ? 0 : -1;
}

MINT_C_INLINE int mint_sem_wait(mint_sem_t *sem)
{
    DWORD rc = WaitForSingleObject((HANDLE) sem, INFINITE);
    return (rc == WAIT_OBJECT_0) ? 0 : -1;
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_SEMAPHORE_MSVC_H__
