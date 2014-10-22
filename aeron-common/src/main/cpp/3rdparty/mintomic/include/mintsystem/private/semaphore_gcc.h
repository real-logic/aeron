#ifndef __MINTSYSTEM_PRIVATE_SEMAPHORE_GCC_H__
#define __MINTSYSTEM_PRIVATE_SEMAPHORE_GCC_H__

#include <semaphore.h>

#ifdef __cplusplus
extern "C" {
#endif


typedef struct _mint_sem_t mint_sem_t;

#define MINT_SEM_FAILED ((mint_sem_t *) SEM_FAILED)

mint_sem_t *mint_sem_create();
int mint_sem_destroy(mint_sem_t *sem);
int mint_sem_wait(mint_sem_t *sem);

MINT_C_INLINE int mint_sem_post(mint_sem_t *sem)
{
    return sem_post((sem_t *) sem);
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_SEMAPHORE_GCC_H__
