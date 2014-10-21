#include <mintsystem/semaphore.h>
#include <errno.h>

#if MINT_IS_APPLE
    #include <mintomic/mintomic.h>
    #include <time.h>
    #include <unistd.h>
    #include <stdio.h>
#else
    #include <stdlib.h>
#endif


mint_sem_t *mint_sem_create()
{
#if MINT_IS_APPLE
    // On Apple platforms, sem_init() is unimplemented!
    // This is true at least up to iOS SDK 6.0 and MacOSX SDK 10.8.
    // The only way to create an unnamed semaphore is by jumping through these hoops.
    // We can use sem_open(), but if two processes open the same name, they will
    // clash. Worse (at least on iOS), if one process crashes during debugging, its named semaphores will remain
    // present in the kernel, and all future processes will pick up the existing semaphore if
    // they open using the same name! To avoid these possibilities, we generate a unique name,
    // and immediately call sem_unlink() on it to remove it from the kernel namespace.
    static mint_atomic32_t shared_sem_number = { 0 };
    char name[256];
    sem_t *sem;
    int32_t sem_number = mint_fetch_add_32_relaxed(&shared_sem_number, 1);
    sprintf(name, "mint_sem_%u_%u_%u", (uint32_t) getpid(), (uint32_t) time(NULL), sem_number);
    sem = sem_open(name, O_CREAT, 0644, 0);
    if (sem != SEM_FAILED)
        sem_unlink(name);
    return (mint_sem_t *) sem;
#else
    int rc;
    sem_t *sem = (sem_t *) malloc(sizeof(sem_t));
    if (!sem)
        return MINT_SEM_FAILED;
    rc = sem_init(sem, 0, 0);
    if (rc == -1)
    {
        int y = errno;
        free(sem);
        return MINT_SEM_FAILED;
    }
    return (mint_sem_t *) sem;
#endif
}

int mint_sem_destroy(mint_sem_t *sem)
{
#if MINT_IS_APPLE
    return sem_close((sem_t *) sem);
#else
    int rc = sem_destroy((sem_t *) sem);
    free(sem);
    return rc;
#endif
}

int mint_sem_wait(mint_sem_t *sem)
{
    int rc;
    do
    {
        // When debugging on iOS 6.0 with Xcode 4.5, breaking into the debugger can cause
        // sem_wait() to return a failure code with errno set to EINTR, leading to synchronization
        // bugs in the calling code, which typically doesn't check for EINTR.
        // Indeed, this is reported to happen on Linux too:
        // http://stackoverflow.com/questions/2013181/gdb-causes-sem-wait-to-fail-with-eintr-error
        // Let's assume the caller does not want to take any action as a result of EINTR,
        // and consume all such errors here.
        rc = sem_wait((sem_t *) sem);
    } while (rc == -1 && errno == EINTR);
    return rc;
}
