#include <mintomic/mintomic.h>
#include <mintsystem/thread.h>
#include <assert.h>
#include <malloc.h>


struct mint_thread_msvc_t
{
    HANDLE hThread;
    void *(*start_routine) (void *);
    void *arg_or_retval;
};

DWORD WINAPI mint_thread_entry_point(LPVOID lpParameter)
{
    mint_thread_t thread = (mint_thread_t) lpParameter;
    thread->arg_or_retval = thread->start_routine(thread->arg_or_retval);
    return 0;
}

int mint_thread_create(mint_thread_t *thread, void *(*start_routine) (void *), void *arg)
{
#if MINT_TARGET_XBOX_360
    static mint_atomic32_t sharedHwThreadCounter = { 0 };
    uint32_t hwThread;
#endif
    mint_thread_t t = (mint_thread_t) malloc(sizeof(struct mint_thread_msvc_t));
    t->start_routine = start_routine;
    t->arg_or_retval = arg;
    t->hThread = CreateThread(NULL, 0, mint_thread_entry_point, t, 0, NULL);
#if MINT_TARGET_XBOX_360
    // A quick, hacky way to get threads running on separate cores on Xbox 360,
    // postponing the need to implement a portable thread affinity module in MintSystem.
    // As threads are created, we simply assign them to HW threads in a cycle: 0, 2, 4, 1, 3, 5, in that order.
    // This helps the Mintomic sample programs demonstrate useful things without any extra thread creation code.
    hwThread = ((uint32_t) mint_fetch_add_32_relaxed(&sharedHwThreadCounter, 1) * 2) % 12;
    XSetThreadProcessor(t->hThread, hwThread >= 6 ? hwThread - 5 : hwThread);
#endif
    *thread = t;
    return (t->hThread != NULL) ? 0 : -1;
}

int mint_thread_join(mint_thread_t thread, void **retval)
{
    DWORD rc = WaitForSingleObject(thread->hThread, INFINITE);
    CloseHandle(thread->hThread);
    if (retval)
        *retval = thread->arg_or_retval;
    free(thread);
    return (rc == WAIT_OBJECT_0) ? 0 : -1;
}
