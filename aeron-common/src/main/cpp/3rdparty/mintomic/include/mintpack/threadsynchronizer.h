#ifndef __MINTPACK_THREAD_SYNCHRONIZER_H__
#define __MINTPACK_THREAD_SYNCHRONIZER_H__

#include <mintsystem/thread.h>
#include <mintsystem/semaphore.h>
#include <mintsystem/timer.h>
#include <mintomic/mintomic.h>


//-------------------------------------
//  ThreadSynchronizer
//-------------------------------------
class ThreadSynchronizer
{
private:
    struct ThreadInfo
    {
        mint_thread_t thread;
        ThreadSynchronizer *parent;
        mint_sem_t *beginSema;
        mint_timer_tick_t runningTime;
    };
    int m_numThreads;
    ThreadInfo* m_threadInfos;
    mint_atomic32_t m_syncRemaining;
    mint_sem_t* m_endSema;

    static void* threadStart(void* param);
    bool m_mustExit;
    void (*m_threadFunc)(int);
    void (*m_threadParamFunc)(int, void*);
    void* m_param;
    void kickThreads();

public:
    ThreadSynchronizer(int numThreads);
    ~ThreadSynchronizer();
    void run(void (*func)(int));
    void run(void (*func)(int, void*), void* param);
    mint_timer_tick_t getThreadRunningTime(int num) const { return m_threadInfos[num].runningTime; }
    mint_timer_tick_t getAverageThreadRunningTime() const;
};


#endif // __MINTPACK_THREAD_SYNCHRONIZER_H__
