#include <mintpack/threadsynchronizer.h>
#include <mintsystem/timer.h>
#include <mintomic/mintomic.h>


void* ThreadSynchronizer::threadStart(void* param)
{
    ThreadInfo* info = (ThreadInfo*) param;
    ThreadSynchronizer *synchronizer = info->parent;
    int threadNum = (int) (info - synchronizer->m_threadInfos);
    mint_timer_tick_t spinLimit = (mint_timer_tick_t) (0.00001 * mint_timer_secondsToTicks);

    for (;;)
    {
        mint_sem_wait(info->beginSema);
        if (synchronizer->m_mustExit)
            break;

        // This initial sleep(0) saves time on Xbox 360 when the main thread is on the same HW thread,
        // and there's more than one thread to kick. Effectively we yield CPU back to the main thread
        // before spinning.
        mint_sleep_millis(0);
        mint_timer_tick_t start = mint_timer_get();
        uint32_t newCount = mint_fetch_add_32_relaxed(&synchronizer->m_syncRemaining, -1);
        while (newCount > 0)
        {
            if (mint_timer_get() - start >= spinLimit)
            {
                mint_sleep_millis(0);
                start = mint_timer_get();
            }
            mint_yield_hw_thread();
            if (mint_load_32_relaxed(&synchronizer->m_syncRemaining) == 0)
                break;
        }

        start = mint_timer_get();
        if (synchronizer->m_threadFunc)
            synchronizer->m_threadFunc(threadNum);
        else if (synchronizer->m_threadParamFunc)
            synchronizer->m_threadParamFunc(threadNum, synchronizer->m_param);
        mint_timer_tick_t end = mint_timer_get();
        info->runningTime = end - start;

        mint_sem_post(synchronizer->m_endSema);
    }
    mint_sem_post(synchronizer->m_endSema);
    return NULL;
}

ThreadSynchronizer::ThreadSynchronizer(int numThreads)
{
    m_numThreads = numThreads;
    m_syncRemaining._nonatomic = 0;
    m_endSema = mint_sem_create();
    m_mustExit = false;
    m_threadInfos = new ThreadInfo[numThreads];
    for (int i = 0; i < numThreads; i++)
    {
        ThreadInfo* info = m_threadInfos + i;
        info->parent = this;
        info->beginSema = mint_sem_create();
        info->runningTime = 0;
        mint_thread_create(&info->thread, threadStart, info);
    }
    m_threadFunc = NULL;
    m_threadParamFunc = NULL;
}

ThreadSynchronizer::~ThreadSynchronizer()
{
    m_mustExit = true;
    kickThreads();
    for (int i = 0; i < m_numThreads; i++)
    {
        ThreadInfo* info = m_threadInfos + i;
        mint_thread_join(info->thread, NULL);
        mint_sem_destroy(info->beginSema);
    }
    delete[] m_threadInfos;
    mint_sem_destroy(m_endSema);
}

void ThreadSynchronizer::kickThreads()
{
    for (int i = 0; i < m_numThreads; i++)
        mint_sem_post(m_threadInfos[i].beginSema);
    for (int i = 0; i < m_numThreads; i++)
        mint_sem_wait(m_endSema);
}

void ThreadSynchronizer::run(void (*func)(int threadNum))
{
    m_threadFunc = func;
    m_syncRemaining._nonatomic = m_numThreads;
    kickThreads();
    m_threadFunc = NULL;
}

void ThreadSynchronizer::run(void (*func)(int threadNum, void*), void* param)
{
    m_threadParamFunc = func;
    m_param = param;
    m_syncRemaining._nonatomic = m_numThreads;
    kickThreads();
    m_threadParamFunc = NULL;
}

mint_timer_tick_t ThreadSynchronizer::getAverageThreadRunningTime() const
{
    mint_timer_tick_t total = 0;
    for (int t = 0; t < m_numThreads; t++)
        total += getThreadRunningTime(t);
    return total / m_numThreads;
}
