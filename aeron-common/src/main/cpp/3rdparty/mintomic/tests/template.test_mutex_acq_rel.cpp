// This test is based on the blog post: http://preshing.com/20121019/this-is-why-they-call-it-a-weakly-ordered-cpu

#include <mintomic/mintomic.h>
#include <mintpack/timewaster.h>
#include <mintpack/threadsynchronizer.h>
#include <assert.h>
#include <stdio.h>


#cmakedefine01 TEST_FORCE_FAIL

static TimeWaster g_timeWasters[4];
static mint_atomic${TEST_INT_BITSIZE}_t g_flag;
static uint${TEST_INT_BITSIZE}_t g_sharedValue;

static void threadFunc(int threadNum)
{
    TimeWaster &tw = g_timeWasters[threadNum];

    int count = 0;
    while (count < 5000000)
    {
        tw.wasteRandomCycles();
        if (mint_compare_exchange_strong_${TEST_INT_BITSIZE}_relaxed(&g_flag, 0, 1) == 0)
        {
            // Lock was successful
#if !TEST_FORCE_FAIL
            mint_thread_fence_acquire();
#endif
            g_sharedValue++;
#if !TEST_FORCE_FAIL
            mint_thread_fence_release();
#endif
            mint_store_${TEST_INT_BITSIZE}_relaxed(&g_flag, 0);
            count++;
        }
    }
}

bool ${TEST_FUNC}(int numThreads)
{
    assert(numThreads <= 4);

    g_flag._nonatomic = 0;
    g_sharedValue = 0;
    ThreadSynchronizer threads(numThreads);
    threads.run(threadFunc);
    bool success = (g_sharedValue == (uint${TEST_INT_BITSIZE}_t) 5000000 * numThreads);
    if (!success)
        printf(" g_sharedValue=%llu", (uint64_t) g_sharedValue);
    return success;
}
