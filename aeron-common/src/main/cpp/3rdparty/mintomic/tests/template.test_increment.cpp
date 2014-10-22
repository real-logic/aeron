#include <mintomic/mintomic.h>
#include <mintpack/timewaster.h>
#include <mintpack/threadsynchronizer.h>


static mint_atomic${TEST_INT_BITSIZE}_t g_sharedInt;

static void threadFunc(int threadNum)
{
    TimeWaster tw(threadNum);

    for (int i = 0; i < 10000000; i++)
    {
        mint_fetch_add_${TEST_INT_BITSIZE}_relaxed(&g_sharedInt, 1);
        tw.wasteRandomCycles();
    }
}

bool ${TEST_FUNC}(int numThreads)
{
    g_sharedInt._nonatomic = 0;
    ThreadSynchronizer threads(numThreads);
    threads.run(threadFunc);
    return g_sharedInt._nonatomic == (uint${TEST_INT_BITSIZE}_t) 10000000 * numThreads;
}
