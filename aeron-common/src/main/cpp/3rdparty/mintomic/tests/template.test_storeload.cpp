// This test is based on the blog post: http://preshing.com/20120515/memory-reordering-caught-in-the-act

#include <mintomic/mintomic.h>
#include <mintpack/random.h>
#include <mintpack/threadsynchronizer.h>
#include <assert.h>


#cmakedefine01 TEST_FORCE_FAIL

static Random g_random[2];
static int X, Y;
static int r1, r2;

static void threadFunc(int threadNum)
{
    if (threadNum == 0)
    {
        while (g_random[0].generate32() % 8 != 0) {}  // Random delay
        X = 1;
#if TEST_FORCE_FAIL
        mint_signal_fence_seq_cst();  // Prevent compiler reordering only
#else
        mint_thread_fence_seq_cst();  // Prevent CPU reordering
#endif
        r1 = Y;
    }
    else
    {
        while (g_random[1].generate32() % 8 != 0) {}  // Random delay
        Y = 1;
#if TEST_FORCE_FAIL
        mint_signal_fence_seq_cst();  // Prevent compiler reordering only
#else
        mint_thread_fence_seq_cst();  // Prevent CPU reordering
#endif
        r2 = X;
    }
}

bool ${TEST_FUNC}(int numThreads)
{
    assert(numThreads == 2);
    ThreadSynchronizer threads(2);
    for (int i = 0; i < 100000; i++)
    {
        X = Y = 0;
        threads.run(threadFunc);
        if (r1 == 0 && r2 == 0)
            return false;
    }
    return true;
}
