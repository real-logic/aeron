// The "test_load_store_64_fail" version of this test is designed to fail on 32-bit CPUs,
// demonstrating that inconsistent values can be seen if loads/stores are not atomic.

#include <mintomic/mintomic.h>
#include <mintpack/threadsynchronizer.h>
#include <assert.h>


#cmakedefine01 TEST_FORCE_FAIL

static mint_atomic${TEST_INT_BITSIZE}_t g_sharedInt;
static bool g_success;

#if ${TEST_INT_BITSIZE} == 32

    // These values all satisfy x * x >= g_limit:
    static uint32_t g_values[] =
    {
        0x37b91364u,
        0x1c5970efu,
        0x536c76bau,
        0x0a10207fu,
        0x71043c77u,
        0x4db84a83u,
        0x27cf0273u,
        0x74a15a69u,
    };
    static uint32_t g_limit = 0xffffff00u;

#elif ${TEST_INT_BITSIZE} == 64

    // These values all satisfy x * x >= g_limit:
    static uint64_t g_values[] =
    {
        0x3c116d2362a21633ull,
        0x7747508552ab6bc6ull,
        0x289a0e1528a43422ull,
        0x15e36d0a61d326eaull,
        0x3ccb2e8c0c6224c4ull,
        0x074504c13a1716e1ull,
        0x6c82417a3ad77b24ull,
        0x3124440040454919ull,
    };
    static uint64_t g_limit = 0xffffff0000000000ull;

#endif

static void threadFunc(int threadNum)
{
    uint${TEST_INT_BITSIZE}_t value;

    if (threadNum == 0)
    {
        uint32_t index = 0;
        for (int i = 0; i < 10000000; i++)
        {
            index = (index * index + 1) % 65521;            // Next pseudorandom index
            value = g_values[index & 7];                    // Value to store
#if TEST_FORCE_FAIL
            g_sharedInt._nonatomic = value;                 // Nonatomic store
#else
            mint_store_${TEST_INT_BITSIZE}_relaxed(&g_sharedInt, value);     // Atomic store
#endif
            // Issue a compiler barrier to prevent hoisting the store out of the loop
            mint_signal_fence_release();
        }
    }
    else
    {
        for (int i = 0; i < 10000000; i++)
        {
            // Issue a compiler barrier to prevent hoisting the load out of the loop
            mint_signal_fence_acquire();
#if TEST_FORCE_FAIL
            value = g_sharedInt._nonatomic;                 // Nonatomic load
#else
            value = mint_load_${TEST_INT_BITSIZE}_relaxed(&g_sharedInt);     // Atomic load
#endif
            if (value * value < g_limit)
                g_success = false;                          // Invalid value loaded!
        }
    }
}

bool ${TEST_FUNC}(int numThreads)
{
    assert(numThreads == 2);
    g_sharedInt._nonatomic = g_values[0];   // Initial value must be valid
    g_success = true;
    ThreadSynchronizer threads(2);
    threads.run(threadFunc);
    return g_success;
}
