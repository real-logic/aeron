#include <mintomic/mintomic.h>
#include <mintpack/random.h>
#include <mintpack/timewaster.h>
#include <mintpack/threadsynchronizer.h>
#include <assert.h>
#include <string.h>


#define ELEMENT(index) ((index) >> ${TEST_INT_BITSHIFT})
#define BIT(index) ((uint${TEST_INT_BITSIZE}_t) 1 << ((index) & (${TEST_INT_BITSIZE} - 1)))

static int g_success;
static const int kDataBitSize = ${TEST_DATA_SIZE};
static mint_atomic${TEST_INT_BITSIZE}_t* g_data;
static int* g_indices;
static int g_numThreads;

static void threadFunc(int threadNum)
{
    // Decide upon index range and number of times to iterate
    int lo = kDataBitSize * threadNum / g_numThreads;
    int hi = kDataBitSize * (threadNum + 1) / g_numThreads;
    int times = 10000000 / kDataBitSize;

    TimeWaster tw(threadNum);
    for (int i = 0; i < times; i++)
    {
        for (int j = lo; j < hi; j++)
        {
            int index = g_indices[j];
            mint_fetch_or_${TEST_INT_BITSIZE}_relaxed(&g_data[ELEMENT(index)], BIT(index));
            tw.wasteRandomCycles();
        }
        for (int j = lo; j < hi; j++)
        {
            int index = g_indices[j];
            if ((g_data[ELEMENT(index)]._nonatomic & BIT(index)) == 0)
                g_success = 0;
        }
        for (int j = lo; j < hi; j++)
        {
            int index = g_indices[j];
            mint_fetch_and_${TEST_INT_BITSIZE}_relaxed(&g_data[ELEMENT(index)], ~BIT(index));
            tw.wasteRandomCycles();
        }
        for (int j = lo; j < hi; j++)
        {
            int index = g_indices[j];
            if ((g_data[ELEMENT(index)]._nonatomic & BIT(index)) != 0)
                g_success = 0;
        }
    }
}

bool ${TEST_FUNC}(int numThreads)
{
    g_success = 1;
    g_numThreads = numThreads;

    // Create bit array and clear it
    assert((kDataBitSize & 63) == 0);
    g_data = new mint_atomic${TEST_INT_BITSIZE}_t[kDataBitSize / ${TEST_INT_BITSIZE}];
    memset(g_data, 0, kDataBitSize / 8);

    // Create index array
    g_indices = new int[kDataBitSize];
    for (int i = 0; i < kDataBitSize; i++)
        g_indices[i] = i;

    // Shuffle the index array
    Random random;
    for (int i = 0; i < kDataBitSize; i++)
    {
        int swap = random.generate32() % (kDataBitSize - i);
        int temp = g_indices[i];
        g_indices[i] = g_indices[swap];
        g_indices[swap] = temp;
    }

    // Launch threads
    ThreadSynchronizer threads(numThreads);
    threads.run(threadFunc);

    // Clean up
    delete[] g_data;
    delete[] g_indices;

    return g_success != 0;
}

