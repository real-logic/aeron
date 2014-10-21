#include <mintomic/mintomic.h>
#include <mintpack/threadsynchronizer.h>


struct Node
{
    Node* next;
};

static mint_atomicPtr_t g_head;

static void threadFunc(int threadNum)
{
    for (int i = 0; i < 2000000; i++)
    {
        Node* insert = new Node;
        Node* h;
        do
        {
            h = (Node*) mint_load_ptr_relaxed(&g_head);
            insert->next = h;
        }
        while (mint_compare_exchange_strong_ptr_relaxed(&g_head, h, insert) != h);
    }
}

bool ${TEST_FUNC}(int numThreads)
{
    g_head._nonatomic = NULL;
    ThreadSynchronizer threads(numThreads);
    threads.run(threadFunc);
    int count = 0;
    while (g_head._nonatomic)
    {
        Node* t = (Node*) g_head._nonatomic;
        g_head._nonatomic = t->next;
        // If this loop runs too slowly in Windows,
        // make sure to set the environment variable _NO_DEBUG_HEAP to 1,
        // or run without the debugger attached.
        delete t;
        count++;
    }
    return count == 2000000 * numThreads;
}
