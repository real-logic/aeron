#include <stdio.h>
#include <mintpack/timewaster.h>
#include <mintsystem/timer.h>


typedef bool TestEntryFunc(int numThreads);

struct TestInfo
{
    const char* description;
    TestEntryFunc* func;
    int numThreads;
    bool allowFailure;
};

${TEST_ENTRY_FUNCS}

TestInfo g_testInfos[] =
{
${TEST_INFOS}
};

struct TestGroup
{
    int numTests;
    int numPassed;

    TestGroup()
    {
        numTests = 0;
        numPassed = 0;
    }
    void addResult(bool success)
    {
        if (success)
            numPassed++;
        numTests++;
    }
};

int main()
{
    mint_timer_initialize();
    TimeWaster::Initialize();

    TestGroup required, optional;
    int numTests = sizeof(g_testInfos) / sizeof(g_testInfos[0]);
    for (int i = 0; i < numTests; i++)
    {
        TestInfo& info = g_testInfos[i];
        printf("[%d/%d] Test \"%s\"...", i + 1, numTests, info.description);

        mint_timer_tick_t start = mint_timer_get();
        bool success = info.func(info.numThreads);
        mint_timer_tick_t end = mint_timer_get();

        const char* status = success ? "pass" : info.allowFailure ? "fail (allowed)" : "*** FAIL ***";
        printf(" %s, %.3f ms\n", status, (end - start) * mint_timer_ticksToSeconds * 1000);

        TestGroup& group = info.allowFailure ? optional : required;
        group.addResult(success);
    }

    printf("\n----- Summary: -----\n");
    printf("Out of the tests required to pass, %d/%d passed.\n", required.numPassed, required.numTests);
    printf("Out of the tests allowed to fail, %d/%d passed.\n", optional.numPassed, optional.numTests);

    return (required.numPassed == required.numTests) ? 0 : 1;
}
