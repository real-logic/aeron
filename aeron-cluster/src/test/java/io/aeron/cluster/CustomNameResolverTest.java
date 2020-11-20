package io.aeron.cluster;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class CustomNameResolverTest
{

    private TestClusterWithNameResolver cluster;

    @AfterEach
    void tearDown()
    {
        CloseHelper.close(cluster);
    }

    @Test
    void test(final TestInfo testInfo)
    {
        final int messageCount = 300;
        cluster = TestClusterWithNameResolver.startThreeNodeStaticCluster();
        try
        {
            cluster.awaitLeader();
            cluster.connectClient();

            final int iterations = 30000;
            for (int i = 0; i < iterations; i++)
            {
                cluster.sendMessages(messageCount);
                final int expectedMessageCount = (i * messageCount) + messageCount;
                cluster.awaitResponseMessageCount(expectedMessageCount);
                // System.out.println("sent " + expectedMessageCount + "/" + (iterations * messageCount));
            }

        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo);
            LangUtil.rethrowUnchecked(ex);
        }
    }
}

