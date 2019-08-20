package io.aeron.cluster;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.concurrent.ShutdownSignalBarrier;
import org.junit.Test;

import io.aeron.driver.ext.LossGenerator;

public class ResilienceTest
{
    static int LEADER_ID = 0;
    static int FOLLOWER_ID = 1;

    @Test
    public void shouldGoBackToNormalStateAfterLoss() throws Exception
    {
        final AtomicBoolean shouldDrop = new AtomicBoolean(false);

        Function<Supplier<TestNode>, LossGenerator> funcLoss = (node) -> (LossGenerator) (address, buffer, length) -> {
            List<Integer> followerPorts = new ArrayList<>();
            if (node.get() != null && address != null)
            {
                String follower = node.get().getClusterMembers().split("\\|")[FOLLOWER_ID];
                Matcher matcher = Pattern.compile(":(\\d+),*").matcher(follower);
                while (matcher.find())
                {
                    followerPorts.add(Integer.valueOf(matcher.group(1)));
                }

                return node.get().isLeader()
                    && followerPorts.contains(address.getPort())
                    && shouldDrop.get();
            }
            return false;
        };

        try (TestCluster cluster = TestCluster.startThreeNodeStaticClusterWithLossGenerator(LEADER_ID, funcLoss))
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);
            TestNode follower2 = cluster.followers().get(1);

            cluster.connectClient();
            cluster.sendMessages(100);

            Thread.sleep(5000);
            assertNull("no election in progress", follower.electionState());
            assertNull("no election in progress", follower2.electionState());

            System.out.println("start loss");
            shouldDrop.set(true);
            Thread.sleep(20000);

            assertThat("loss between leader and follower - election state should be CANVASS",
                follower.electionState(), is(Election.State.CANVASS));
            assertNull(follower2.electionState());

            System.out.println("send more messages");
            cluster.sendMessages(100);

            Thread.sleep(20000);

            System.out.println("end loss");
            shouldDrop.set(false);
            Thread.sleep(20000);

            assertNull("no election in progress", follower2.electionState());
            // this is the issue instead of -1 (null) election state is Election.State.FOLLOWER_CATCHUP_TRANSITION
            assertNull("no election in progress", follower.electionState());

            System.out.println("success end test");
//            return;
            new ShutdownSignalBarrier().await();

        }
    }
}
