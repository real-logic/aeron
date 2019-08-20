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

import io.aeron.cluster.service.Cluster;
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

            Thread.sleep(10_00);
            assertNull("no election in progress", follower.electionState());
            assertNull("no election in progress", follower2.electionState());

            System.out.println("start loss");
            shouldDrop.set(true);
            Thread.sleep(10_000);

            assertThat("loss between leader and follower - election state should be CANVASS",
                follower.electionState(), is(Election.State.CANVASS));
            assertNull(follower2.electionState());

            Thread.sleep(10_000);

            System.out.println("end loss");
            shouldDrop.set(false);
            Thread.sleep(15_000);

            assertNull("no election in progress", follower2.electionState());
            // this is the issue instead of -1 (null) election state is Election.State.FOLLOWER_CATCHUP_TRANSITION
            assertNull("no election in progress", follower.electionState());

            System.out.println("success end test");
//            return;
            new ShutdownSignalBarrier().await();

        }
    }

    @Test
    public void shouldRecoverQuicklyAfterKillingFollowersThenRestarting() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(-1))
        {
            cluster.awaitLeader();
            final TestNode leader = cluster.findLeader();
            final TestNode follower = cluster.followers().get(0);
            final TestNode follower2 = cluster.followers().get(1);

            // without client, it's ok, only 8 sec to get the leader...
            cluster.connectClient();
            cluster.sendMessages(10);

            cluster.stopNode(follower);
            cluster.stopNode(follower2);

            while (leader.role() != Cluster.Role.FOLLOWER)
            {
                System.out.println("leader role " + leader.role());
                Thread.sleep(5_000);
            }

            System.out.println("cluster is down");

            System.out.println("restart 2nd follower");
            cluster.startStaticNode(follower2.index(), true);

            System.out.println("awaiting leader");
            final long start = System.currentTimeMillis();
            cluster.awaitLeader();
            final long end = System.currentTimeMillis();
            System.out.println("time to get leader again " + (end - start));

            System.out.println("new leader " + cluster.findLeader().index());
        }
    }
}
