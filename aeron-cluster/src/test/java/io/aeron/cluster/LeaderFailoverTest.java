/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.Aeron.NULL_VALUE;

@Ignore
public class LeaderFailoverTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int MEMBER_COUNT = 3;
    private static final String MSG = "Hello World!";

    private static final String CLUSTER_MEMBERS = clusterMembersString();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=256k|control-mode=manual|control=localhost:55550";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8020";

    private final AtomicLong timeOffset = new AtomicLong();
    private final EpochClock epochClock = () -> System.currentTimeMillis() + timeOffset.get();

    private final EchoService[] echoServices = new EchoService[MEMBER_COUNT];
    private final ClusteredMediaDriver[] clusteredMediaDrivers = new ClusteredMediaDriver[MEMBER_COUNT];
    private final ClusteredServiceContainer[] containers = new ClusteredServiceContainer[MEMBER_COUNT];
    private MediaDriver clientMediaDriver;
    private AeronCluster client;

    private final MutableInteger responseCount = new MutableInteger();
    private final EgressListener egressMessageListener =
        (clusterSessionId, timestamp, buffer, offset, length, header) -> responseCount.value++;
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private volatile long sentMessages = 0;
    private static final IdleStrategy IDLE_STRATEGY = new SleepingIdleStrategy(5_000L);

    @Before
    public void before()
    {
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            startNode(i);
        }
    }

    @After
    public void after() throws InterruptedException
    {
        executor.shutdownNow();

        CloseHelper.close(client);
        CloseHelper.close(clientMediaDriver);

        if (null != clientMediaDriver)
        {
            clientMediaDriver.context().deleteAeronDirectory();
        }

        for (final ClusteredServiceContainer container : containers)
        {
            CloseHelper.close(container);

            if (null != container)
            {
                container.context().deleteDirectory();
            }
        }

        for (final ClusteredMediaDriver driver : clusteredMediaDrivers)
        {
            CloseHelper.close(driver);

            if (null != driver)
            {
                driver.mediaDriver().context().deleteAeronDirectory();
                driver.consensusModule().context().deleteDirectory();
                driver.archive().context().deleteArchiveDirectory();
            }
        }

        if (!executor.awaitTermination(5, TimeUnit.SECONDS))
        {
            System.out.println("Warning: not all tasks completed promptly");
        }
    }

    @Test(timeout = 60_000)
    public void shouldFailOverToNewLeader() throws Exception
    {
        final Future<?> messageSender = executor.submit(this::sendMessages);
        int initialLeaderMemberId;
        while (NULL_VALUE == (initialLeaderMemberId = findLeaderId()))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        waitForMessageCount(15_000);

        containers[initialLeaderMemberId].close();
        clusteredMediaDrivers[initialLeaderMemberId].close();
        clusteredMediaDrivers[initialLeaderMemberId] = null;

        int electedLeaderMemberId;
        while (NULL_VALUE == (electedLeaderMemberId = findLeaderId()))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        waitForMessageCount(30_000);

        messageSender.cancel(true);
    }

    private void waitForMessageCount(final int requiredMessageCount) throws InterruptedException
    {
        while (true)
        {
            boolean serviceHasReceivedRequiredMessages = false;
            for (final EchoService echoService : echoServices)
            {
                serviceHasReceivedRequiredMessages |= echoService.messageCount >= requiredMessageCount;
            }

            if (serviceHasReceivedRequiredMessages && sentMessages >= requiredMessageCount)
            {
                return;
            }


            TestUtil.checkInterruptedStatus();
            Thread.sleep(100);
        }
    }

    private void startNode(final int index)
    {
        if (null == echoServices[index])
        {
            echoServices[index] = new EchoService(index);
        }
        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + index;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + index + "-driver";

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, index))
            .controlRequestStreamId(100 + index)
            .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, index))
            .controlResponseStreamId(110 + index)
            .aeronDirectoryName(baseDirName);

        clusteredMediaDrivers[index] = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .errorHandler(errorHandler("Driver error on node " + index))
                .spiesSimulateConnection(true)
                .sharedIdleStrategy(IDLE_STRATEGY)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(aeronDirName)
                .archiveDir(new File(baseDirName, "archive"))
                .controlChannel(archiveCtx.controlRequestChannel())
                .controlStreamId(archiveCtx.controlRequestStreamId())
                .localControlChannel("aeron:ipc?term-length=64k")
                .localControlStreamId(archiveCtx.controlRequestStreamId())
                .threadingMode(ArchiveThreadingMode.SHARED)
                .idleStrategySupplier(() -> IDLE_STRATEGY)
                .errorHandler(errorHandler("Archive error on node " + index))
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .epochClock(epochClock)
                .errorHandler(errorHandler("Consensus Module Error on node " + index))
                .clusterMemberId(index)
                .clusterMembers(CLUSTER_MEMBERS)
                .aeronDirectoryName(aeronDirName)
                .clusterDir(new File(baseDirName, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(memberSpecificPort(LOG_CHANNEL, index))
                .archiveContext(archiveCtx.clone())
                .idleStrategySupplier(() -> IDLE_STRATEGY)
                .deleteDirOnStart(true));

        containers[index] = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(archiveCtx.clone())
                .clusterDir(new File(baseDirName, "service"))
                .clusteredService(echoServices[index])
                .idleStrategySupplier(() -> IDLE_STRATEGY)
                .errorHandler(errorHandler("Error on node " + index))

        );
    }

    private void startClient()
    {
        if (client != null)
        {
            client.close();
            client = null;

        }
        if (clientMediaDriver != null)
        {
            clientMediaDriver.close();
            clientMediaDriver = null;
        }

        final String aeronDirName = CommonContext.getAeronDirectoryName();

        clientMediaDriver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .aeronDirectoryName(aeronDirName)
                .sharedIdleStrategy(IDLE_STRATEGY)
                .dirDeleteOnStart(true));

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressMessageListener)
                .aeronDirectoryName(aeronDirName)
                .ingressChannel("aeron:udp")
                .clusterMemberEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112"));
    }

    private void sendMessages()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        startClient();
        while (true)
        {
            long response = Publication.NOT_CONNECTED;
            while (client != null && (response = client.offer(msgBuffer, 0, MSG.length())) < 0)
            {
                if (response == Publication.CLOSED || response == Publication.NOT_CONNECTED)
                {
                    break;
                }

                TestUtil.checkInterruptedStatus();
                client.pollEgress();
                Thread.yield();
            }
            if (response == Publication.CLOSED || response == Publication.NOT_CONNECTED)
            {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
                try
                {
                    startClient();
                }
                catch (final TimeoutException e)
                {
                     // ignore - this could be because the new leader has not yet been elected
                }
            }
            else
            {
                sentMessages++;
            }
            if (client != null)
            {
                client.pollEgress();
            }
            TestUtil.checkInterruptedStatus();
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(1L));
        }
    }

    private static String memberSpecificPort(final String channel, final int memberId)
    {
        return channel.substring(0, channel.length() - 1) + memberId;
    }

    private static String clusterMembersString()
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            builder
                .append(i).append(',')
                .append("localhost:2011").append(i).append(',')
                .append("localhost:2022").append(i).append(',')
                .append("localhost:2033").append(i).append(',')
                .append("localhost:2044").append(i).append(',')
                .append("localhost:801").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static class EchoService extends StubClusteredService
    {
        private volatile int messageCount;
        private final int index;

        EchoService(final int index)
        {
            this.index = index;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            while (session.offer(buffer, offset, length) < 0)
            {
                cluster.idle();
            }

            ++messageCount;
        }
    }

    private int findLeaderId()
    {
        int leaderMemberId = NULL_VALUE;

        for (int i = 0; i < 3; i++)
        {

            final ClusteredMediaDriver driver = clusteredMediaDrivers[i];
            if (driver != null)
            {
                final Cluster.Role role = Cluster.Role.get(
                    (int)driver.consensusModule().context().clusterNodeCounter().get());

                if (Cluster.Role.LEADER == role)
                {
                    leaderMemberId = driver.consensusModule().context().clusterMemberId();
                }
            }
        }

        return leaderMemberId;
    }

    private static ErrorHandler errorHandler(final String header)
    {
        return e ->
        {
            System.err.println(header);
            e.printStackTrace();
        };
    }
}