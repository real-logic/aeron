/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.SessionMessageListener;
import io.aeron.cluster.service.*;
import io.aeron.driver.*;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.junit.*;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.Aeron.NULL_VALUE;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ClusterTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int MEMBER_COUNT = 3;
    private static final int MESSAGE_COUNT = 10;
    private static final String MSG = "Hello World!";

    private static final String CLUSTER_MEMBERS = clusterMembersString();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=64k|control-mode=manual|control=localhost:55550";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8020";

    private final AtomicLong timeOffset = new AtomicLong();
    private final EpochClock epochClock = () -> System.currentTimeMillis() + timeOffset.get();

    private final CountDownLatch latchOne = new CountDownLatch(MEMBER_COUNT);
    private final CountDownLatch latchTwo = new CountDownLatch(MEMBER_COUNT - 1);

    private final EchoService[] echoServices = new EchoService[MEMBER_COUNT];
    private ClusteredMediaDriver[] clusteredMediaDrivers = new ClusteredMediaDriver[MEMBER_COUNT];
    private ClusteredServiceContainer[] containers = new ClusteredServiceContainer[MEMBER_COUNT];
    private MediaDriver clientMediaDriver;
    private AeronCluster client;

    private final MutableInteger responseCount = new MutableInteger();
    private final SessionMessageListener sessionMessageListener =
        (correlationId, clusterSessionId, timestamp, buffer, offset, length, header) -> responseCount.value++;

    @Before
    public void before()
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            echoServices[i] = new EchoService(i, latchOne, latchTwo);

            final String baseDirName = aeronDirName + "-" + i;

            final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, i))
                .controlRequestStreamId(100 + i)
                .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, i))
                .controlResponseStreamId(110 + i)
                .aeronDirectoryName(baseDirName);

            clusteredMediaDrivers[i] = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                    .aeronDirectoryName(baseDirName)
                    .threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true)
                    .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                    .errorHandler(Throwable::printStackTrace)
                    .dirDeleteOnStart(true),
                new Archive.Context()
                    .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                    .aeronDirectoryName(baseDirName)
                    .archiveDir(new File(baseDirName, "archive"))
                    .controlChannel(archiveCtx.controlRequestChannel())
                    .controlStreamId(archiveCtx.controlRequestStreamId())
                    .localControlChannel("aeron:ipc?term-length=64k")
                    .localControlStreamId(archiveCtx.controlRequestStreamId())
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .deleteArchiveOnStart(true),
                new ConsensusModule.Context()
                    .epochClock(epochClock)
                    .errorHandler(Throwable::printStackTrace)
                    .clusterMemberId(i)
                    .clusterMembers(CLUSTER_MEMBERS)
                    .aeronDirectoryName(baseDirName)
                    .clusterDir(new File(baseDirName, "consensus-module"))
                    .ingressChannel("aeron:udp?term-length=64k")
                    .logChannel(memberSpecificPort(LOG_CHANNEL, i))
                    .archiveContext(archiveCtx.clone())
                    .deleteDirOnStart(true));

            containers[i] = ClusteredServiceContainer.launch(
                new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(baseDirName)
                    .archiveContext(archiveCtx.clone())
                    .clusterDir(new File(baseDirName, "service"))
                    .clusteredService(echoServices[i])
                    .errorHandler(Throwable::printStackTrace));
        }

        clientMediaDriver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .aeronDirectoryName(aeronDirName));

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .sessionMessageListener(sessionMessageListener)
                .aeronDirectoryName(aeronDirName)
                .ingressChannel("aeron:udp")
                .clusterMemberEndpoints("0=localhost:20110,1=localhost:20111,2=localhost:20112"));
    }

    @After
    public void after()
    {
        CloseHelper.close(client);
        CloseHelper.close(clientMediaDriver);

        if (null != clientMediaDriver)
        {
            clientMediaDriver.context().deleteAeronDirectory();
        }

        for (final ClusteredServiceContainer container : containers)
        {
            CloseHelper.close(container);
        }

        for (final ClusteredMediaDriver driver : clusteredMediaDrivers)
        {
            CloseHelper.close(driver);

            if (null != driver)
            {
                driver.mediaDriver().context().deleteAeronDirectory();
            }
        }
    }

    @Ignore
    @Test(timeout = 30_000)
    public void shouldEchoMessagesThenContinueOnNewLeader() throws Exception
    {
        final int leaderMemberId = findLeaderId(NULL_VALUE);
        assertThat(leaderMemberId, not(NULL_VALUE));

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT);

        latchOne.await();

        assertThat(client.leaderMemberId(), is(leaderMemberId));
        assertThat(responseCount.get(), is(MESSAGE_COUNT));
        for (final EchoService service : echoServices)
        {
            assertThat(service.messageCount(), is(MESSAGE_COUNT));
        }

        containers[leaderMemberId].close();
        clusteredMediaDrivers[leaderMemberId].close();

        int newLeaderMemberId;
        while (NULL_VALUE == (newLeaderMemberId = findLeaderId(leaderMemberId)))
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1000);
        }

        assertThat(newLeaderMemberId, not(leaderMemberId));

        sendMessages(msgBuffer);
        awaitResponses(MESSAGE_COUNT * 2);
        assertThat(client.leaderMemberId(), is(newLeaderMemberId));

        latchTwo.await();
        assertThat(responseCount.get(), is(MESSAGE_COUNT * 2));
        for (final EchoService service : echoServices)
        {
            if (service.index() != leaderMemberId)
            {
                assertThat(service.messageCount(), is(MESSAGE_COUNT * 2));
            }
        }
    }

    private void sendMessages(final ExpandableArrayBuffer msgBuffer)
    {
        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            final long msgCorrelationId = client.nextCorrelationId();
            while (client.offer(msgCorrelationId, msgBuffer, 0, MSG.length()) < 0)
            {
                TestUtil.checkInterruptedStatus();
                client.pollEgress();
                Thread.yield();
            }

            client.pollEgress();
        }
    }

    private void awaitResponses(final int messageCount)
    {
        while (responseCount.get() < messageCount)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
            client.pollEgress();
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
        private final CountDownLatch latchOne;
        private final CountDownLatch latchTwo;

        EchoService(final int index, final CountDownLatch latchOne, final CountDownLatch latchTwo)
        {
            this.index = index;
            this.latchOne = latchOne;
            this.latchTwo = latchTwo;
        }

        int index()
        {
            return index;
        }

        int messageCount()
        {
            return messageCount;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long correlationId,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            while (session.offer(correlationId, buffer, offset, length) < 0)
            {
                cluster.idle();
            }

            ++messageCount;

            if (messageCount == MESSAGE_COUNT)
            {
                latchOne.countDown();
            }

            if (messageCount == (MESSAGE_COUNT * 2))
            {
                latchTwo.countDown();
            }
        }
    }

    private int findLeaderId(final int skipMemberId)
    {
        int leaderMemberId = NULL_VALUE;

        for (int i = 0; i < 3; i++)
        {
            if (i == skipMemberId)
            {
                continue;
            }

            final ClusteredMediaDriver driver = clusteredMediaDrivers[i];

            final Cluster.Role role = Cluster.Role.get(
                (int)driver.consensusModule().context().clusterNodeCounter().get());

            if (Cluster.Role.LEADER == role)
            {
                leaderMemberId = driver.consensusModule().context().clusterMemberId();
            }
        }

        return leaderMemberId;
    }
}
