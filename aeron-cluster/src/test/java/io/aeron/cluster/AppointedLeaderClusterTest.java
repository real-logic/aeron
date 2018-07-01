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

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.SessionMessageListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.junit.*;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AppointedLeaderClusterTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int MEMBER_COUNT = 3;
    private static final int MESSAGE_COUNT = 1000;
    private static final String MSG = "Hello World!";

    private static final String CLUSTER_MEMBERS = clusterMembersString();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=64k|control-mode=manual|control=localhost:55550";
    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
        "aeron:udp?term-length=64k|endpoint=localhost:8020";

    private final CountDownLatch latch = new CountDownLatch(MEMBER_COUNT);
    private final EchoService[] echoServices = new EchoService[MEMBER_COUNT];
    private ClusteredMediaDriver[] drivers = new ClusteredMediaDriver[MEMBER_COUNT];
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
            echoServices[i] = new EchoService(latch);

            final String baseDirName = aeronDirName + "-" + i;

            final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, i))
                .controlRequestStreamId(100 + i)
                .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, i))
                .controlResponseStreamId(110 + i)
                .aeronDirectoryName(baseDirName);

            drivers[i] = ClusteredMediaDriver.launch(
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
                    .errorHandler(Throwable::printStackTrace)
                    .clusterMemberId(i)
                    .clusterMembers(CLUSTER_MEMBERS)
                    .appointedLeaderId(0)
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

        for (final ClusteredMediaDriver driver : drivers)
        {
            CloseHelper.close(driver);

            if (null != driver)
            {
                driver.mediaDriver().context().deleteAeronDirectory();
            }
        }
    }

    @Test(timeout = 10_000)
    public void shouldConnectAndSendKeepAlive()
    {
        assertTrue(client.sendKeepAlive());
    }

    @Test(timeout = 10_000)
    public void shouldEchoMessagesViaService() throws Exception
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        msgBuffer.putStringWithoutLengthAscii(0, MSG);

        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            final long msgCorrelationId = client.nextCorrelationId();
            while (client.offer(msgCorrelationId, msgBuffer, 0, MSG.length()) < 0)
            {
                TestUtil.checkInterruptedStatus();
                Thread.yield();
            }

            client.pollEgress();
        }

        while (responseCount.get() < MESSAGE_COUNT)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
            client.pollEgress();
        }

        latch.await();

        assertThat(responseCount.get(), is(MESSAGE_COUNT));
        for (final EchoService service : echoServices)
        {
            assertThat(service.messageCount(), is(MESSAGE_COUNT));
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
        private int messageCount;
        private final CountDownLatch latch;

        EchoService(final CountDownLatch latch)
        {
            this.latch = latch;
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

            if (++messageCount == MESSAGE_COUNT)
            {
                latch.countDown();
            }
        }
    }
}
