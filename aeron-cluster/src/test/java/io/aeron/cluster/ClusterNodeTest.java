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

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.SessionMessageListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ClusterNodeTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    @Before
    public void before()
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .errorHandler(Throwable::printStackTrace)
                .deleteDirOnStart(true));
    }

    @After
    public void after()
    {
        CloseHelper.close(aeronCluster);
        CloseHelper.close(container);
        CloseHelper.close(clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteArchiveDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
        }
    }

    @Test
    public void shouldConnectAndSendKeepAlive()
    {
        container = launchEchoService();
        aeronCluster = connectToCluster(null);

        assertTrue(aeronCluster.sendKeepAlive());
    }

    @Test(timeout = 10_000)
    public void shouldEchoMessageViaService()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableLong msgCorrelationId = new MutableLong();
        final MutableInteger messageCount = new MutableInteger();

        final SessionMessageListener listener =
            (correlationId, clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertThat(correlationId, is(msgCorrelationId.value));
                assertThat(buffer.getStringWithoutLengthAscii(offset, length), is(msg));

                messageCount.value += 1;
            };

        container = launchEchoService();
        aeronCluster = connectToCluster(listener);

        msgCorrelationId.value = aeronCluster.nextCorrelationId();

        while (aeronCluster.offer(msgCorrelationId.value, msgBuffer, 0, msg.length()) < 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        while (messageCount.get() == 0)
        {
            if (aeronCluster.pollEgress() <= 0)
            {
                TestUtil.checkInterruptedStatus();
                Thread.yield();
            }
        }
    }

    @Test(timeout = 10_000)
    public void shouldScheduleEventInService()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableLong msgCorrelationId = new MutableLong();
        final MutableInteger messageCount = new MutableInteger();

        final SessionMessageListener listener =
            (correlationId, clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertThat(correlationId, is(msgCorrelationId.value));
                assertThat(buffer.getStringWithoutLengthAscii(offset, length), is(msg + "-scheduled"));

                messageCount.value += 1;
            };

        container = launchTimedService();
        aeronCluster = connectToCluster(listener);

        msgCorrelationId.value = aeronCluster.nextCorrelationId();

        while (aeronCluster.offer(msgCorrelationId.value, msgBuffer, 0, msg.length()) < 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        while (messageCount.get() == 0)
        {
            if (aeronCluster.pollEgress() <= 0)
            {
                TestUtil.checkInterruptedStatus();
                Thread.yield();
            }
        }
    }

    private ClusteredServiceContainer launchEchoService()
    {
        final ClusteredService echoService = new StubClusteredService()
        {
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
                    TestUtil.checkInterruptedStatus();
                    Thread.yield();
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(echoService)
                .errorHandler(Throwable::printStackTrace));
    }

    private ClusteredServiceContainer launchTimedService()
    {
        final ClusteredService timedService = new StubClusteredService()
        {
            long clusterSessionId;
            long correlationId;
            String msg;

            public void onSessionMessage(
                final ClientSession session,
                final long correlationId,
                final long timestampMs,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                this.clusterSessionId = session.id();
                this.correlationId = correlationId;
                this.msg = buffer.getStringWithoutLengthAscii(offset, length);

                while (!cluster.scheduleTimer(correlationId, timestampMs + 100))
                {
                    cluster.idle();
                }
            }

            public void onTimerEvent(final long correlationId, final long timestampMs)
            {
                final String responseMsg = msg + "-scheduled";
                final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
                buffer.putStringWithoutLengthAscii(0, responseMsg);
                final ClientSession clientSession = cluster.getClientSession(clusterSessionId);

                while (clientSession.offer(correlationId, buffer, 0, responseMsg.length()) < 0)
                {
                    cluster.idle();
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(timedService)
                .errorHandler(Throwable::printStackTrace));
    }

    private AeronCluster connectToCluster(final SessionMessageListener sessionMessageListener)
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .sessionMessageListener(sessionMessageListener)
                .ingressChannel("aeron:udp")
                .clusterMemberEndpoints("0=localhost:9010,1=localhost:9011,2=localhost:9012"));
    }
}