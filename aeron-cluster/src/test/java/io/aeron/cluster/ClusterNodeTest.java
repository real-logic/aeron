/*
 * Copyright 2017 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressAdapter;
import io.aeron.cluster.client.SessionDecorator;
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
import org.agrona.concurrent.NoOpLock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ClusterNodeTest
{
    private static final int FRAGMENT_LIMIT = 1;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    @Before
    public void before()
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .spiesSimulateConnection(true)
                .termBufferSparseFile(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .deleteDirOnStart(true));
    }

    @After
    public void after()
    {
        CloseHelper.close(container);
        CloseHelper.close(clusteredMediaDriver);

        clusteredMediaDriver.consensusModule().context().deleteDirectory();
        clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();

        if (null != container)
        {
            container.context().deleteDirectory();
        }
    }

    @Test
    public void shouldConnectAndSendKeepAlive()
    {
        container = launchEchoService();

        try (AeronCluster aeronCluster = connectToCluster())
        {
            assertTrue(aeronCluster.sendKeepAlive());
        }
    }

    @Test(timeout = 10_000)
    public void shouldEchoMessageViaService()
    {
        container = launchEchoService();

        final AeronCluster aeronCluster = connectToCluster();
        final Aeron aeron = aeronCluster.context().aeron();

        final SessionDecorator sessionDecorator = new SessionDecorator(aeronCluster.sessionId());
        final Publication publication = aeronCluster.ingressPublication();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final long msgCorrelationId = aeron.nextCorrelationId();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length()) < 0)
        {
            Thread.yield();
        }

        final MutableInteger messageCount = new MutableInteger();
        final EgressAdapter adapter = new EgressAdapter(
            new StubEgressListener()
            {
                public void onMessage(
                    final long correlationId,
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    assertThat(correlationId, is(msgCorrelationId));
                    assertThat(buffer.getStringWithoutLengthAscii(offset, length), is(msg));

                    messageCount.value += 1;
                }
            },
            aeronCluster.egressSubscription(),
            FRAGMENT_LIMIT);

        while (messageCount.get() == 0)
        {
            if (adapter.poll() <= 0)
            {
                Thread.yield();
            }
        }

        aeronCluster.close();
    }

    @Test(timeout = 10_000)
    public void shouldScheduleEventInService()
    {
        container = launchScheduledService();

        final AeronCluster aeronCluster = connectToCluster();
        final Aeron aeron = aeronCluster.context().aeron();

        final SessionDecorator sessionDecorator = new SessionDecorator(aeronCluster.sessionId());
        final Publication publication = aeronCluster.ingressPublication();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final long msgCorrelationId = aeron.nextCorrelationId();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length()) < 0)
        {
            Thread.yield();
        }

        final MutableInteger messageCount = new MutableInteger();
        final EgressAdapter adapter = new EgressAdapter(
            new StubEgressListener()
            {
                public void onMessage(
                    final long correlationId,
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    assertThat(correlationId, is(msgCorrelationId));
                    assertThat(buffer.getStringWithoutLengthAscii(offset, length), is(msg + "-scheduled"));

                    messageCount.value += 1;
                }
            },
            aeronCluster.egressSubscription(),
            FRAGMENT_LIMIT);

        while (messageCount.get() == 0)
        {
            if (adapter.poll() <= 0)
            {
                Thread.yield();
            }
        }

        aeronCluster.close();
    }

    private ClusteredServiceContainer launchEchoService()
    {
        final ClusteredService echoService = new StubClusteredService()
        {
            public void onSessionMessage(
                final long clusterSessionId,
                final long correlationId,
                final long timestampMs,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                final ClientSession session = cluster.getClientSession(clusterSessionId);

                while (session.offer(correlationId, buffer, offset, length) < 0)
                {
                    Thread.yield();
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(echoService)
                .errorHandler(Throwable::printStackTrace)
                .deleteDirOnStart(true));
    }

    private ClusteredServiceContainer launchScheduledService()
    {
        final ClusteredService echoScheduledService = new StubClusteredService()
        {
            long clusterSessionId;
            long correlationId;
            String msg;

            public void onSessionMessage(
                final long clusterSessionId,
                final long correlationId,
                final long timestampMs,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                this.clusterSessionId = clusterSessionId;
                this.correlationId = correlationId;
                this.msg = buffer.getStringWithoutLengthAscii(offset, length);

                cluster.scheduleTimer(correlationId, timestampMs + 100);
            }

            public void onTimerEvent(final long correlationId, final long timestampMs)
            {
                final String responseMsg = msg + "-scheduled";
                final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
                buffer.putStringWithoutLengthAscii(0, responseMsg);
                final ClientSession clientSession = cluster.getClientSession(clusterSessionId);

                while (clientSession.offer(correlationId, buffer, 0, responseMsg.length()) < 0)
                {
                    Thread.yield();
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(echoScheduledService)
                .errorHandler(Throwable::printStackTrace)
                .deleteDirOnStart(true));
    }

    private AeronCluster connectToCluster()
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .lock(new NoOpLock()));
    }
}