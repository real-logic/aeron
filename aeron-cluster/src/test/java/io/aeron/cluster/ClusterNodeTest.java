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
import io.aeron.cluster.client.*;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ClusterNodeTest
{
    private static final int FRAGMENT_LIMIT = 1;

    private MediaDriver driver;
    private ConsensusModule consensusModule;
    private AeronCluster aeronCluster;

    @Before
    public void before()
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .spiesSimulateConnection(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true));

        consensusModule = ConsensusModule.launch(
            new ConsensusModule.Context()
                .errorCounter(driver.context().systemCounters().get(SystemCounterDescriptor.ERRORS))
                .errorHandler(driver.context().errorHandler()));

        aeronCluster = AeronCluster.connect(
            new AeronCluster.Context()
                .lock(new NoOpLock()));
    }

    @After
    public void after()
    {
        CloseHelper.close(aeronCluster);
        CloseHelper.close(consensusModule);
        CloseHelper.close(driver);

        driver.context().deleteAeronDirectory();
    }

    @Test
    public void shouldConnectAndSendKeepAlive()
    {
        assertTrue(aeronCluster.sendKeepAlive());
    }

    @Test(timeout = 10_000)
    public void shouldEchoMessageViaService() throws Exception
    {
        final ClusteredServiceContainer container = launchContainer();
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

        container.close();
    }

    private ClusteredServiceContainer launchContainer()
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
                .errorCounter(mock(AtomicCounter.class))
                .errorHandler(Throwable::printStackTrace));
    }
}