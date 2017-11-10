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
import io.aeron.Subscription;
import io.aeron.cluster.client.*;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceAgent;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ClusterNodeTest
{
    private static final int FRAGMENT_LIMIT = 1;

    private MediaDriver driver;
    private ClusterNode clusterNode;
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

        clusterNode = ClusterNode.launch(
            new ClusterNode.Context()
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
        CloseHelper.close(clusterNode);
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
        final Aeron aeron = aeronCluster.context().aeron();
        final AgentRunner serviceAgentRunner = launchClusteredService(aeron);

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

        serviceAgentRunner.close();
    }

    private AgentRunner launchClusteredService(final Aeron aeron)
    {
        final Subscription logSubscription = aeron.addSubscription(
            ClusterNode.Configuration.logChannel(),
            ClusterNode.Configuration.logStreamId());

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

        final AgentRunner serviceAgentRunner = new AgentRunner(
            new SleepingMillisIdleStrategy(1),
            Throwable::printStackTrace,
            null,
            new ClusteredServiceAgent(aeron, echoService, logSubscription));

        AgentRunner.startOnThread(serviceAgentRunner);

        return serviceAgentRunner;
    }
}