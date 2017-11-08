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
import io.aeron.cluster.codecs.SessionEventCode;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceAgent;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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

    @Before
    public void before()
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .useConcurrentCountersManager(true)
                .spiesSimulateConnection(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true));

        clusterNode = ClusterNode.launch(
            new ClusterNode.Context()
                .countersManager(driver.context().countersManager())
                .errorHandler(driver.context().errorHandler()));
    }

    @After
    public void after()
    {
        CloseHelper.close(clusterNode);
        CloseHelper.close(driver);

        driver.context().deleteAeronDirectory();
    }

    @Test
    public void shouldConnectAndSendKeepAlive()
    {
        final AeronCluster cluster = AeronCluster.connect(
            new AeronCluster.Context()
                .lock(new NoOpLock()));

        assertTrue(cluster.sendKeepAlive());

        cluster.close();
    }

    @Test(timeout = 10_000)
    public void shouldEchoMessageViaService() throws Exception
    {
        final Aeron aeron = Aeron.connect();

        final Subscription logSubscription = aeron.addSubscription(
            ClusterNode.Configuration.logChannel(),
            ClusterNode.Configuration.logStreamId());

        final EchoService echoService = new EchoService();
        final AgentRunner serviceAgentRunner = new AgentRunner(
            new SleepingMillisIdleStrategy(1),
            Throwable::printStackTrace,
            null,
            new ClusteredServiceAgent(aeron, echoService, logSubscription));

        AgentRunner.startOnThread(serviceAgentRunner);

        final AeronCluster cluster = AeronCluster.connect(
            new AeronCluster.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .lock(new NoOpLock()));

        final VectoredSessionDecorator vectoredSessionDecorator = new VectoredSessionDecorator(cluster.sessionId());
        final Publication publication = cluster.ingressPublication();

        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final long msgCorrelationId = aeron.nextCorrelationId();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        while (vectoredSessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length()) < 0)
        {
            Thread.yield();
        }

        final MutableInteger messageCount = new MutableInteger();
        final EgressAdapter adapter = new EgressAdapter(
            new EgressListener()
            {
                public void sessionEvent(
                    final long correlationId,
                    final long clusterSessionId,
                    final SessionEventCode code,
                    final String detail)
                {
                }

                public void newLeader(
                    final long correlationId,
                    final long clusterSessionId,
                    final long lastMessageTimestamp,
                    final long clusterTermTimestamp,
                    final long clusterMessageIndex,
                    final long clusterTermId,
                    final String leader)
                {
                }

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
            cluster.egressSubscription(),
            FRAGMENT_LIMIT);

        while (messageCount.get() == 0)
        {
            if (adapter.poll() <= 0)
            {
                Thread.yield();
            }
        }

        cluster.close();
        serviceAgentRunner.close();
        aeron.close();
    }

    public static class EchoService implements ClusteredService
    {
        private Cluster cluster;

        public void onStart(final Cluster cluster)
        {
            this.cluster = cluster;
        }

        public void onSessionOpen(final ClientSession session)
        {
        }

        public void onSessionClose(final ClientSession session)
        {
        }

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

        public void onTimerEvent(final long correlationId)
        {
        }
    }
}