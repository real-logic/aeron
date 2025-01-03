/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples.cluster.tutorial;

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.console.ContinueBarrier;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static io.aeron.samples.cluster.ClusterConfig.*;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Single Node Cluster that includes everything needed to run all in one place. Includes a simple service to show
 * event processing, and includes a cluster client.
 * <p>
 * Perfect for playing around with the Cluster.
 */
public final class SingleNodeCluster implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;
    private static final int MESSAGE_ID = 1;
    private static final int TIMER_ID = 2;
    private static final int PORT_BASE = 9000;

    // cluster side
    private final ClusterConfig config;
    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;

    // cluster client side
    private MediaDriver clientMediaDriver;
    private AeronCluster client;
    private final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final EgressListener egressMessageListener = new EgressListener()
    {
        /**
         * {@inheritDoc}
         */
        public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            System.out.println("egress onMessage " + clusterSessionId);
        }

        /**
         * {@inheritDoc}
         */
        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints)
        {
            System.out.println("SingleNodeCluster.onNewLeader");
        }
    };

    static class Service implements ClusteredService
    {
        protected Cluster cluster;
        protected IdleStrategy idleStrategy;
        private int messageCount = 0;
        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        /**
         * {@inheritDoc}
         */
        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            this.cluster = cluster;
            this.idleStrategy = cluster.idleStrategy();

            if (null != snapshotImage)
            {
                System.out.println("onStart load snapshot");
                final FragmentHandler fragmentHandler =
                    (buffer, offset, length, header) -> messageCount = buffer.getInt(offset);

                idleStrategy.reset();
                while (snapshotImage.poll(fragmentHandler, 1) <= 0)
                {
                    idleStrategy.idle();
                }

                System.out.println("snapshot messageCount=" + messageCount);
            }
        }

        /**
         * {@inheritDoc}
         */
        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
            System.out.println("onSessionOpen " + session.id());
        }

        /**
         * {@inheritDoc}
         */
        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
            System.out.println("onSessionClose " + session.id() + " " + closeReason);
        }

        /**
         * {@inheritDoc}
         */
        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            messageCount++;
            System.out.println(cluster.role() + " onSessionMessage " + session.id() + " count=" + messageCount);

            final int id = buffer.getInt(offset);
            if (TIMER_ID == id)
            {
                idleStrategy.reset();
                while (!cluster.scheduleTimer(serviceCorrelationId(1), cluster.time() + 1_000))
                {
                    idleStrategy.idle();
                }
            }
            else
            {
                echoMessage(session, buffer, offset, length);
            }
        }

        /**
         * {@inheritDoc}
         */
        public void onTimerEvent(final long correlationId, final long timestamp)
        {
            System.out.println("onTimerEvent " + correlationId);

            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
            buffer.putInt(0, 1);

            cluster.forEachClientSession((clientSession) -> echoMessage(clientSession, buffer, 0, SIZE_OF_INT));
        }

        /**
         * {@inheritDoc}
         */
        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            System.out.println("onTakeSnapshot messageCount=" + messageCount);

            buffer.putInt(0, messageCount);
            idleStrategy.reset();
            while (snapshotPublication.offer(buffer, 0, 4) < 0)
            {
                idleStrategy.idle();
            }
        }

        /**
         * {@inheritDoc}
         */
        public void onRoleChange(final Cluster.Role newRole)
        {
            System.out.println("onRoleChange " + newRole);
        }

        /**
         * {@inheritDoc}
         */
        public void onTerminate(final Cluster cluster)
        {
        }

        /**
         * {@inheritDoc}
         */
        public void onNewLeadershipTermEvent(
            final long leadershipTermId,
            final long logPosition,
            final long timestamp,
            final long termBaseLogPosition,
            final int leaderMemberId,
            final int logSessionId,
            final TimeUnit timeUnit,
            final int appVersion)
        {
            System.out.println("onNewLeadershipTermEvent");
        }

        protected long serviceCorrelationId(final int correlationId)
        {
            return ((long)cluster.context().serviceId()) << 56 | correlationId;
        }

        private void echoMessage(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length)
        {
            idleStrategy.reset();
            int attempts = SEND_ATTEMPTS;
            do
            {
                final long result = session.offer(buffer, offset, length);
                if (result > 0)
                {
                    return;
                }
                idleStrategy.idle();
            }
            while (--attempts > 0);
        }
    }

    /**
     * Create and launch a new single node cluster.
     *
     * @param externalService to run in the container.
     * @param isCleanStart    to indicate if a clean start should be made.
     */
    public SingleNodeCluster(final ClusteredService externalService, final boolean isCleanStart)
    {
        final ClusteredService service = null == externalService ? new SingleNodeCluster.Service() : externalService;
        config = ClusterConfig.create(0, Collections.singletonList("localhost"), PORT_BASE, service);

        config.mediaDriverContext().dirDeleteOnStart(true);
        config.archiveContext().deleteArchiveOnStart(isCleanStart);
        config.consensusModuleContext()
            .deleteDirOnStart(isCleanStart)
            .ingressChannel("aeron:udp?endpoint=" + config.ingressHostname() + ":" +
            calculatePort(config.memberId(), PORT_BASE, CLIENT_FACING_PORT_OFFSET));

        clusteredMediaDriver = ClusteredMediaDriver.launch(
            config.mediaDriverContext(),
            config.archiveContext(),
            config.consensusModuleContext());

        container = ClusteredServiceContainer.launch(config.clusteredServiceContext());
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        final ErrorHandler errorHandler = clusteredMediaDriver.mediaDriver().context().errorHandler();
        CloseHelper.close(errorHandler, client);
        CloseHelper.close(errorHandler, clientMediaDriver);
        CloseHelper.close(errorHandler, clusteredMediaDriver.consensusModule());
        CloseHelper.close(errorHandler, container);
        CloseHelper.close(clusteredMediaDriver); // ErrorHandler will be closed during that call so can't use it
    }

    void connectClientToCluster()
    {
        final String aeronDirectoryName = CommonContext.getAeronDirectoryName() + "-client";

        clientMediaDriver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .errorHandler(Throwable::printStackTrace)
                .aeronDirectoryName(aeronDirectoryName));

        client = AeronCluster.connect(
            new AeronCluster.Context()
                .errorHandler(Throwable::printStackTrace)
                .egressListener(egressMessageListener)
                .aeronDirectoryName(aeronDirectoryName)
                .ingressChannel("aeron:udp")
                .ingressEndpoints(ingressEndpoints(
                    Collections.singletonList(config.ingressHostname()), PORT_BASE, CLIENT_FACING_PORT_OFFSET)));
    }

    void sendMessageToCluster(final int id, final int messageLength)
    {
        msgBuffer.putInt(0, id);
        idleStrategy.reset();
        while (client.offer(msgBuffer, 0, messageLength) < 0)
        {
            idleStrategy.idle();
        }
    }

    int pollEgress()
    {
        return null == client ? 0 : client.pollEgress();
    }

    void pollEgressUntilMessage()
    {
        idleStrategy.reset();
        while (pollEgress() <= 0)
        {
            idleStrategy.idle();
        }
    }

    void takeSnapshot()
    {
        final ConsensusModule.Context consensusModuleContext = clusteredMediaDriver.consensusModule().context();
        final AtomicCounter snapshotCounter = consensusModuleContext.snapshotCounter();
        final long snapshotCount = snapshotCounter.get();

        final AtomicCounter controlToggle = ClusterControl.findControlToggle(
            clusteredMediaDriver.mediaDriver().context().countersManager(),
            consensusModuleContext.clusterId());
        ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle);

        idleStrategy.reset();
        while (snapshotCounter.get() <= snapshotCount)
        {
            idleStrategy.idle();
        }
    }

    static void sendSingleMessageAndEchoBack()
    {
        try (SingleNodeCluster cluster = new SingleNodeCluster(null, true))
        {
            cluster.connectClientToCluster();
            cluster.sendMessageToCluster(MESSAGE_ID, 4);
            cluster.pollEgressUntilMessage();

            final ContinueBarrier barrier = new ContinueBarrier("continue");
            barrier.await();
        }
    }

    static void loadPreviousLogAndSendAnotherMessageAndEchoBack()
    {
        try (SingleNodeCluster cluster = new SingleNodeCluster(null, false))
        {
            cluster.connectClientToCluster();
            cluster.sendMessageToCluster(MESSAGE_ID, 4);
            cluster.pollEgressUntilMessage();

            final ContinueBarrier barrier = new ContinueBarrier("continue");
            barrier.await();
        }
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        sendSingleMessageAndEchoBack();
        loadPreviousLogAndSendAnotherMessageAndEchoBack();
    }
}
