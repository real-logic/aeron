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
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

public class ClusterNodeRestartTest
{

    private static final int FRAGMENT_LIMIT = 1;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    @Before
    public void before()
    {
        launchClusteredMediaDriver(true);
    }

    @After
    public void after()
    {
        CloseHelper.close(container);
        CloseHelper.close(clusteredMediaDriver);

        clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();

        if (null != container)
        {
            container.context().deleteClusterDirectory();
        }
    }

    @Test(timeout = 10_000)
    public void shouldLaunchServiceSendMessageCloseServiceAndRestartServiceWithReplay()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);
        final AtomicLong restartServiceMsgCounter = new AtomicLong(0);

        container = launchService(true, serviceMsgCounter);

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

        while (serviceMsgCounter.get() == 0)
        {
            Thread.yield();
        }

        aeronCluster.close();
        container.close();
        clusteredMediaDriver.close();

        launchClusteredMediaDriver(false);
        container = launchService(false, restartServiceMsgCounter);

        while (restartServiceMsgCounter.get() == 0)
        {
            Thread.yield();
        }
    }

    private ClusteredServiceContainer launchService(final boolean initialLaunch, final AtomicLong msgCounter)
    {
        final ClusteredService service =
            new StubClusteredService()
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
                    msgCounter.getAndIncrement();
                }
            };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .errorCounter(mock(AtomicCounter.class))
                .errorHandler(Throwable::printStackTrace)
                .deleteDirOnStart(initialLaunch));
    }

    private AeronCluster connectToCluster()
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .lock(new NoOpLock()));
    }

    private void launchClusteredMediaDriver(final boolean initialLaunch)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .warnIfDirectoryExists(initialLaunch)
                .threadingMode(ThreadingMode.SHARED)
                .spiesSimulateConnection(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(initialLaunch),
            new ConsensusModule.Context());
    }
}
