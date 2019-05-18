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

import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterTimerTest
{
    private static final long MAX_CATALOG_ENTRIES = 128;
    private static final int MESSAGE_LENGTH = SIZE_OF_INT;
    private static final int MESSAGE_VALUE_OFFSET = 0;
    private static final int INTERVAL_MS = 20;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final AtomicLong snapshotCount = new AtomicLong();
    private final Counter mockSnapshotCounter = mock(Counter.class);

    @Before
    public void before()
    {
        when(mockSnapshotCounter.incrementOrdered()).thenAnswer((inv) -> snapshotCount.getAndIncrement());

        launchClusteredMediaDriver(true);
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

    @Test(timeout = 10_000)
    public void shouldRestartServiceWithTimerFromSnapshotWithFurtherLog()
    {
        final AtomicInteger triggeredTimersCounter = new AtomicInteger();

        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);

        while (triggeredTimersCounter.get() < 2)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        final CountersReader counters = aeronCluster.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        while (snapshotCount.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        while (triggeredTimersCounter.get() < 4)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();
        triggeredTimersCounter.set(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        while (triggeredTimersCounter.get() < (2 + 4))
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();
        final int triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        triggeredTimersCounter.set(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        while (triggeredTimersCounter.get() < (triggeredSinceStart + 4))
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldTriggerRescheduledTimerAfterReplay()
    {
        final AtomicInteger triggeredTimersCounter = new AtomicInteger();

        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);

        while (triggeredTimersCounter.get() < 2)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();

        int triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);

        while (triggeredTimersCounter.get() <= (triggeredSinceStart + 2))
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();

        triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);

        while (triggeredTimersCounter.get() <= (triggeredSinceStart + 4))
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private void sendCountedMessageIntoCluster(final int value)
    {
        msgBuffer.putInt(MESSAGE_VALUE_OFFSET, value);
        sendMessageIntoCluster(aeronCluster, msgBuffer);
    }

    private static void sendMessageIntoCluster(final AeronCluster cluster, final DirectBuffer buffer)
    {
        while (true)
        {
            final long result = cluster.offer(buffer, 0, MESSAGE_LENGTH);
            if (result > 0)
            {
                break;
            }

            checkResult(result);
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private void launchReschedulingService(final AtomicInteger triggeredTimersCounter)
    {
        final ClusteredService service = new StubClusteredService()
        {
            int timerId = 1;

            public void onSessionMessage(
                final ClientSession session,
                final long timestampMs,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                scheduleNext(serviceCorrelationId(timerId++), timestampMs + INTERVAL_MS);
            }

            public void onTimerEvent(final long correlationId, final long timestampMs)
            {
                triggeredTimersCounter.getAndIncrement();
                scheduleNext(serviceCorrelationId(timerId++), timestampMs + INTERVAL_MS);
            }

            public void onStart(final Cluster cluster, final Image snapshotImage)
            {
                super.onStart(cluster, snapshotImage);
                this.cluster = cluster;

                if (null != snapshotImage)
                {
                    final FragmentHandler fragmentHandler =
                        (buffer, offset, length, header) -> timerId = buffer.getInt(offset);

                    while (true)
                    {
                        final int fragments = snapshotImage.poll(fragmentHandler, 1);
                        if (fragments == 1)
                        {
                            break;
                        }

                        cluster.idle();
                    }
                }
            }

            public void onTakeSnapshot(final Publication snapshotPublication)
            {
                final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(SIZE_OF_INT);
                buffer.putInt(0, timerId);

                while (snapshotPublication.offer(buffer, 0, SIZE_OF_INT) < 0)
                {
                    cluster.idle();
                }
            }

            private void scheduleNext(final long correlationId, final long deadlineMs)
            {
                while (!cluster.scheduleTimer(correlationId, deadlineMs))
                {
                    cluster.idle();
                }
            }
        };

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .errorHandler(TestUtil.errorHandler(0)));
    }

    private AeronCluster connectToCluster()
    {
        return AeronCluster.connect();
    }

    private void forceCloseForRestart()
    {
        clusteredMediaDriver.consensusModule().close();
        container.close();
        aeronCluster.close();
        clusteredMediaDriver.close();
    }

    private void connectClient()
    {
        aeronCluster = null;
        aeronCluster = connectToCluster();
    }

    private void launchClusteredMediaDriver(final boolean initialLaunch)
    {
        clusteredMediaDriver = null;

        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .warnIfDirectoryExists(initialLaunch)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(TestUtil.errorHandler(0))
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(initialLaunch),
            new ConsensusModule.Context()
                .errorHandler(TestUtil.errorHandler(0))
                .snapshotCounter(mockSnapshotCounter)
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .deleteDirOnStart(initialLaunch));
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("unexpected publication state: " + result);
        }
    }
}
