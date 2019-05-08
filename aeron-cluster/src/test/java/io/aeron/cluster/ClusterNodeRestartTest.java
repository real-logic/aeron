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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterNodeRestartTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int MESSAGE_LENGTH = SIZE_OF_INT;
    private static final int TIMER_MESSAGE_LENGTH = SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG;
    private static final int MESSAGE_VALUE_OFFSET = 0;
    private static final int TIMER_MESSAGE_ID_OFFSET = MESSAGE_VALUE_OFFSET + SIZE_OF_INT;
    private static final int TIMER_MESSAGE_DELAY_OFFSET = TIMER_MESSAGE_ID_OFFSET + SIZE_OF_LONG;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private final AtomicReference<String> serviceState = new AtomicReference<>();
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
    public void shouldRestartServiceWithReplay()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);
        final AtomicLong restartServiceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);

        while (serviceMsgCounter.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();

        launchClusteredMediaDriver(false);
        launchService(restartServiceMsgCounter);
        connectClient();

        while (restartServiceMsgCounter.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldRestartServiceWithReplayAndContinue()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);
        final AtomicLong restartServiceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);

        while (serviceMsgCounter.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();

        launchClusteredMediaDriver(false);
        launchService(restartServiceMsgCounter);
        connectClient();

        sendCountedMessageIntoCluster(1);

        while (restartServiceMsgCounter.get() == 1)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldRestartServiceFromEmptySnapshot() throws Exception
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        final CountersReader counters = container.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        while (snapshotCount.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1);
        }

        forceCloseForRestart();

        serviceState.set(null);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCounter);
        connectClient();

        while (null == serviceState.get())
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        assertThat(serviceState.get(), is("0"));
    }

    @Test(timeout = 10_000)
    public void shouldRestartServiceFromSnapshot() throws Exception
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);
        sendCountedMessageIntoCluster(1);
        sendCountedMessageIntoCluster(2);

        while (serviceMsgCounter.get() != 3)
        {
            Thread.yield();
        }

        final CountersReader counters = aeronCluster.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        while (snapshotCount.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1);
        }

        forceCloseForRestart();

        serviceState.set(null);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCounter);
        connectClient();

        while (null == serviceState.get())
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        assertThat(serviceState.get(), is("3"));
    }

    @Test(timeout = 10_000)
    public void shouldRestartServiceFromSnapshotWithFurtherLog() throws Exception
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);
        sendCountedMessageIntoCluster(1);
        sendCountedMessageIntoCluster(2);

        while (serviceMsgCounter.get() != 3)
        {
            Thread.yield();
        }

        final CountersReader counters = aeronCluster.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        while (snapshotCount.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1);
        }

        sendCountedMessageIntoCluster(3);

        while (serviceMsgCounter.get() != 4)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();

        serviceMsgCounter.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCounter);
        connectClient();

        while (serviceMsgCounter.get() != 1)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        assertThat(serviceState.get(), is("4"));
    }

    @Test(timeout = 10_000)
    public void shouldTakeMultipleSnapshots() throws Exception
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        final CountersReader counters = aeronCluster.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters);
        assertNotNull(controlToggle);

        for (int i = 0; i < 3; i++)
        {
            assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

            while (controlToggle.get() != ClusterControl.ToggleState.NEUTRAL.code())
            {
                TestUtil.checkInterruptedStatus();
                Thread.sleep(1);
            }
        }

        assertThat(snapshotCount.get(), is(3L));
    }

    @Test(timeout = 10_000)
    public void shouldRestartServiceWithTimerFromSnapshotWithFurtherLog() throws Exception
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0);

        launchService(serviceMsgCounter);
        connectClient();

        sendCountedMessageIntoCluster(0);
        sendCountedMessageIntoCluster(1);
        sendCountedMessageIntoCluster(2);
        sendTimerMessageIntoCluster(3, 1, TimeUnit.HOURS.toMillis(10));

        while (serviceMsgCounter.get() != 4)
        {
            Thread.yield();
        }

        final CountersReader counters = aeronCluster.context().aeron().countersReader();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        while (snapshotCount.get() == 0)
        {
            TestUtil.checkInterruptedStatus();
            Thread.sleep(1);
        }

        sendCountedMessageIntoCluster(4);

        while (serviceMsgCounter.get() != 5)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        forceCloseForRestart();

        serviceMsgCounter.set(0);
        launchClusteredMediaDriver(false);
        launchService(serviceMsgCounter);
        connectClient();

        while (serviceMsgCounter.get() != 1)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }

        assertThat(serviceState.get(), is("5"));
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

        final int triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);

        while (triggeredTimersCounter.get() <= triggeredSinceStart)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private void sendCountedMessageIntoCluster(final int value)
    {
        msgBuffer.putInt(MESSAGE_VALUE_OFFSET, value);

        sendMessageIntoCluster(aeronCluster, msgBuffer, MESSAGE_LENGTH);
    }

    private void sendTimerMessageIntoCluster(final int value, final long timerCorrelationId, final long delayMs)
    {
        msgBuffer.putInt(MESSAGE_VALUE_OFFSET, value);
        msgBuffer.putLong(TIMER_MESSAGE_ID_OFFSET, timerCorrelationId);
        msgBuffer.putLong(TIMER_MESSAGE_DELAY_OFFSET, delayMs);

        sendMessageIntoCluster(aeronCluster, msgBuffer, TIMER_MESSAGE_LENGTH);
    }

    private static void sendMessageIntoCluster(final AeronCluster cluster, final DirectBuffer buffer, final int length)
    {
        while (true)
        {
            final long result = cluster.offer(buffer, 0, length);
            if (result > 0)
            {
                break;
            }

            checkResult(result);
            TestUtil.checkInterruptedStatus();

            Thread.yield();
        }
    }

    private void launchService(final AtomicLong msgCounter)
    {
        final ClusteredService service =
            new StubClusteredService()
            {
                private int nextCorrelationId = 0;
                private int counterValue = 0;

                public void onStart(final Cluster cluster, final Image snapshotImage)
                {
                    super.onStart(cluster, snapshotImage);

                    if (null != snapshotImage)
                    {
                        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
                        {
                            nextCorrelationId = buffer.getInt(offset);
                            offset += SIZE_OF_INT;

                            counterValue = buffer.getInt(offset);
                            offset += SIZE_OF_INT;

                            final String s = buffer.getStringAscii(offset);

                            serviceState.set(s);
                        };

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

                public void onSessionMessage(
                    final ClientSession session,
                    final long timestampMs,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    final int sentValue = buffer.getInt(offset + MESSAGE_VALUE_OFFSET);
                    assertThat(sentValue, is(counterValue));

                    counterValue++;
                    serviceState.set(Integer.toString(counterValue));
                    msgCounter.getAndIncrement();

                    if (TIMER_MESSAGE_LENGTH == length)
                    {
                        final long correlationId = serviceCorrelationId(nextCorrelationId++);
                        final long deadlineMs = timestampMs + buffer.getLong(offset + TIMER_MESSAGE_DELAY_OFFSET);

                        assertTrue(cluster.scheduleTimer(correlationId, deadlineMs));
                    }
                }

                public void onTakeSnapshot(final Publication snapshotPublication)
                {
                    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

                    int length = 0;
                    buffer.putInt(length, nextCorrelationId);
                    length += SIZE_OF_INT;

                    buffer.putInt(length, counterValue);
                    length += SIZE_OF_INT;

                    length += buffer.putStringAscii(length, Integer.toString(counterValue));

                    snapshotPublication.offer(buffer, 0, length);
                }
            };

        container = null;

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .errorHandler(TestUtil.errorHandler(0)));
    }

    private void launchReschedulingService(final AtomicInteger triggeredTimersCounter)
    {
        final ClusteredService service =
            new StubClusteredService()
            {
                public void onSessionMessage(
                    final ClientSession session,
                    final long timestampMs,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    scheduleNext(serviceCorrelationId(7), timestampMs + 100);
                }

                public void onTimerEvent(final long correlationId, final long timestampMs)
                {
                    triggeredTimersCounter.getAndIncrement();
                    scheduleNext(correlationId, timestampMs + 100);
                }

                public void onStart(final Cluster cluster, final Image snapshotImage)
                {
                    super.onStart(cluster, snapshotImage);

                    if (null != snapshotImage)
                    {
                        final FragmentHandler fragmentHandler =
                            (buffer, offset, length, header) -> triggeredTimersCounter.set(buffer.getInt(offset));

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
                    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

                    buffer.putInt(0, triggeredTimersCounter.get());

                    snapshotPublication.offer(buffer, 0, SIZE_OF_INT);
                }

                private void scheduleNext(final long correlationId, final long deadlineMs)
                {
                    while (!cluster.scheduleTimer(correlationId, deadlineMs))
                    {
                        cluster.idle();
                    }
                }
            };

        container = null;

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
