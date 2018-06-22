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

import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterNodeRestartTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private AeronCluster aeronCluster;
    private final AtomicReference<String> serviceState = new AtomicReference<>();
    private final AtomicBoolean isTerminated = new AtomicBoolean();
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

        container.close();
        clusteredMediaDriver.close();
        aeronCluster.close();

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

        container.close();
        clusteredMediaDriver.close();
        aeronCluster.close();

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

        container.close();
        clusteredMediaDriver.close();
        aeronCluster.close();

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

        container.close();
        clusteredMediaDriver.close();
        aeronCluster.close();

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

        container.close();
        clusteredMediaDriver.close();
        aeronCluster.close();

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

    private void sendCountedMessageIntoCluster(final int value)
    {
        final long msgCorrelationId = aeronCluster.nextCorrelationId();
        msgBuffer.putInt(0, value);

        while (true)
        {
            final long result = aeronCluster.offer(msgCorrelationId, msgBuffer, 0, SIZE_OF_INT);
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
                private int counterValue = 0;

                public void onSessionMessage(
                    final ClientSession session,
                    final long correlationId,
                    final long timestampMs,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    final int sentValue = buffer.getInt(offset);
                    assertThat(sentValue, is(counterValue));

                    counterValue++;
                    serviceState.set(Integer.toString(counterValue));
                    msgCounter.getAndIncrement();
                }

                public void onTakeSnapshot(final Publication snapshotPublication)
                {
                    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

                    int length = 0;
                    buffer.putInt(length, counterValue);
                    length += SIZE_OF_INT;

                    length += buffer.putIntAscii(length, counterValue);

                    snapshotPublication.offer(buffer, 0, length);
                }

                public void onLoadSnapshot(final Image snapshotImage)
                {
                    while (true)
                    {
                        final int fragments = snapshotImage.poll(
                            (buffer, offset, length, header) ->
                            {
                                counterValue = buffer.getInt(offset);

                                final String s = buffer.getStringWithoutLengthAscii(
                                    offset + SIZE_OF_INT, length - SIZE_OF_INT);

                                serviceState.set(s);
                            },
                            1);

                        if (fragments == 1)
                        {
                            break;
                        }

                        cluster.idle();
                    }
                }
            };

        container = null;

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(() -> {})
                .errorHandler(Throwable::printStackTrace));
    }

    private AeronCluster connectToCluster()
    {
        return AeronCluster.connect();
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
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(initialLaunch),
            new ConsensusModule.Context()
                .errorHandler(Throwable::printStackTrace)
                .snapshotCounter(mockSnapshotCounter)
                .terminationHook(() -> isTerminated.set(true))
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
