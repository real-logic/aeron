/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ArchiveAbandonedClientTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    @TempDir
    Path tempDir;

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive client1;
    private AeronArchive client2;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("media-driver").toString())
            .termBufferSparseFile(true)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true)
            .enableExperimentalFeatures(true)
            .threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(client1, client2, aeron, archive, driver);
    }

    @ParameterizedTest
    @CsvSource({
        "aeron:ipc, aeron:ipc",
        "aeron:udp?endpoint=localhost:10001, aeron:udp?endpoint=localhost:10002",
        "aeron:udp?endpoint=localhost:10001, aeron:udp?control=localhost:10002|control-mode=response",
    })
    @InterruptAfter(10)
    void test(final String requestChannel, final String responseChannel)
    {
        launch(requestChannel, responseChannel);

        final String channel = "aeron:ipc?ssc=true|term-length=128k";
        final int streamId = 444;
        final ExclusivePublication recordedPublication =
            client1.addRecordedExclusivePublication(channel, streamId);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        ThreadLocalRandom.current().nextBytes(buffer.byteArray());

        final CountersReader counters = aeron.countersReader();
        final int recordingCounterId = Tests.awaitRecordingCounterId(
            counters, recordedPublication.sessionId(), client1.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);

        final int archiveResponseSubscriptionCounterId =
            client1.controlResponsePoller().subscription().imageAtIndex(0).subscriberPositionId();

        // ensure at least one full term of archive responses is generated in order to trigger blocking
        final int controlTermBufferLength = archive.context().controlTermBufferLength();
        while (counters.getCounterValue(archiveResponseSubscriptionCounterId) < controlTermBufferLength)
        {
            final int length = ThreadLocalRandom.current().nextInt(buffer.capacity());
            while (recordedPublication.offer(buffer, 0, length) < 0)
            {
                Tests.yield();
            }

            while (client1.getMaxRecordedPosition(recordingId) < recordedPublication.position())
            {
                Tests.yield();
            }
        }
    }

    private void launch(final String requestChannel, final String responseChannel)
    {
        final int requestStreamId = 111;
        final int responseStreamId = 222;
        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .controlChannel("aeron:udp?endpoint=localhost:10001")
            .controlStreamId(requestStreamId)
            .localControlChannel("aeron:ipc")
            .localControlStreamId(requestStreamId)
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .fileSyncLevel(0)
            .deleteArchiveOnStart(true)
            .archiveDir(tempDir.resolve("archive-test").toFile())
            .segmentFileLength(1024 * 1024)
            .idleStrategySupplier(YieldingIdleStrategy::new)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .useConductorAgentInvoker(true);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .controlRequestChannel(requestChannel)
            .controlRequestStreamId(requestStreamId)
            .controlResponseChannel(responseChannel)
            .controlResponseStreamId(responseStreamId)
            .controlTermBufferLength(64 * 1024)
            .controlTermBufferSparse(true);

        archive = Archive.launch(archiveContext);
        systemTestWatcher.dataCollector().add(archive.context().archiveDir());

        aeron = Aeron.connect(aeronCtx);
        client1 = AeronArchive.connect(aeronArchiveContext.clone().aeron(aeron).ownsAeronClient(false));
        client2 = AeronArchive.connect(aeronArchiveContext.clone().aeron(aeron).ownsAeronClient(false));

        assertEquals(archive.context().archiveId(), client1.archiveId());
        assertEquals(archive.context().archiveId(), client2.archiveId());
    }
}
