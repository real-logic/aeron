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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.File;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ArchiveDeleteAndRestartTest
{
    private static final int SYNC_LEVEL = 0;
    private static final int STREAM_ID = 1;

    private final long seed = System.nanoTime();

    @RegisterExtension
    final TestWatcher randomSeedWatcher = Tests.seedWatcher(seed);

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron client;

    private Archive.Context archiveContext;

    @BeforeEach
    void before()
    {
        final Random rnd = new Random();
        rnd.setSeed(seed);

        final int termLength = 1 << (16 + rnd.nextInt(10)); // 1M to 8M
        final int segmentFileLength = termLength << rnd.nextInt(4);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        archiveContext = TestContexts.localhostArchive()
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .fileSyncLevel(SYNC_LEVEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive-test"))
            .segmentFileLength(segmentFileLength)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(YieldingIdleStrategy::new);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveContext.clone());
        systemTestWatcher.dataCollector().add(archiveContext.archiveDir());

        client = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(client, archive, driver);
    }

    @InterruptAfter(10)
    @Test
    void recordAndReplayExclusivePublication()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        buffer.setMemory(0, buffer.capacity(), (byte)'z');

        AeronArchive aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive().aeron(client));

        final String uri = "aeron:ipc?term-length=16m|init-term-id=502090867|term-offset=0|term-id=502090867";
        final ExclusivePublication recordedPublication1 = client.addExclusivePublication(uri, STREAM_ID);

        final long subscriptionId = aeronArchive.startRecording(uri, STREAM_ID, SourceLocation.LOCAL);

        for (int i = 0; i < 10; i++)
        {
            while (recordedPublication1.offer(buffer, 0, 1024) < 0)
            {
                Tests.yieldingIdle("Failed to offer data");
            }
        }

        final long position1 = recordedPublication1.position();
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);

        while (aeronArchive.listRecordings(0, Integer.MAX_VALUE, collector.reset()) < 1)
        {
            Tests.yieldingIdle("Didn't find recording");
        }

        while (position1 != aeronArchive.getRecordingPosition(collector.descriptors().get(0).recordingId()))
        {
            Tests.yieldingIdle("Failed to record data");
        }

        recordedPublication1.close();
        aeronArchive.stopRecording(subscriptionId);

        while (position1 != aeronArchive.getStopPosition(collector.descriptors().get(0).recordingId()))
        {
            Tests.yieldingIdle("Failed to stop recording");
        }

        aeronArchive.close();
        archive.close();
        archive.context().deleteDirectory();

        archive = Archive.launch(archiveContext.clone());
        aeronArchive = AeronArchive.connect(TestContexts.localhostAeronArchive().aeron(client));

        final ExclusivePublication recordedPublication2 = client.addExclusivePublication(uri, STREAM_ID);
        aeronArchive.startRecording(uri, STREAM_ID, SourceLocation.LOCAL);

        for (int i = 0; i < 10; i++)
        {
            while (recordedPublication2.offer(buffer, 0, 1024) < 0)
            {
                Tests.yieldingIdle("Failed to offer data");
            }
        }

        while (aeronArchive.listRecordings(0, Integer.MAX_VALUE, collector.reset()) < 1)
        {
            Tests.yieldingIdle("Didn't find recording");
        }

        assertEquals(
            1, aeronArchive.listRecordings(0, Integer.MAX_VALUE, collector.reset()), collector.descriptors()::toString);
    }
}
