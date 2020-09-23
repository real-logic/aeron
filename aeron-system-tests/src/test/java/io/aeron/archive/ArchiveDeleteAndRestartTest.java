/*
 * Copyright 2014-2020 Real Logic Limited.
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
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.test.MediaDriverTestWatcher;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.File;
import java.util.Random;

import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArchiveDeleteAndRestartTest
{
    private static final int SYNC_LEVEL = 0;
    private static final int PUBLISH_STREAM_ID = 1;

    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(4096, FrameDescriptor.FRAME_ALIGNMENT));
    private final Random rnd = new Random();
    private final long seed = System.nanoTime();

    @RegisterExtension
    public final TestWatcher randomSeedWatcher = ArchiveTests.newWatcher(seed);

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron client;

    private Archive.Context archiveContext;

    @BeforeEach
    public void before()
    {
        rnd.setSeed(seed);

        final int termLength = 1 << (16 + rnd.nextInt(10)); // 1M to 8M
        final int segmentFileLength = termLength << rnd.nextInt(4);

        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
                .spiesSimulateConnection(true)
                .errorHandler(Tests::onError)
                .dirDeleteOnStart(true),
            testWatcher);

        archiveContext = new Archive.Context()
            .maxCatalogEntries(ArchiveSystemTests.MAX_CATALOG_ENTRIES)
            .fileSyncLevel(SYNC_LEVEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive-test"))
            .segmentFileLength(segmentFileLength)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(YieldingIdleStrategy::new)
            .errorHandler(Tests::onError);

        archive = Archive.launch(archiveContext.clone());
        client = Aeron.connect();
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(client, archive, driver);

        if (null != archive)
        {
            archive.context().deleteDirectory();
        }

        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Timeout(10)
    @Test
    public void recordAndReplayExclusivePublication()
    {
        buffer.setMemory(0, 1024, (byte)'z');

        AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(client));

        final String uri = "aeron:ipc?term-length=16777216|init-term-id=502090867|term-offset=0|term-id=502090867";
        final ExclusivePublication recordedPublication1 = client.addExclusivePublication(uri, PUBLISH_STREAM_ID);

        aeronArchive.startRecording(uri, PUBLISH_STREAM_ID, SourceLocation.LOCAL);

        for (int i = 0; i < 10; i++)
        {
            while (recordedPublication1.offer(buffer, 0, 1024) < 0)
            {
                Tests.yieldingWait("Failed to offer data");
            }
        }

        final long position1 = recordedPublication1.position();
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector();

        while (aeronArchive.listRecordings(0, Integer.MAX_VALUE, collector) < 1)
        {
            Tests.yieldingWait("Didn't find recording");
        }

        while (position1 != aeronArchive.getRecordingPosition(collector.descriptors.get(0).recordingId))
        {
            Tests.yieldingWait("Failed to record data");
        }

        recordedPublication1.close();

        while (position1 != aeronArchive.getStopPosition(collector.descriptors.get(0).recordingId))
        {
            Tests.yieldingWait("Failed to stop recording");
        }

        final File file = archive.context().archiveDir();

        aeronArchive.close();
        archive.close();

        IoUtil.delete(file, true);

        archive = Archive.launch(archiveContext.clone());
        do
        {
            try
            {
                aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(client));
                break;
            }
            catch (final Exception ignore)
            {
                Tests.sleep(1000);
            }
        }
        while (true);

        final ExclusivePublication recordedPublication2 = client.addExclusivePublication(uri, PUBLISH_STREAM_ID);

        aeronArchive.startRecording(uri, PUBLISH_STREAM_ID, SourceLocation.LOCAL);

        for (int i = 0; i < 10; i++)
        {
            while (recordedPublication2.offer(buffer, 0, 1024) < 0)
            {
                Tests.yieldingWait("Failed to offer data");
            }
        }

        while (aeronArchive.listRecordings(0, Integer.MAX_VALUE, collector) < 1)
        {
            Tests.yieldingWait("Didn't find recording");
        }

        collector.descriptors.clear();
        assertEquals(1, aeronArchive.listRecordings(0, Integer.MAX_VALUE, collector), collector.descriptors::toString);
    }
}
