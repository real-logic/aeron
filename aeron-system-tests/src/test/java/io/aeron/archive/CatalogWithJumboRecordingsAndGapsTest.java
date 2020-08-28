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
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.MediaDriverTestWatcher;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.stream.IntStream;

import static io.aeron.archive.AbstractListRecordingsSession.MAX_SCANS_PER_WORK_CYCLE;
import static io.aeron.archive.Catalog.PAGE_SIZE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.test.Tests.generateStringWithSuffix;
import static org.agrona.BitUtil.next;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CatalogWithJumboRecordingsAndGapsTest
{
    private static final int MTU_LENGTH = PAGE_SIZE * 4;
    private static final int TERM_LENGTH = MTU_LENGTH * 8;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;

    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final EpochClock epochClock = () -> 1;
    private long[] recordingIds;

    private TestMediaDriver mediaDriver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @BeforeEach
    void before()
    {
        recordingIds = new long[MAX_SCANS_PER_WORK_CYCLE * 3];
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            for (int i = 0, current = 0; i < recordingIds.length; i++)
            {
                final String stippedChannel;
                final String originalChannel;
                final String sourceIdentity;
                switch (current)
                {
                    case 0:
                        stippedChannel = generateStringWithSuffix("ch", "1", 2000);
                        originalChannel = "ch1?tag=OK";
                        sourceIdentity = "src1";
                        break;
                    case 1:
                        stippedChannel = "ch2";
                        originalChannel = generateStringWithSuffix("ch1?tag=O", "K", 2500);
                        sourceIdentity = "src2";
                        break;
                    case 2:
                        stippedChannel = "ch3";
                        originalChannel = "ch3?tag=OK|endpoint=localhost:8089";
                        sourceIdentity = generateStringWithSuffix("src", "3", 1999);
                        break;
                    default:
                        throw new Error();
                }

                recordingIds[i] = catalog.addNewRecording(i, NULL_POSITION, i * 100, NULL_TIMESTAMP, 0, SEGMENT_LENGTH,
                    TERM_LENGTH, MTU_LENGTH, current, current, stippedChannel, originalChannel, sourceIdentity);

                current = next(current, 3);
            }

            invalidateRecordings(catalog, 0, 3);
            invalidateRecordings(catalog, 20, 30);
            invalidateRecordings(catalog, 100, 111);
            invalidateRecordings(catalog, recordingIds.length - 5, recordingIds.length);
        }

        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        mediaDriver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .spiesSimulateConnection(false)
                .publicationTermBufferLength(TERM_MIN_LENGTH)
                .ipcTermBufferLength(TERM_MIN_LENGTH)
                .dirDeleteOnStart(true),
            testWatcher);

        archive = Archive.launch(
            new Archive.Context()
                .catalogCapacity(Common.CATALOG_CAPACITY)
                .aeronDirectoryName(aeronDirectoryName)
                .errorHandler(Tests::onError)
                .archiveDir(archiveDir)
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, mediaDriver);

        archive.context().deleteDirectory();
        mediaDriver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    void listRecordingShouldHandleHugeRecordingDescriptors()
    {
        final int count = aeronArchive.listRecording(recordingIds[3],
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) ->
            {
                assertEquals(300, startTimestamp);
                assertEquals("ch1?tag=OK", originalChannel);
                assertEquals("src1", sourceIdentity);
            });

        assertEquals(1, count);
    }

    private void invalidateRecordings(final Catalog catalog, final int from, final int to)
    {
        IntStream.range(from, to).forEach(i -> catalog.invalidateRecording(recordingIds[i]));
    }
}
