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
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.List;
import java.util.stream.IntStream;

import static io.aeron.archive.AbstractListRecordingsSession.MAX_SCANS_PER_WORK_CYCLE;
import static io.aeron.archive.Catalog.PAGE_SIZE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static io.aeron.archive.codecs.RecordingState.INVALID;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.test.Tests.generateStringWithSuffix;
import static java.util.Arrays.asList;
import static org.agrona.BitUtil.next;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class CatalogWithJumboRecordingsAndGapsTest
{
    private static final int MTU_LENGTH = PAGE_SIZE * 4;
    private static final int TERM_LENGTH = MTU_LENGTH * 8;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;
    private static final int NUM_RECORDINGS = MAX_SCANS_PER_WORK_CYCLE * 3;
    private static final String[] STRIPPED_CHANNELS =
        new String[]{ generateStringWithSuffix("ch", "1", 2000), "ch2", "ch3" };
    private static final String[] ORIGINAL_CHANNELS =
        new String[]{
            "ch1?tag=OK",
            generateStringWithSuffix("ch2?tag=O", "K", 2500),
            "ch3?tag=OK|endpoint=localhost:8089" };
    private static final String[] SOURCE_IDENTITIES =
        new String[]{ "src1", "src2", generateStringWithSuffix("src", "3", 1999) };

    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final CachedEpochClock epochClock = new CachedEpochClock();
    private long[] recordingIds;

    private TestMediaDriver mediaDriver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before()
    {
        epochClock.update(1);
        recordingIds = new long[NUM_RECORDINGS];

        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            for (int i = 0, current = 0; i < recordingIds.length; i++)
            {
                recordingIds[i] = catalog.addNewRecording(
                    i,
                    NULL_POSITION,
                    i * 100L,
                    NULL_TIMESTAMP,
                    0,
                    SEGMENT_LENGTH,
                    TERM_LENGTH,
                    MTU_LENGTH,
                    current,
                    current,
                    STRIPPED_CHANNELS[current],
                    ORIGINAL_CHANNELS[current],
                    SOURCE_IDENTITIES[current]);

                current = next(current, 3);
            }

            changeRecordingsState(catalog, 0, 3);
            changeRecordingsState(catalog, 20, 30);
            changeRecordingsState(catalog, 100, 111);
            changeRecordingsState(catalog, recordingIds.length - 5, recordingIds.length);
        }

        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .publicationTermBufferLength(TERM_MIN_LENGTH)
            .ipcTermBufferLength(TERM_MIN_LENGTH)
            .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .aeronDirectoryName(aeronDirectoryName)
            .archiveDir(archiveDir)
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        mediaDriver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            TestContexts.localhostAeronArchive()
                .aeron(aeron));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, mediaDriver);
    }

    @Test
    @InterruptAfter(10)
    void listRecording()
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

    @ParameterizedTest
    @InterruptAfter(10)
    @MethodSource("listRecordingsArguments")
    void listRecordings(final long fromRecordingId, final int recordCount, final int expectedRecordCount)
    {
        final MutableInteger callCount = new MutableInteger();

        final int count = aeronArchive.listRecordings(
            fromRecordingId,
            recordCount,
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
            sourceIdentity) -> callCount.increment());

        assertEquals(expectedRecordCount, count);
        assertEquals(expectedRecordCount, callCount.get());
    }

    @ParameterizedTest
    @InterruptAfter(10)
    @MethodSource("listRecordingsForUriArguments")
    void listRecordingsForUri(
        final long fromRecordingId,
        final int recordCount,
        final String channelFragment,
        final int streamId,
        final int expectedRecordCount)
    {
        final MutableInteger callCount = new MutableInteger();

        final int count = aeronArchive.listRecordingsForUri(
            fromRecordingId,
            recordCount,
            channelFragment,
            streamId,
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
            streamId1,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> callCount.increment());

        assertEquals(expectedRecordCount, count);
        assertEquals(expectedRecordCount, callCount.get());
    }

    private void changeRecordingsState(final Catalog catalog, final int from, final int to)
    {
        IntStream.range(from, to).forEach(i -> catalog.changeState(recordingIds[i], INVALID));
    }

    private static List<Arguments> listRecordingsArguments()
    {
        return asList(
            arguments(Long.MAX_VALUE, 5, 0),
            arguments(-1, 10, 10),
            arguments(2, 10, 10),
            arguments(5, MAX_SCANS_PER_WORK_CYCLE, MAX_SCANS_PER_WORK_CYCLE),
            arguments(25, 100, 100),
            arguments(10, MAX_SCANS_PER_WORK_CYCLE * 2, MAX_SCANS_PER_WORK_CYCLE * 2),
            arguments(NUM_RECORDINGS - 10, MAX_SCANS_PER_WORK_CYCLE, 5));
    }

    private static List<Arguments> listRecordingsForUriArguments()
    {
        return asList(
            arguments(Long.MAX_VALUE, 5, ORIGINAL_CHANNELS[2], 2, 0),
            arguments(-1, 10, ORIGINAL_CHANNELS[2], 2, 10),
            arguments(2, 10, ORIGINAL_CHANNELS[2], 2, 10),
            arguments(-1, 10, ORIGINAL_CHANNELS[2], 0, 0),
            arguments(-1, 10, ORIGINAL_CHANNELS[1], 2, 0),
            arguments(5, MAX_SCANS_PER_WORK_CYCLE, ORIGINAL_CHANNELS[2], 2, MAX_SCANS_PER_WORK_CYCLE - 11),
            arguments(10, MAX_SCANS_PER_WORK_CYCLE * 2, ORIGINAL_CHANNELS[2], 2, MAX_SCANS_PER_WORK_CYCLE - 13),
            arguments(NUM_RECORDINGS - 10, MAX_SCANS_PER_WORK_CYCLE, ORIGINAL_CHANNELS[2], 2, 2));
    }
}
