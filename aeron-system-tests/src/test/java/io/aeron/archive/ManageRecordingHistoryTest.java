/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ManageRecordingHistoryTest
{
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 2;
    private static final int STREAM_ID = 1033;
    private static final int MTU_LENGTH = 1024;

    private final ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .mtu(MTU_LENGTH)
        .termLength(TERM_LENGTH);

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @BeforeEach
    public void before()
    {
        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .publicationTermBufferLength(TERM_LENGTH)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .spiesSimulateConnection(true)
                .dirDeleteOnStart(true),
            testWatcher);

        archive = Archive.launch(
            new Archive.Context()
                .catalogCapacity(CATALOG_CAPACITY)
                .segmentFileLength(SEGMENT_LENGTH)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "archive"))
                .fileSyncLevel(0)
                .errorHandler(Tests::onError)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);

        archive.context().deleteDirectory();
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldPurgeForStreamJoinedAtTheBeginning()
    {
        final String messagePrefix = "Message-Prefix-";
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;

        try (Publication publication = aeronArchive.addRecordedPublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, counterId, publication.position());

            final long startPosition = 0L;
            final long segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
                startPosition, SEGMENT_LENGTH * 2L, TERM_LENGTH, SEGMENT_LENGTH);

            final long count = aeronArchive.purgeSegments(recordingId, segmentFileBasePosition);
            assertEquals(2L, count);
            assertEquals(segmentFileBasePosition, aeronArchive.getStartPosition(recordingId));

            aeronArchive.stopRecording(publication);
        }
    }

    @Test
    @Timeout(10)
    public void shouldPurgeForLateJoinedStream()
    {
        final String messagePrefix = "Message-Prefix-";
        final int initialTermId = 7;
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;
        final long startPosition = (TERM_LENGTH * 2L) + (FRAME_ALIGNMENT * 2L);
        uriBuilder.initialPosition(startPosition, initialTermId, TERM_LENGTH);

        try (Publication publication = aeronArchive.addRecordedExclusivePublication(uriBuilder.build(), STREAM_ID))
        {
            assertEquals(startPosition, publication.position());

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, counterId, publication.position());

            final long segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
                startPosition, startPosition + (SEGMENT_LENGTH * 2L), TERM_LENGTH, SEGMENT_LENGTH);

            final long purgeSegments = aeronArchive.purgeSegments(recordingId, segmentFileBasePosition);
            assertEquals(2L, purgeSegments);
            assertEquals(segmentFileBasePosition, aeronArchive.getStartPosition(recordingId));

            aeronArchive.stopRecording(publication);
        }
    }

    @Test
    @Timeout(10)
    public void shouldDetachThenAttachFullSegments()
    {
        final String messagePrefix = "Message-Prefix-";
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;

        try (Publication publication = aeronArchive.addRecordedPublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, counterId, publication.position());
            aeronArchive.stopRecording(publication);

            final long startPosition = 0L;
            final long segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
                startPosition, SEGMENT_LENGTH * 2L, TERM_LENGTH, SEGMENT_LENGTH);

            aeronArchive.detachSegments(recordingId, segmentFileBasePosition);
            assertEquals(segmentFileBasePosition, aeronArchive.getStartPosition(recordingId));

            final long attachSegments = aeronArchive.attachSegments(recordingId);
            assertEquals(2L, attachSegments);
            assertEquals(startPosition, aeronArchive.getStartPosition(recordingId));
        }
    }

    @Test
    @Timeout(10)
    public void shouldDetachThenAttachWhenStartNotSegmentAligned()
    {
        final String messagePrefix = "Message-Prefix-";
        final int initialTermId = 7;
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;
        final long startPosition = (TERM_LENGTH * 2L) + (FRAME_ALIGNMENT * 2L);
        uriBuilder.initialPosition(startPosition, initialTermId, TERM_LENGTH);

        try (Publication publication = aeronArchive.addRecordedExclusivePublication(uriBuilder.build(), STREAM_ID))
        {
            assertEquals(startPosition, publication.position());

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, counterId, publication.position());
            aeronArchive.stopRecording(publication);

            final long segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
                startPosition, startPosition + (SEGMENT_LENGTH * 2L), TERM_LENGTH, SEGMENT_LENGTH);

            aeronArchive.detachSegments(recordingId, segmentFileBasePosition);
            assertEquals(segmentFileBasePosition, aeronArchive.getStartPosition(recordingId));

            final long attachSegments = aeronArchive.attachSegments(recordingId);
            assertEquals(2L, attachSegments);
            assertEquals(startPosition, aeronArchive.getStartPosition(recordingId));
        }
    }

    @Test
    @Timeout(10)
    public void shouldMigrateSegmentsForStreamJoinedAtTheBeginning()
    {
        final String messagePrefix = "Message-Prefix-";
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;

        try (Publication publication = aeronArchive.addRecordedPublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int dstCounterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long dstRecordingId = RecordingPos.getRecordingId(counters, dstCounterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, dstCounterId, publication.position());
            aeronArchive.stopRecording(publication);

            final long startPosition = 0L;
            final long segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
                startPosition, SEGMENT_LENGTH * 2L, TERM_LENGTH, SEGMENT_LENGTH);

            final long count = aeronArchive.purgeSegments(dstRecordingId, segmentFileBasePosition);
            assertEquals(2L, count);
            assertEquals(segmentFileBasePosition, aeronArchive.getStartPosition(dstRecordingId));

            final long srcRecordingId;
            final String migrateChannel = uriBuilder
                .initialPosition(startPosition, publication.initialTermId(), TERM_LENGTH)
                .endpoint("localhost:4444")
                .build();

            try (Publication migratePub = aeronArchive.addRecordedExclusivePublication(migrateChannel, STREAM_ID))
            {
                final int srcCounterId = awaitRecordingCounterId(counters, migratePub.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(counters, srcCounterId);

                offerToPosition(migratePub, messagePrefix, segmentFileBasePosition);
                awaitPosition(counters, srcCounterId, migratePub.position());
                aeronArchive.stopRecording(migratePub);
            }

            aeronArchive.truncateRecording(srcRecordingId, segmentFileBasePosition);
            final long migratedSegments = aeronArchive.migrateSegments(srcRecordingId, dstRecordingId);
            assertEquals(2L, migratedSegments);
            assertEquals(startPosition, aeronArchive.getStartPosition(dstRecordingId));
        }
    }

    @Test
    @Timeout(10)
    public void shouldMigrateSegmentsForStreamNotSegmentAligned()
    {
        final String messagePrefix = "Message-Prefix-";
        final int initialTermId = 7;
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;
        final long startPosition = (TERM_LENGTH * 2L) + (FRAME_ALIGNMENT * 2L);
        uriBuilder.initialPosition(startPosition, initialTermId, TERM_LENGTH);

        try (Publication publication = aeronArchive.addRecordedExclusivePublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int dstCounterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long dstRecordingId = RecordingPos.getRecordingId(counters, dstCounterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, dstCounterId, publication.position());
            aeronArchive.stopRecording(publication);

            final long segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
                startPosition, startPosition + (SEGMENT_LENGTH * 2L), TERM_LENGTH, SEGMENT_LENGTH);

            final long purgedSegments = aeronArchive.purgeSegments(dstRecordingId, segmentFileBasePosition);
            assertEquals(2L, purgedSegments);
            assertEquals(segmentFileBasePosition, aeronArchive.getStartPosition(dstRecordingId));

            final long srcRecordingId;
            final String migrateChannel = uriBuilder
                .initialPosition(startPosition, initialTermId, TERM_LENGTH)
                .endpoint("localhost:4444")
                .build();

            try (Publication migratePub = aeronArchive.addRecordedExclusivePublication(migrateChannel, STREAM_ID))
            {
                final int srcCounterId = awaitRecordingCounterId(counters, migratePub.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(counters, srcCounterId);

                offerToPosition(migratePub, messagePrefix, segmentFileBasePosition);
                awaitPosition(counters, srcCounterId, migratePub.position());
                aeronArchive.stopRecording(migratePub);
            }

            aeronArchive.truncateRecording(srcRecordingId, segmentFileBasePosition);
            final long migratedSegments = aeronArchive.migrateSegments(srcRecordingId, dstRecordingId);
            assertEquals(2L, migratedSegments);
            assertEquals(startPosition, aeronArchive.getStartPosition(dstRecordingId));
            assertEquals(startPosition, aeronArchive.getStopPosition(srcRecordingId));
        }
    }

    @Test
    @Timeout(10)
    public void shouldPurgeRecording()
    {
        final String messagePrefix = "Message-Prefix-";
        final long targetPosition = (SEGMENT_LENGTH * 3L) + 1;

        try (Publication publication = aeronArchive.addRecordedPublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
            final long newRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offerToPosition(publication, messagePrefix, targetPosition);
            awaitPosition(counters, counterId, publication.position());

            final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
            final RecordingSignalConsumer consumer =
                (controlSessionId, correlationId, recordingId, subscriptionId, position, transitionType) ->
                {
                    if (newRecordingId == recordingId)
                    {
                        signalRef.set(transitionType);
                    }
                };

            final RecordingSignalAdapter adapter = new RecordingSignalAdapter(
                aeronArchive.controlSessionId(),
                ERROR_CONTROL_LISTENER,
                consumer,
                aeronArchive.controlResponsePoller().subscription(),
                FRAGMENT_LIMIT);

            aeronArchive.stopRecording(publication);
            assertEquals(RecordingSignal.STOP, awaitSignal(signalRef, adapter));

            aeronArchive.purgeRecording(newRecordingId);
            assertEquals(RecordingSignal.DELETE, awaitSignal(signalRef, adapter));
        }
    }
}
