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
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.Archive.Configuration.ARCHIVE_DIR_DEFAULT;

/**
 * Command line utility for creating a new Archive for migration testing and replay.
 * <p>
 * Creates 2 recordings, one starts at position 0 and the other starts in the second term.
 */
public class ArchiveCreator
{
    private static final String MESSAGE_PREFIX = "Message-Prefix-";
    private static final long CATALOG_CAPACITY = 128 * 1024;
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 2;
    private static final int STREAM_ID = 33;

    private static int recordingNumber = 0;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        final String archiveDirName = Archive.Configuration.archiveDirName();
        final File archiveDir =
            ARCHIVE_DIR_DEFAULT.equals(archiveDirName) ? new File("archive") : new File(archiveDirName);

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_LENGTH)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Throwable::printStackTrace)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        final Archive.Context archiveContext = new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(SEGMENT_LENGTH)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        System.out.println("Creating basic archive at " + archiveContext.archiveDir());

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverContext, archiveContext);
            Aeron aeron = Aeron.connect();
            AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(aeron)))
        {
            createRecording(
                aeron,
                aeronArchive,
                0,
                (SEGMENT_LENGTH * 5L) + 1);

            createRecording(
                aeron,
                aeronArchive,
                (long)TERM_LENGTH + (FrameDescriptor.FRAME_ALIGNMENT * 2),
                (SEGMENT_LENGTH * 3L) + 1);

        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void createRecording(
        final Aeron aeron, final AeronArchive aeronArchive, final long startPosition, final long targetPosition)
    {
        final int initialTermId = 7;
        recordingNumber++;
        final ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:" + recordingNumber)
            .termLength(TERM_LENGTH);

        if (startPosition > 0)
        {
            uriBuilder.initialPosition(startPosition, initialTermId, TERM_LENGTH);
        }

        try (Publication publication = aeronArchive.addRecordedExclusivePublication(uriBuilder.build(), STREAM_ID))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            System.out.println(
                "recordingId=" + recordingId +
                " position " + publication.position() +
                " to " + targetPosition);

            offerToPosition(publication, targetPosition);
            awaitPosition(counters, counterId, publication.position());

            aeronArchive.stopRecording(publication);
        }
    }

    private static void checkInterruptStatus()
    {
        if (Thread.interrupted())
        {
            LangUtil.rethrowUnchecked(new InterruptedException());
        }
    }

    private static int awaitRecordingCounterId(final CountersReader counters, final int sessionId, final long archiveId)
    {
        int counterId;
        while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId)))
        {
            Thread.yield();
            checkInterruptStatus();
        }

        return counterId;
    }

    private static void offerToPosition(final Publication publication, final long minimumPosition)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = 0; publication.position() < minimumPosition; i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Thread.yield();
                checkInterruptStatus();
            }
        }
    }

    private static void awaitPosition(final CountersReader counters, final int counterId, final long position)
    {
        while (counters.getCounterValue(counterId) < position)
        {
            if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
            {
                throw new IllegalStateException("count not active: " + counterId);
            }

            Thread.yield();
            checkInterruptStatus();
        }
    }
}
