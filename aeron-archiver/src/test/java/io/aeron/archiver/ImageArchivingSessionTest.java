/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;


import io.aeron.*;
import io.aeron.archiver.messages.ArchiveMetaFileFormatDecoder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.*;
import org.junit.*;
import org.mockito.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveFileUtil.archiveDataFileName;
import static io.aeron.archiver.ArchiveFileUtil.archiveMetaFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ImageArchivingSessionTest
{
    @Test
    public void shouldRecordDataFromImage() throws IOException
    {
        final int streamInstanceId = 12345;

        final String channel = "channel";
        final String source = "sourceIdentity";
        final int streamId = 54321;
        final int sessionId = 12345;


        final int initialTermId = 0;
        final int termBufferLength = 4096;

        final ArchiverConductor archive = mock(ArchiverConductor.class);
        final File tempFolderForTest = makeTempFolder();
        when(archive.archiveFolder()).thenReturn(tempFolderForTest);
        when(archive.notifyArchiveStarted(source, sessionId, channel, streamId)).thenReturn(streamInstanceId);

        final Subscription subscription = mockSubscription(channel, streamId);

        final Image image = mockImage(source, sessionId, initialTermId, termBufferLength, subscription);

        final File tempFile = File.createTempFile("archiver.test", "source");
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw"))
        {
            // size this file as a mock term buffer
            final FileChannel mockLogBuffer = raf.getChannel();
            mockLogBuffer.position(termBufferLength - 1);
            mockLogBuffer.write(ByteBuffer.wrap(new byte[1]));

            // write some data at term offset
            final ByteBuffer bb = ByteBuffer.allocate(100);
            final int termOffset = 1024;
            mockLogBuffer.position(termOffset);
            mockLogBuffer.write(bb);
            final UnsafeBuffer mockLogBufferMapped =
                    new UnsafeBuffer(mockLogBuffer.map(FileChannel.MapMode.READ_WRITE, 0, termBufferLength));

            // prep a single message in the log buffer
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.wrap(mockLogBufferMapped);
            headerFlyweight.headerType(DataHeaderFlyweight.HDR_TYPE_DATA).frameLength(100);

            final EpochClock epochClock = Mockito.mock(EpochClock.class);
            when(epochClock.time()).thenReturn(42L);
            final ImageArchivingSession session = new ImageArchivingSession(archive, image, epochClock);
            assertEquals(ImageArchivingSession.State.INIT, session.state());
            Assert.assertEquals(streamInstanceId, session.streamInstanceId());

            // setup the mock image to pass on the mock log buffer
            when(image.rawPoll(eq(session), anyInt())).
                thenAnswer(invocation ->
                {
                    session.onBlock(mockLogBuffer, 0, mockLogBufferMapped, termOffset, 100, sessionId, 0);
                    return 100;
                });

            // expecting session to archive the available data from the image
            final int work = session.doWork();
            Assert.assertNotEquals("Expect some work", 0, work);

            // We now evaluate the output of the archiver...

            // meta data exists and is as expected
            final File archiveMetaFile = new File(tempFolderForTest, archiveMetaFileName(session.streamInstanceId()));
            assertTrue(archiveMetaFile.exists());

            final ArchiveMetaFileFormatDecoder metaData =
                ArchiveFileUtil.archiveMetaFileFormatDecoder(archiveMetaFile);

            Assert.assertEquals(streamInstanceId, metaData.streamInstanceId());
            Assert.assertEquals(termBufferLength, metaData.termBufferLength());
            Assert.assertEquals(initialTermId, metaData.initialTermId());
            Assert.assertEquals(termOffset, metaData.initialTermOffset());
            Assert.assertEquals(initialTermId, metaData.lastTermId());
            Assert.assertEquals(streamId, metaData.streamId());
            Assert.assertEquals(termOffset + 100, metaData.lastTermOffset());
            Assert.assertEquals(42L, metaData.startTime());
            Assert.assertEquals(source, metaData.source());
            Assert.assertEquals(channel, metaData.channel());


            // data exists and is as expected
            final File archiveDataFile = new File(tempFolderForTest,
                                                  archiveDataFileName(session.streamInstanceId(), 0));
            assertTrue(archiveDataFile.exists());
            final ArchiveReader reader = new ArchiveReader(session.streamInstanceId(), tempFolderForTest);
            reader.forEachFragment((buffer, offset, length, header) ->
            {
                Assert.assertEquals(100, header.frameLength());
                Assert.assertEquals(termOffset + DataHeaderFlyweight.HEADER_LENGTH, offset);
                Assert.assertEquals(100 - DataHeaderFlyweight.HEADER_LENGTH, length);
            }, initialTermId, termOffset, 100);
        }

    }

    private Subscription mockSubscription(final String channel, final int streamId)
    {
        final Subscription subscription = mock(Subscription.class);
        when(subscription.channel()).thenReturn(channel);
        when(subscription.streamId()).thenReturn(streamId);
        return subscription;
    }

    private Image mockImage(final String sourceIdentity, final int sessionId, final int initialTermId, final int
        termBufferLength, final Subscription subscription)
    {
        final Image image = mock(Image.class);

        when(image.sessionId()).thenReturn(sessionId);
        when(image.sourceIdentity()).thenReturn(sourceIdentity);
        when(image.subscription()).thenReturn(subscription);
        when(image.initialTermId()).thenReturn(initialTermId);
        when(image.termBufferLength()).thenReturn(termBufferLength);
        return image;
    }

    static File makeTempFolder() throws IOException
    {
        final File tempFolderForTest = File.createTempFile("archiver.test", "tmp");
        // we really need a temp dir, not a file... delete and remake!
        assertTrue(tempFolderForTest.delete());
        assertTrue(tempFolderForTest.mkdir());
        return tempFolderForTest;
    }
}
