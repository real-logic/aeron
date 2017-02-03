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


import io.aeron.Image;
import io.aeron.Subscription;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
        final String channel = "channel";
        final String sourceIdentity = "sourceIdentity";
        final int streamId = 54321;
        final int sessionId = 12345;
        final int initialTermId = 0;
        final int termBufferLength = 4096;

        final ArchiverConductor archive = mock(ArchiverConductor.class);
        final File tempFolderForTest = makeTempFolder();
        when(archive.archiveFolder()).thenReturn(tempFolderForTest);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.channel()).thenReturn(channel);
        when(subscription.streamId()).thenReturn(streamId);

        final Image image = mock(Image.class);

        when(image.sessionId()).thenReturn(sessionId);
        when(image.sourceIdentity()).thenReturn(sourceIdentity);
        when(image.subscription()).thenReturn(subscription);
        when(image.initialTermId()).thenReturn(initialTermId);
        when(image.termBufferLength()).thenReturn(termBufferLength);

        final File source = File.createTempFile("archiver.test", "source");
        try (RandomAccessFile raf = new RandomAccessFile(source, "rw"))
        {
            // size this file as a mock term buffer
            final FileChannel fileChannel = raf.getChannel();
            fileChannel.position(termBufferLength - 1);
            fileChannel.write(ByteBuffer.wrap(new byte[1]));

            // write some data at term offset
            final ByteBuffer bb = ByteBuffer.allocate(100);
            final int termOffset = 1024;
            fileChannel.position(termOffset);
            fileChannel.write(bb);
            final UnsafeBuffer ub =
                    new UnsafeBuffer(fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, termBufferLength));
            final ImageArchivingSession session = new ImageArchivingSession(archive, image);
            assertEquals(ImageArchivingSession.State.INIT, session.state());

            when(image.rawPoll(eq(session), anyInt())).
                thenAnswer(invocation ->
                {
                    session.onBlock(fileChannel, 0, ub, termOffset, 100, sessionId, 0);
                    return 100;
                });
            final int work = session.doWork();
            Assert.assertNotEquals("Expect some work", 0, work);

            // meta and data exist
            final File archiveMetaFile = new File(tempFolderForTest, archiveMetaFileName(session.streamInstanceId()));
            assertTrue(archiveMetaFile.exists());

            final File archiveDataFile = new File(tempFolderForTest,
                                                  archiveDataFileName(session.streamInstanceId(), 0));
            assertTrue(archiveDataFile.exists());
        }

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
