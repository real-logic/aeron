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
import org.agrona.CloseHelper;
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
    private final int streamInstanceId = 12345;

    private final String channel = "channel";
    private final String source = "sourceIdentity";
    private final int streamId = 54321;
    private final int sessionId = 12345;


    private final int initialTermId = 0;
    private final int termBufferLength = 4096;
    private final int termOffset = 1024;

    private final File tempFolderForTest = makeTempFolder();
    private final ArchiverConductor archive;

    private final Subscription subscription;
    private final Image image;

    private RandomAccessFile logBufferRandomAccessFile;
    private FileChannel mockLogBufferChannel;
    private UnsafeBuffer mockLogBufferMapped;
    private File termFile;

    public ImageArchivingSessionTest() throws IOException
    {
        archive = mock(ArchiverConductor.class);
        when(archive.archiveFolder()).thenReturn(tempFolderForTest);
        when(archive.notifyArchiveStarted(source, sessionId, channel, streamId)).thenReturn(streamInstanceId);
        subscription = mockSubscription(channel, streamId);
        image = mockImage(source, sessionId, initialTermId, termBufferLength, subscription);
    }

    @Before
    public void setupMockTermBuff() throws IOException
    {
        termFile = File.createTempFile("archiver.test", "source");
        logBufferRandomAccessFile = new RandomAccessFile(termFile, "rw");
        // size this file as a mock term buffer
        mockLogBufferChannel = logBufferRandomAccessFile.getChannel();
        mockLogBufferChannel.position(termBufferLength - 1);
        mockLogBufferChannel.write(ByteBuffer.wrap(new byte[1]));

        // write some data at term offset
        final ByteBuffer bb = ByteBuffer.allocate(100);
        mockLogBufferChannel.position(termOffset);
        mockLogBufferChannel.write(bb);
        mockLogBufferMapped =
            new UnsafeBuffer(mockLogBufferChannel.map(FileChannel.MapMode.READ_WRITE, 0, termBufferLength));

        // prep a single message in the log buffer
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(mockLogBufferMapped);
        headerFlyweight.headerType(DataHeaderFlyweight.HDR_TYPE_DATA).frameLength(100);
    }

    @After
    public void teardownMockTermBuff()
    {
        CloseHelper.quietClose(logBufferRandomAccessFile);
        CloseHelper.quietClose(mockLogBufferChannel);
        for (String fn : tempFolderForTest.list())
        {
            new File(tempFolderForTest, fn).delete();
        }
        tempFolderForTest.delete();
        termFile.delete();
    }
    @Test
    public void shouldRecordDataFromImage() throws IOException
    {
        final EpochClock epochClock = Mockito.mock(EpochClock.class);
        when(epochClock.time()).thenReturn(42L);

        final ImageArchivingSession session = new ImageArchivingSession(archive, image, epochClock);
        assertEquals(ImageArchivingSession.State.INIT, session.state());
        Assert.assertEquals(streamInstanceId, session.streamInstanceId());

        // setup the mock image to pass on the mock log buffer
        when(image.rawPoll(eq(session), anyInt())).
            thenAnswer(invocation ->
            {
                session.onBlock(mockLogBufferChannel, 0, mockLogBufferMapped, termOffset, 100, sessionId, 0);
                return 100;
            });

        // expecting session to archive the available data from the image
        Assert.assertNotEquals("Expect some work", 0, session.doWork());

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
        Assert.assertEquals(-1L, metaData.endTime());
        Assert.assertEquals(source, metaData.source());
        Assert.assertEquals(channel, metaData.channel());


        // data exists and is as expected
        final File archiveDataFile = new File(tempFolderForTest,
                                              archiveDataFileName(session.streamInstanceId(), 0));
        assertTrue(archiveDataFile.exists());
        final ArchiveDataFileReader reader = new ArchiveDataFileReader(session.streamInstanceId(), tempFolderForTest);
        reader.forEachFragment((buffer, offset, length, header) ->
        {
            Assert.assertEquals(100, header.frameLength());
            Assert.assertEquals(termOffset + DataHeaderFlyweight.HEADER_LENGTH, offset);
            Assert.assertEquals(100 - DataHeaderFlyweight.HEADER_LENGTH, length);
        });

        // next poll has no data
        when(image.rawPoll(eq(session), anyInt())).thenReturn(0);
        Assert.assertEquals("Expect no work", 0, session.doWork());

        // image is closed
        when(image.isClosed()).thenReturn(true);
        Assert.assertNotEquals("Expect some work", 0, session.doWork());
        assertEquals(ImageArchivingSession.State.DONE, session.state());

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
