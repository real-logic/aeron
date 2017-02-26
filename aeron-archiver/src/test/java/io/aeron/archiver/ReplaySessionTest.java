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
import io.aeron.archiver.messages.*;
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.junit.*;
import org.mockito.Mockito;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ImageArchivingSessionTest.makeTempFolder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ReplaySessionTest
{
    private static final int STREAM_INSTANCE_ID = 0;
    private static final long START_TIME = 42L;
    private static final int TERM_BUFFER_LENGTH = 4096;
    private static final int INITIAL_TERM_ID = 8231773;
    private static final int INITIAL_TERM_OFFSET = 1024;
    private File archiveFolder;
    private RandomAccessFile metaDataFile;
    private FileChannel metadataFileChannel;
    private MappedByteBuffer metaDataBuffer;
    private ArchiveMetaFileFormatEncoder metaDataWriter;
    private ArchiverConductor conductor;
    private int polled;

    @Before
    public void setup() throws Exception
    {
        archiveFolder = makeTempFolder();
        conductor = Mockito.mock(ArchiverConductor.class);
        when(conductor.archiveFolder()).thenReturn(archiveFolder);

        final EpochClock epochClock = mock(EpochClock.class);
        final StreamInstance streamInstance = new StreamInstance("source", 1, "channel", 1);
        try (StreamInstanceArchiveWriter writer = new StreamInstanceArchiveWriter(
            archiveFolder, epochClock, STREAM_INSTANCE_ID, TERM_BUFFER_LENGTH, streamInstance))
        {
            when(epochClock.time()).thenReturn(42L);
            final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(TERM_BUFFER_LENGTH, 64));
            buffer.setMemory(0, TERM_BUFFER_LENGTH, (byte)0);

            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.wrap(buffer, INITIAL_TERM_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
            headerFlyweight
                .termOffset(INITIAL_TERM_OFFSET)
                .termId(INITIAL_TERM_ID)
                .headerType(DataHeaderFlyweight.HDR_TYPE_DATA)
                .frameLength(1024);
            buffer.setMemory(INITIAL_TERM_OFFSET + DataHeaderFlyweight.HEADER_LENGTH,
                1024 - DataHeaderFlyweight.HEADER_LENGTH,
                (byte)1);

            final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
            header.buffer(buffer);
            header.offset(INITIAL_TERM_OFFSET);
            Assert.assertEquals(1024, header.frameLength());
            Assert.assertEquals(INITIAL_TERM_ID, header.termId());
            Assert.assertEquals(INITIAL_TERM_OFFSET, header.offset());

            writer.onFragment(
                buffer,
                header.offset() + DataHeaderFlyweight.HEADER_LENGTH,
                header.frameLength() - DataHeaderFlyweight.HEADER_LENGTH,
                header);

            when(epochClock.time()).thenReturn(84L);
        }

        try (StreamInstanceArchiveChunkReader chunkReader =
                 new StreamInstanceArchiveChunkReader(
                     STREAM_INSTANCE_ID,
                     archiveFolder,
                     INITIAL_TERM_ID,
                     TERM_BUFFER_LENGTH,
                     INITIAL_TERM_ID,
                     INITIAL_TERM_OFFSET,
                     1024))
        {
            chunkReader.readChunk((termBuff, termOffset, length) ->
            {
                final DataHeaderFlyweight hf = new DataHeaderFlyweight();
                hf.wrap(termBuff, termOffset, DataHeaderFlyweight.HEADER_LENGTH);
                Assert.assertEquals(INITIAL_TERM_ID, hf.termId());
                Assert.assertEquals(1024, hf.frameLength());
                return true;
            }, 1024);
        }
        final StreamInstanceArchiveFragmentReader reader =
            new StreamInstanceArchiveFragmentReader(STREAM_INSTANCE_ID, archiveFolder);

        final int polled = reader.poll((b, offset, length, h) ->
        {
            Assert.assertEquals(offset, INITIAL_TERM_OFFSET + DataHeaderFlyweight.HEADER_LENGTH);
            Assert.assertEquals(length, 1024 - DataHeaderFlyweight.HEADER_LENGTH);
        });
        Assert.assertEquals(1, polled);
    }

    @After
    public void teardown()
    {
        IoUtil.unmap(metaDataBuffer);
        CloseHelper.quietClose(metadataFileChannel);
        CloseHelper.quietClose(metaDataFile);
        IoUtil.delete(archiveFolder, true);
    }

    int messageIndex = 0;

    @Test
    public void shouldReplayDataFromFile()
    {
        final long length = 1024L;
        final Publication reply = Mockito.mock(Publication.class);
        final Image image = Mockito.mock(Image.class);

        final ReplaySession replaySession =
            new ReplaySession(
                STREAM_INSTANCE_ID,
                INITIAL_TERM_ID,
                INITIAL_TERM_OFFSET,
                length,
                reply,
                image,
                conductor);
        when(reply.isClosed()).thenReturn(false);
        when(reply.isConnected()).thenReturn(false);
        Assert.assertEquals(0, replaySession.doWork());

        // does not switch to replay mode until publications are established
        Assert.assertEquals(ReplaySession.State.INIT, replaySession.state());

        when(reply.isConnected()).thenReturn(true);

        Assert.assertNotEquals(0, replaySession.doWork());
        Mockito.verify(conductor, times(1)).sendResponse(reply, null);
        Assert.assertEquals(ReplaySession.State.REPLAY, replaySession.state());

        final UnsafeBuffer mockTermBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64));
        when(reply.tryClaim(anyInt(), any(BufferClaim.class))).then(invocation ->
        {
            final int claimedSize = invocation.getArgument(0);
            final BufferClaim buffer = invocation.getArgument(1);
            buffer.wrap(mockTermBuffer, 0, claimedSize + DataHeaderFlyweight.HEADER_LENGTH);
            messageIndex++;
            return (long)claimedSize;
        });
        when(reply.maxPayloadLength()).thenReturn(4096);
        Assert.assertNotEquals(0, replaySession.doWork());
        Assert.assertTrue(messageIndex > 0);
        Assert.assertEquals(ReplaySession.REPLAY_DATA_HEADER,
            mockTermBuffer.getLong(DataHeaderFlyweight.HEADER_LENGTH));
        Assert.assertEquals(1024, mockTermBuffer.getInt(0) -
            (DataHeaderFlyweight.HEADER_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH));
        Assert.assertEquals(ReplaySession.State.CLOSE, replaySession.state());

        Assert.assertNotEquals(0, replaySession.doWork());
        Assert.assertEquals(ReplaySession.State.DONE, replaySession.state());

        Assert.assertEquals(0, replaySession.doWork());
    }
}
