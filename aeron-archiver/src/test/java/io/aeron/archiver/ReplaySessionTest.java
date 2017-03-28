/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.junit.*;
import org.mockito.Mockito;

import java.io.File;

import static io.aeron.archiver.TestUtil.makeTempFolder;
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

    private int messageIndex = 0;
    private ArchiverProtocolProxy proxy;

    @Before
    public void setup() throws Exception
    {
        archiveFolder = makeTempFolder();
        proxy = Mockito.mock(ArchiverProtocolProxy.class);
        final EpochClock epochClock = mock(EpochClock.class);
        final StreamInstance streamInstance = new StreamInstance("source", 1, "channel", 1);
        try (StreamInstanceArchiveWriter writer =
                 new StreamInstanceArchiveWriter(
                     archiveFolder,
                     epochClock,
                     STREAM_INSTANCE_ID,
                     TERM_BUFFER_LENGTH,
                     INITIAL_TERM_ID,
                     streamInstance))
        {
            when(epochClock.time()).thenReturn(42L);
            final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(TERM_BUFFER_LENGTH, 64));
            buffer.setMemory(0, TERM_BUFFER_LENGTH, (byte) 0);

            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.wrap(buffer, INITIAL_TERM_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
            headerFlyweight
                .termOffset(INITIAL_TERM_OFFSET)
                .termId(INITIAL_TERM_ID)
                .headerType(DataHeaderFlyweight.HDR_TYPE_DATA)
                .frameLength(1024);
            buffer.setMemory(
                INITIAL_TERM_OFFSET + DataHeaderFlyweight.HEADER_LENGTH,
                1024 - DataHeaderFlyweight.HEADER_LENGTH,
                (byte) 1);

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

        try (StreamInstanceArchiveFragmentReader reader =
            new StreamInstanceArchiveFragmentReader(STREAM_INSTANCE_ID, archiveFolder))
        {

            final int polled = reader.controlledPoll((b, offset, length, h) ->
            {
                Assert.assertEquals(offset, INITIAL_TERM_OFFSET + DataHeaderFlyweight.HEADER_LENGTH);
                Assert.assertEquals(length, 1024 - DataHeaderFlyweight.HEADER_LENGTH);
                return ControlledFragmentHandler.Action.CONTINUE;
            }, 1);
            Assert.assertEquals(1, polled);
        }
    }

    @After
    public void teardown()
    {
        IoUtil.delete(archiveFolder, false);
    }

    @Test
    public void shouldReplayDataFromFile()
    {
        final long length = 1024L;
        final ExclusivePublication replay = Mockito.mock(ExclusivePublication.class);
        final ExclusivePublication control = Mockito.mock(ExclusivePublication.class);
        final Image image = Mockito.mock(Image.class);

        final ReplaySession replaySession =
            new ReplaySession(
                STREAM_INSTANCE_ID,
                INITIAL_TERM_ID,
                INITIAL_TERM_OFFSET,
                length,
                replay,
                control,
                image,
                archiveFolder,
                proxy);
        when(replay.isClosed()).thenReturn(false);
        when(replay.isConnected()).thenReturn(false);
        when(control.isClosed()).thenReturn(false);
        when(control.isConnected()).thenReturn(false);
        // does not switch to replay mode until publications are established
        Assert.assertEquals(0, replaySession.doWork());

        when(replay.isConnected()).thenReturn(true);
        when(control.isConnected()).thenReturn(true);

        Assert.assertNotEquals(0, replaySession.doWork());
        Mockito.verify(proxy, times(1)).sendResponse(control, null);

        final UnsafeBuffer mockTermBuffer =
            new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64));
        when(replay.tryClaim(anyInt(), any(ExclusiveBufferClaim.class))).then(invocation ->
        {
            final int claimedSize = invocation.getArgument(0);
            final ExclusiveBufferClaim buffer = invocation.getArgument(1);
            buffer.wrap(mockTermBuffer, 0, claimedSize + DataHeaderFlyweight.HEADER_LENGTH);
            messageIndex++;
            return (long) claimedSize;
        });
        Assert.assertNotEquals(0, replaySession.doWork());
        Assert.assertTrue(messageIndex > 0);
        Assert.assertEquals(1024, mockTermBuffer.getInt(0));
        // TODO: add validation for reserved value and flags

        Assert.assertTrue(replaySession.isDone());

        Assert.assertEquals(0, replaySession.doWork());
    }
}
