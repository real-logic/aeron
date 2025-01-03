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
package io.aeron.logbuffer;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static org.junit.jupiter.api.Assertions.*;

class TermGapFillerTest
{
    private static final int INITIAL_TERM_ID = 11;
    private static final int TERM_ID = 22;
    private static final int SESSION_ID = 333;
    private static final int STREAM_ID = 1007;

    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH));
    private final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_MIN_LENGTH));
    private final DataHeaderFlyweight dataFlyweight = new DataHeaderFlyweight(termBuffer);

    @BeforeEach
    void setup()
    {
        initialTermId(metaDataBuffer, INITIAL_TERM_ID);
        storeDefaultFrameHeader(metaDataBuffer, createDefaultHeader(SESSION_ID, STREAM_ID, INITIAL_TERM_ID));
    }

    @Test
    void shouldFillGapAtBeginningOfTerm()
    {
        final int gapOffset = 0;
        final int gapLength = 64;

        assertTrue(TermGapFiller.tryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

        assertEquals(gapLength, dataFlyweight.frameLength());
        assertEquals(gapLength, dataFlyweight.frameLength());
        assertEquals(gapOffset, dataFlyweight.termOffset());
        assertEquals(SESSION_ID, dataFlyweight.sessionId());
        assertEquals(TERM_ID, dataFlyweight.termId());
        assertEquals(PADDING_FRAME_TYPE, dataFlyweight.headerType());
        assertEquals(UNFRAGMENTED, (byte)(dataFlyweight.flags()));
    }

    @Test
    void shouldNotOverwriteExistingFrame()
    {
        final int gapOffset = 0;
        final int gapLength = 64;

        dataFlyweight.frameLength(32);

        assertFalse(TermGapFiller.tryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));
    }

    @Test
    void shouldFillGapAfterExistingFrame()
    {
        final int gapOffset = 128;
        final int gapLength = 64;

        dataFlyweight
            .sessionId(SESSION_ID)
            .termId(TERM_ID)
            .streamId(STREAM_ID)
            .flags(UNFRAGMENTED)
            .frameLength(gapOffset);
        dataFlyweight.setMemory(0, gapOffset - DataHeaderFlyweight.HEADER_LENGTH, (byte)'x');

        assertTrue(TermGapFiller.tryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

        dataFlyweight.wrap(termBuffer, gapOffset, termBuffer.capacity() - gapOffset);
        assertEquals(gapLength, dataFlyweight.frameLength());
        assertEquals(gapOffset, dataFlyweight.termOffset());
        assertEquals(SESSION_ID, dataFlyweight.sessionId());
        assertEquals(TERM_ID, dataFlyweight.termId());
        assertEquals(PADDING_FRAME_TYPE, dataFlyweight.headerType());
        assertEquals(UNFRAGMENTED, (byte)(dataFlyweight.flags()));
    }

    @Test
    void shouldFillGapBetweenExistingFrames()
    {
        final int gapOffset = 128;
        final int gapLength = 64;

        dataFlyweight
            .sessionId(SESSION_ID)
            .termId(TERM_ID)
            .termOffset(0)
            .streamId(STREAM_ID)
            .flags(UNFRAGMENTED)
            .frameLength(gapOffset)
            .setMemory(0, gapOffset - DataHeaderFlyweight.HEADER_LENGTH, (byte)'x');

        final int secondExistingFrameOffset = gapOffset + gapLength;
        dataFlyweight
            .wrap(termBuffer, secondExistingFrameOffset, termBuffer.capacity() - secondExistingFrameOffset);
        dataFlyweight
            .sessionId(SESSION_ID)
            .termId(TERM_ID)
            .termOffset(secondExistingFrameOffset)
            .streamId(STREAM_ID)
            .flags(UNFRAGMENTED)
            .frameLength(64);

        assertTrue(TermGapFiller.tryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

        dataFlyweight.wrap(termBuffer, gapOffset, termBuffer.capacity() - gapOffset);
        assertEquals(gapLength, dataFlyweight.frameLength());
        assertEquals(gapOffset, dataFlyweight.termOffset());
        assertEquals(SESSION_ID, dataFlyweight.sessionId());
        assertEquals(TERM_ID, dataFlyweight.termId());
        assertEquals(PADDING_FRAME_TYPE, dataFlyweight.headerType());
        assertEquals(UNFRAGMENTED, (byte)(dataFlyweight.flags()));
    }

    @Test
    void shouldFillGapAtEndOfTerm()
    {
        final int gapOffset = termBuffer.capacity() - 64;
        final int gapLength = 64;

        dataFlyweight
            .sessionId(SESSION_ID)
            .termId(TERM_ID)
            .streamId(STREAM_ID)
            .flags(UNFRAGMENTED)
            .frameLength(termBuffer.capacity() - gapOffset);
        dataFlyweight.setMemory(0, gapOffset - DataHeaderFlyweight.HEADER_LENGTH, (byte)'x');

        assertTrue(TermGapFiller.tryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

        dataFlyweight.wrap(termBuffer, gapOffset, termBuffer.capacity() - gapOffset);
        assertEquals(gapLength, dataFlyweight.frameLength());
        assertEquals(gapOffset, dataFlyweight.termOffset());
        assertEquals(SESSION_ID, dataFlyweight.sessionId());
        assertEquals(TERM_ID, dataFlyweight.termId());
        assertEquals(PADDING_FRAME_TYPE, dataFlyweight.headerType());
        assertEquals(UNFRAGMENTED, (byte)(dataFlyweight.flags()));
    }
}
