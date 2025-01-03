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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static org.junit.jupiter.api.Assertions.*;

class LogBufferDescriptorTest
{
    private final UnsafeBuffer metadataBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH));

    @Test
    void rotateLogIsAShouldCasActiveTermCountEvenWhenTermIdDoesNotMatch()
    {
        final int termId = 5;
        final int termCount = 1;
        rawTail(metadataBuffer, 2, packTail(termId, 1024));
        rawTail(metadataBuffer, 0, packTail(termId + 1, 2048));
        rawTail(metadataBuffer, 1, packTail(termId + 2, 4096));
        activeTermCount(metadataBuffer, termCount);

        assertTrue(rotateLog(metadataBuffer, termCount, termId));

        assertEquals(packTail(termId, 1024), rawTail(metadataBuffer, 2));
        assertEquals(packTail(termId + 1, 2048), rawTail(metadataBuffer, 0));
        assertEquals(packTail(termId + 2, 4096), rawTail(metadataBuffer, 1));
        assertEquals(termCount + 1, activeTermCount(metadataBuffer));
    }

    @Test
    void rotateLogIsAShouldCasActiveTermCountAfterSettingTailForTheNextTerm()
    {
        final int termId = 51;
        final int termCount = 19;
        rawTail(metadataBuffer, 1, packTail(termId, 1024));
        rawTail(metadataBuffer, 2, packTail(termId + 1 - PARTITION_COUNT, 2048));
        rawTail(metadataBuffer, 0, packTail(termId + 2 - PARTITION_COUNT, 4096));
        activeTermCount(metadataBuffer, termCount);

        assertTrue(rotateLog(metadataBuffer, termCount, termId));

        assertEquals(packTail(termId, 1024), rawTail(metadataBuffer, 1));
        assertEquals(packTail(termId + 1, 0), rawTail(metadataBuffer, 2));
        assertEquals(packTail(termId + 2 - PARTITION_COUNT, 4096), rawTail(metadataBuffer, 0));
        assertEquals(termCount + 1, activeTermCount(metadataBuffer));
    }

    @Test
    void rotateLogIsANoOpIfNeitherTailNorActiveTermCountCanBeChanged()
    {
        final int termId = 23;
        final int termCount = 42;
        rawTail(metadataBuffer, 0, packTail(termId, 1024));
        rawTail(metadataBuffer, 1, packTail(termId + 18, 2048));
        rawTail(metadataBuffer, 2, packTail(termId - 19, 4096));
        activeTermCount(metadataBuffer, termCount);

        assertFalse(rotateLog(metadataBuffer, 3, termId));

        assertEquals(packTail(termId, 1024), rawTail(metadataBuffer, 0));
        assertEquals(packTail(termId + 18, 2048), rawTail(metadataBuffer, 1));
        assertEquals(packTail(termId - 19, 4096), rawTail(metadataBuffer, 2));
        assertEquals(termCount, activeTermCount(metadataBuffer));
    }

    @ParameterizedTest
    @CsvSource({ "0,1376,0", "10,1024,64", "2048,2048,2080", "4096,1024,4224", "7997,992,8288" })
    void shouldComputeFragmentedFrameLength(
        final int length, final int maxPayloadLength, final int frameLength)
    {
        assertEquals(LogBufferDescriptor.computeFragmentedFrameLength(length, maxPayloadLength), frameLength);
    }
}
