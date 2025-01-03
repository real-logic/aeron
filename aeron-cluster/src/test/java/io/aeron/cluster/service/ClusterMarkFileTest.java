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
package io.aeron.cluster.service;

import io.aeron.cluster.codecs.mark.ClusterComponentType;
import org.agrona.MarkFile;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MAX_LENGTH;
import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.*;

class ClusterMarkFileTest
{
    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -100, ERROR_BUFFER_MIN_LENGTH - 1, ERROR_BUFFER_MAX_LENGTH + 1 })
    void throwsExceptionIfErrorBufferLengthIsInvalid(final int errorBufferLength, final @TempDir Path dir)
    {
        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class,
            () -> new ClusterMarkFile(
            dir.resolve("test.cfg").toFile(),
            ClusterComponentType.CONSENSUS_MODULE,
            errorBufferLength,
            SystemEpochClock.INSTANCE,
            10));
        assertEquals("Invalid errorBufferLength: " + errorBufferLength, exception.getMessage());
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldCallForceIfMarkFileIsNotClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verify(markFile).mappedByteBuffer();
            inOrder.verify(mappedByteBuffer).force();
            inOrder.verifyNoMoreInteractions();
        }
    }

    @Test
    @DisabledForJreRange(min = JRE.JAVA_21)
    void shouldNotCallForceIfMarkFileIsClosed()
    {
        final MarkFile markFile = mock(MarkFile.class);
        final MappedByteBuffer mappedByteBuffer = mock(MappedByteBuffer.class);
        when(markFile.mappedByteBuffer()).thenReturn(mappedByteBuffer);
        when(markFile.buffer()).thenReturn(new UnsafeBuffer(new byte[128]));
        when(markFile.isClosed()).thenReturn(true);
        try (ClusterMarkFile clusterMarkFile = new ClusterMarkFile(markFile))
        {
            clusterMarkFile.force();

            final InOrder inOrder = inOrder(markFile, mappedByteBuffer);
            inOrder.verify(markFile).isClosed();
            inOrder.verifyNoMoreInteractions();
        }
    }
}
