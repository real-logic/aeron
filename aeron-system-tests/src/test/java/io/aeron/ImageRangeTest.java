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
package io.aeron;

import io.aeron.driver.buffer.FileStoreLogFactory;
import io.aeron.driver.buffer.RawLog;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static io.aeron.logbuffer.LogBufferDescriptor.initialTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.isConnected;
import static io.aeron.logbuffer.LogBufferDescriptor.mtuLength;
import static io.aeron.logbuffer.LogBufferDescriptor.pageSize;
import static io.aeron.logbuffer.LogBufferDescriptor.termLength;
import static org.mockito.Mockito.mock;

class ImageRangeTest
{
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldHandleAllPossibleOffsets(final boolean useSparseFiles, final @TempDir File baseDir)
    {
        final int termBufferLength = 65536;
        final int filePageSize = 4096;
        final long subscriberPositionThatWillTriggerException = 3147497471L;
        final Position subscriberPosition = new AtomicLongPosition();
        final AtomicCounter bytesMappedCounter = mock(AtomicCounter.class);

        try (
            FileStoreLogFactory fileStoreLogFactory = new FileStoreLogFactory(
                baseDir.getAbsolutePath(),
                filePageSize,
                false,
                0,
                new RethrowingErrorHandler(),
                bytesMappedCounter);
            RawLog rawLog = fileStoreLogFactory.newImage(0, termBufferLength, useSparseFiles))
        {
            initialTermId(rawLog.metaData(), 0);
            mtuLength(rawLog.metaData(), 1408);
            termLength(rawLog.metaData(), termBufferLength);
            pageSize(rawLog.metaData(), filePageSize);
            isConnected(rawLog.metaData(), true);

            try (LogBuffers logBuffers = new LogBuffers(rawLog.fileName()))
            {
                final Image image = new Image(
                    null, 1, subscriberPosition, logBuffers, new RethrowingErrorHandler(), "127.0.0.1:123", 0);

                subscriberPosition.set(subscriberPositionThatWillTriggerException);

                image.boundedControlledPoll(
                    (buffer, offset, length, header) -> ControlledFragmentHandler.Action.COMMIT, 1024, 1);
                image.boundedPoll((buffer, offset, length, header) -> {}, 1024, 1);
            }
        }
    }
}
