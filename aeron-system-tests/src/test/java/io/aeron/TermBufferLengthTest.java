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

import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TermBufferLengthTest
{
    private static final int STREAM_ID = 1001;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @ParameterizedTest
    @CsvSource({
        "aeron:udp?endpoint=localhost:24325|term-length=64K, 65536",
        "aeron:ipc?term-length=64K, 65536",
        "aeron:udp?endpoint=localhost:24325|term-length=1G, 1073741824",
        "aeron:ipc?term-length=1G, 1073741824",
        "aeron:udp?endpoint=localhost:24325|term-length=4M, 4194304",
        "aeron:ipc?term-length=512K, 524288",
    })
    void shouldHaveCorrectTermBufferLength(final String channel, final int expectedTermBufferLength)
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH * 2)
            .ipcTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH * 2);

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(ctx, testWatcher))
        {
            testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());

            try (
                Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
                Publication publication = aeron.addPublication(channel, STREAM_ID))
            {
                assertEquals(expectedTermBufferLength, publication.termBufferLength());
            }
        }
    }
}
