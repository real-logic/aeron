/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.response.ResponseClient;
import io.aeron.response.ResponseServer;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResponseChannelsTest
{
    private static final String REQUEST_ENDPOINT = "localhost:10000";
    private static final String RESPONSE_ENDPOINT = "localhost:10001";

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
                .errorHandler(Tests::onError)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .threadingMode(ThreadingMode.SHARED),
            watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    void shouldReceiveResponsesOnAPerClientBasis()
    {
        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ResponseServer responseServer = new ResponseServer(
                server, (image) -> new EchoHandler(), REQUEST_ENDPOINT, RESPONSE_ENDPOINT, null, null);
            ResponseClient responseClientA = new ResponseClient(clientA, REQUEST_ENDPOINT, null, null))
        {

        }
    }

    private static final class EchoHandler implements ResponseServer.ResponseHandler
    {
        public void onMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header,
            final Publication responsePublication)
        {
            while (0 > responsePublication.offer(buffer, offset, length))
            {
                Tests.yieldingIdle("failed to send response");
            }
        }
    }
}
