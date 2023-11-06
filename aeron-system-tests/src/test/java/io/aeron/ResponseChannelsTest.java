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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

@ExtendWith(InterruptingTestCallback.class)
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
    @InterruptAfter(10)
    @Disabled
    void shouldReceiveResponsesOnAPerClientBasis()
    {
        final MutableDirectBuffer messageA = new UnsafeBuffer("hello from client A".getBytes(UTF_8));
        final MutableDirectBuffer messageB = new UnsafeBuffer("hello from client B".getBytes(UTF_8));

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ResponseServer responseServer = new ResponseServer(
                server, (image) -> new EchoHandler(), REQUEST_ENDPOINT, RESPONSE_ENDPOINT, null, null);
            ResponseClient responseClientA = new ResponseClient(clientA, REQUEST_ENDPOINT, null, null);
            ResponseClient responseClientB = new ResponseClient(clientB, REQUEST_ENDPOINT, null, null))
        {
            while (2 < responseServer.sessionCount() || !responseClientA.isConnected() || responseClientB.isConnected())
            {
                Tests.yieldingIdle("failed to connect server and clients");
            }

//            while (0 > responseClientA.offer(messageA))
//            {
//                Tests.yieldingIdle("unable to offer message to client A");
//            }
//
//            while (0 > responseClientB.offer(messageB))
//            {
//                Tests.yieldingIdle("unable to offer message to client B");
//            }
        }
    }

    @Test
    @InterruptAfter(10)
    @Disabled
    void shouldResolvePublicationImageViaCorrelationId()
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pub = aeron.addPublication("aeron:udp?endpoint=localhost:10000", 10001);
            Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:10000", 10001))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            final long correlationId = sub.imageAtIndex(0).correlationId();

            try (Publication pubA = aeron.addPublication(
                "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + correlationId,
                10001))
            {
                Objects.requireNonNull(pubA);
                Tests.sleep(5_000);
            }
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldMultiplePublicationsWithTheSameControlAddress()
    {
        final UnsafeBuffer message = new UnsafeBuffer("hello".getBytes(UTF_8));

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10000", 10001);
            Publication pubB = aeron.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10000", 10001);
            )
        {
            Objects.requireNonNull(pubA);
            Objects.requireNonNull(pubB);

            final String channelA = "aeron:udp?control=localhost:10000|session-id=" + pubA.sessionId();
            final String channelB = "aeron:udp?control=localhost:10000|session-id=" + pubB.sessionId();
            try (Subscription subA = aeron.addSubscription(channelA, 10001);
                Subscription subB = aeron.addSubscription(channelB, 10001))
            {
                Tests.awaitConnected(subA);
                Tests.awaitConnected(pubA);
                Tests.awaitConnected(subB);
                Tests.awaitConnected(pubB);

                while (0 > pubA.offer(message))
                {
                    Tests.sleep(1);
                }

                while (0 > pubB.offer(message))
                {
                    Tests.sleep(1);
                }

                int subACount = 0;
                int subBCount = 0;

                while (subACount < 1 || subBCount < 1)
                {
                    subACount += subA.poll((buffer, offset, length, header) -> {}, 10);
                    subBCount += subB.poll((buffer, offset, length, header) -> {}, 10);
                }

                final long deadlineMs = System.currentTimeMillis() + 3_000;
                while (System.currentTimeMillis() < deadlineMs)
                {
                    subACount += subA.poll((buffer, offset, length, header) -> {}, 10);
                    subBCount += subB.poll((buffer, offset, length, header) -> {}, 10);

                    assertThat(subACount, lessThan(2));
                    assertThat(subBCount, lessThan(2));
                }
            }
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
