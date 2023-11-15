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
import io.aeron.logbuffer.FragmentHandler;
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
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
public class ResponseChannelsTest
{
    private static final String REQUEST_ENDPOINT = "localhost:10000";
    private static final int REQUEST_STREAM_ID = 10000;
    private static final String RESPONSE_CONTROL = "localhost:10001";
    private static final int RESPONSE_STREAM_ID = 10001;

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
    void shouldReceiveResponsesOnAPerClientBasis() throws Exception
    {
        final String textA = "hello from client A";
        final String textB = "hello from client B";
        final MutableDirectBuffer messageA = new UnsafeBuffer(textA.getBytes(UTF_8));
        final MutableDirectBuffer messageB = new UnsafeBuffer(textB.getBytes(UTF_8));
        final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
        final List<String> responsesA = new ArrayList<>();
        final List<String> responsesB = new ArrayList<>();
        final FragmentHandler fragmentHandlerA =
            (buffer, offset, length, header) -> responsesA.add(buffer.getStringWithoutLengthUtf8(offset, length));
        final FragmentHandler fragmentHandlerB =
            (buffer, offset, length, header) -> responsesB.add(buffer.getStringWithoutLengthUtf8(offset, length));

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ResponseServer responseServer = new ResponseServer(
                server, (image) -> new EchoHandler(), REQUEST_ENDPOINT, REQUEST_STREAM_ID,
                RESPONSE_CONTROL, RESPONSE_STREAM_ID, null, null);
            ResponseClient responseClientA = new ResponseClient(
                clientA, fragmentHandlerA, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID);
            ResponseClient responseClientB = new ResponseClient(
                clientB, fragmentHandlerB, REQUEST_ENDPOINT, REQUEST_STREAM_ID, RESPONSE_CONTROL, RESPONSE_STREAM_ID))
        {
            final Supplier<String> msg = () -> "responseServer.sessionCount=" + responseServer.sessionCount() + " " +
                "clientA=" + responseClientA + " clientB=" + responseClientB;

            while (responseServer.sessionCount() < 2 ||
                !responseClientA.isConnected() ||
                !responseClientB.isConnected())
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus(msg);
            }

            while (0 > responseClientA.offer(messageA))
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus("unable to offer message to client A");
            }

            while (0 > responseClientB.offer(messageB))
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus("unable to offer message to client A");
            }

            while (!responsesA.contains(textA) || !responsesB.contains(textB))
            {
                idleStrategy.idle(run(responseServer, responseClientA, responseClientB));
                Tests.checkInterruptStatus("failed to receive responses");
            }

            assertEquals(1, responsesA.size());
            assertEquals(1, responsesB.size());
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldConnectResponseChannel()
    {
        final int reqStreamId = 10001;
        final int rspStreamId = 10002;

        try (Aeron server = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Subscription subReq = server.addSubscription(
                "aeron:udp?endpoint=localhost:10001", reqStreamId);
            Subscription subRsp = client.addSubscription(
                "aeron:udp?control-mode=response|control=localhost:10002", rspStreamId);
            Publication pubReq = client.addPublication(
                "aeron:udp?endpoint=localhost:10001|response-correlation-id=" + subRsp.registrationId(), reqStreamId))
        {
            Tests.awaitConnected(subReq);
            Tests.awaitConnected(pubReq);
            Objects.requireNonNull(subRsp);

            final Image image = subReq.imageAtIndex(0);
            final String url = "aeron:udp?control-mode=response|control=localhost:10002|response-correlation-id=" +
                image.correlationId();

            try (Publication pubRsp = client.addPublication(url, rspStreamId))
            {
                Tests.awaitConnected(subRsp);
                Tests.awaitConnected(pubRsp);

                try (Subscription sub2 = client.addSubscription(
                    "aeron:udp?control=localhost:10002|session-id=" + pubRsp.sessionId(), rspStreamId))
                {
                    Tests.awaitConnected(sub2);
                }
            }
        }
    }

    @Test
    @InterruptAfter(10)
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
            }
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldHandleMultiplePublicationsWithTheSameControlAddress()
    {
        final UnsafeBuffer message = new UnsafeBuffer("hello".getBytes(UTF_8));

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10000", 10001);
            Publication pubB = aeron.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10000", 10001);
            )
        {
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

    @Test
    @InterruptAfter(5)
    void shouldBeAbleToProcessMultipleTermsWithMultiplePublicationsWithTheSameControlAddress()
    {
        final UnsafeBuffer message = new UnsafeBuffer(new byte[4096]);
        message.setMemory(0, 4096, (byte)'x');
        final long stopPosition = 4 * 64 * 1024;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ExclusivePublication pubA = aeron.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10000|term-length=64k", 10001);
            ExclusivePublication pubB = aeron.addExclusivePublication(
                "aeron:udp?control-mode=response|control=localhost:10000|term-length=64k", 10001))
        {
            final String channelA = "aeron:udp?control=localhost:10000|session-id=" + pubA.sessionId();
            final String channelB = "aeron:udp?control=localhost:10000|session-id=" + pubB.sessionId();
            try (Subscription subA = aeron.addSubscription(channelA, 10001);
                Subscription subB = aeron.addSubscription(channelB, 10001))
            {
                Tests.awaitConnected(subA);
                Tests.awaitConnected(pubA);
                Tests.awaitConnected(subB);
                Tests.awaitConnected(pubB);

                final int termIdA = pubA.termId();
                final int termIdB = pubB.termId();

                long subAStopPosition = 0;
                long subBStopPosition = 0;

                final Supplier<String> errorMessage = () ->
                {
                    return "pubA.position=" + pubA.position() + ", subA.position=" + subA.imageAtIndex(0).position() +
                        ", pubB.position=" + pubB.position() + ", subB.position=" + subB.imageAtIndex(0).position();
                };

                while (0 == subAStopPosition || subA.imageAtIndex(0).position() >= subAStopPosition ||
                    0 == subBStopPosition || subB.imageAtIndex(0).position() >= subBStopPosition)
                {
                    if (pubA.position() < stopPosition)
                    {
                        if (0 > pubA.offer(message, 0, pubA.maxPayloadLength()))
                        {
                            Tests.yieldingIdle(errorMessage);
                        }
                    }
                    else if (0 == subAStopPosition)
                    {
                        subAStopPosition = pubA.position();
                    }

                    if (pubB.position() < stopPosition)
                    {
                        if (0 > pubB.offer(message, 0, pubB.maxPayloadLength()))
                        {
                            Tests.yieldingIdle(errorMessage);
                        }
                    }
                    else if (0 == subBStopPosition)
                    {
                        subBStopPosition = pubB.position();
                    }

                    if (0 == subA.poll((buffer, offset, length, header) -> {}, 10))
                    {
                        Tests.yieldingIdle(errorMessage);
                    }

                    if (0 == subB.poll((buffer, offset, length, header) -> {}, 10))
                    {
                        Tests.yieldingIdle(errorMessage);
                    }
                }

                assertThat(pubA.termId(), greaterThan(termIdA));
                assertThat(pubB.termId(), greaterThan(termIdB));
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

    private static int run(final Agent... agents) throws Exception
    {
        int work = 0;

        for (final Agent agent : agents)
        {
            work += agent.doWork();
        }

        return work;
    }
}
