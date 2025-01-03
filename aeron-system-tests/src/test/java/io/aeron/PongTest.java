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
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

class PongTest
{
    private static final String PING_URI = "aeron:udp?endpoint=localhost:24325";
    private static final String PONG_URI = "aeron:udp?endpoint=localhost:24326";

    private static final int PING_STREAM_ID = 1001;
    private static final int PONG_STREAM_ID = 1002;

    private Aeron pingClient;
    private Aeron pongClient;
    private TestMediaDriver driver;
    private Subscription pingSubscription;
    private Subscription pongSubscription;
    private Publication pingPublication;
    private Publication pongPublication;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final FragmentHandler pongHandler = mock(FragmentHandler.class);

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @BeforeEach
    void before()
    {
        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .errorHandler(Tests::onError)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .threadingMode(ThreadingMode.SHARED),
            testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        pingClient = Aeron.connect();
        pongClient = Aeron.connect();

        pingSubscription = pongClient.addSubscription(PING_URI, PING_STREAM_ID);
        pingPublication = pingClient.addPublication(PING_URI, PING_STREAM_ID);

        pongSubscription = pingClient.addSubscription(PONG_URI, PONG_STREAM_ID);
        pongPublication = pongClient.addPublication(PONG_URI, PONG_STREAM_ID);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(pongClient, pingClient, driver);
    }

    @Test
    void playPingPong()
    {
        buffer.putInt(0, 1);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        Tests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pingSubscription.poll(this::echoPingHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        Tests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pongSubscription.poll(pongHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        verify(pongHandler).onFragment(
            any(DirectBuffer.class),
            eq(DataHeaderFlyweight.HEADER_LENGTH),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void playPingPongWithRestart()
    {
        buffer.putInt(0, 1);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        Tests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pingSubscription.poll(this::echoPingHandler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        Tests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pongSubscription.poll(pongHandler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        // close Pong side
        pongPublication.close();
        pingSubscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (pingPublication.isConnected())
        {
            Tests.sleep(10);
        }

        // restart Pong side
        pingSubscription = pingClient.addSubscription(PING_URI, PING_STREAM_ID);
        pongPublication = pongClient.addPublication(PONG_URI, PONG_STREAM_ID);

        fragmentsRead.set(0);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yield();
        }

        Tests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pingSubscription.poll(this::echoPingHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        Tests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pongSubscription.poll(pongHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        verify(pongHandler, times(2)).onFragment(
            any(DirectBuffer.class),
            eq(DataHeaderFlyweight.HEADER_LENGTH),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    void echoPingHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        while (pongPublication.offer(buffer, offset, length) < 0L)
        {
            Tests.yield();
        }
    }
}
