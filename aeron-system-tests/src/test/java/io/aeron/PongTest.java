/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PongTest
{
    private static final String PING_URI = "aeron:udp?endpoint=localhost:54325";
    private static final String PONG_URI = "aeron:udp?endpoint=localhost:54326";

    private static final int PING_STREAM_ID = 1;
    private static final int PONG_STREAM_ID = 2;

    private Aeron pingClient;
    private Aeron pongClient;
    private MediaDriver driver;
    private Subscription pingSubscription;
    private Subscription pongSubscription;
    private Publication pingPublication;
    private Publication pongPublication;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final FragmentHandler pongHandler = mock(FragmentHandler.class);

    @Before
    public void before()
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .threadingMode(ThreadingMode.SHARED));

        pingClient = Aeron.connect();
        pongClient = Aeron.connect();

        pingSubscription = pongClient.addSubscription(PING_URI, PING_STREAM_ID);
        pingPublication = pingClient.addPublication(PING_URI, PING_STREAM_ID);

        pongSubscription = pingClient.addSubscription(PONG_URI, PONG_STREAM_ID);
        pongPublication = pongClient.addPublication(PONG_URI, PONG_STREAM_ID);
    }

    @After
    public void after()
    {
        CloseHelper.close(pongClient);
        CloseHelper.close(pingClient);
        CloseHelper.close(driver);

        driver.context().deleteAeronDirectory();
    }

    @Test
    public void playPingPong()
    {
        buffer.putInt(0, 1);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTest.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pingSubscription.poll(this::echoPingHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        SystemTest.executeUntil(
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

    @Ignore
    @Test
    public void playPingPongWithRestart() throws Exception
    {
        buffer.putInt(0, 1);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTest.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pingSubscription.poll(this::echoPingHandler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        SystemTest.executeUntil(
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
            SystemTest.checkInterruptedStatus();
            Thread.sleep(100);
        }

        // restart Pong side
        pingSubscription = pingClient.addSubscription(PING_URI, PING_STREAM_ID);
        pongPublication = pongClient.addPublication(PONG_URI, PONG_STREAM_ID);

        fragmentsRead.set(0);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        SystemTest.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += pingSubscription.poll(this::echoPingHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        SystemTest.executeUntil(
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

    public void echoPingHandler(
        final DirectBuffer buffer, final int offset, final int length, @SuppressWarnings("unused") final Header header)
    {
        while (pongPublication.offer(buffer, offset, length) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }
}
