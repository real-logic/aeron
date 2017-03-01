/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PongTest
{
    public static final String PING_URI = "aeron:udp?endpoint=localhost:54325";
    public static final String PONG_URI = "aeron:udp?endpoint=localhost:54326";

    private static final int PING_STREAM_ID = 1;
    private static final int PONG_STREAM_ID = 2;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private final MediaDriver.Context context = new MediaDriver.Context();
    private final Aeron.Context pingAeronContext = new Aeron.Context();
    private final Aeron.Context pongAeronContext = new Aeron.Context();

    private Aeron pingClient;
    private Aeron pongClient;
    private MediaDriver driver;
    private Subscription pingSubscription;
    private Subscription pongSubscription;
    private Publication pingPublication;
    private Publication pongPublication;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private FragmentHandler pongHandler = mock(FragmentHandler.class);

    @Before
    public void setUp() throws Exception
    {
        context.threadingMode(THREADING_MODE);

        driver = MediaDriver.launch(context);
        pingClient = Aeron.connect(pingAeronContext);
        pongClient = Aeron.connect(pongAeronContext);

        pingPublication = pingClient.addPublication(PING_URI, PING_STREAM_ID);
        pingSubscription = pongClient.addSubscription(PING_URI, PING_STREAM_ID);

        pongPublication = pongClient.addPublication(PONG_URI, PONG_STREAM_ID);
        pongSubscription = pingClient.addSubscription(PONG_URI, PONG_STREAM_ID);
    }

    @After
    public void closeEverything() throws Exception
    {
        pongClient.close();
        pingClient.close();
        driver.close();

        context.deleteAeronDirectory();
    }

    @Test
    public void playPingPong()
    {
        buffer.putInt(0, 1);

        while (pingPublication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
        }

        final AtomicInteger fragmentsRead = new AtomicInteger();

        SystemTestHelper.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.addAndGet(pingSubscription.poll(this::pingHandler, 10));
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        SystemTestHelper.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.addAndGet(pongSubscription.poll(pongHandler, 10));
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

    public void pingHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // echoes back the ping
        while (pongPublication.offer(buffer, offset, length) < 0L)
        {
            Thread.yield();
        }
    }
}
