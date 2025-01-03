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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
public class MdsAndMdcInteractionTest
{
    private static final int STREAM_ID = 10000;
    private static final int SESSION_ID = 20000;
    private static final String CATCHUP_ENDPOINT = "aeron:udp?endpoint=localhost:20001|alias=catchup";
    private static final String LIVE_ENDPOINT_EARLY = "aeron:udp?endpoint=localhost:20002|alias=live";
    private static final String LIVE_ENDPOINT_LATE = "aeron:udp?endpoint=localhost:20003|alias=live";
    private static final String LIVE_URI = "aeron:udp?control-mode=manual|session-id=" + SESSION_ID;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED);
        context.enableExperimentalFeatures(true);

        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }


    @Test
    @InterruptAfter(15)
    void shouldSwitchFromCatchupToLive()
    {
        final UnsafeBuffer msg = new UnsafeBuffer("Hello World".getBytes(StandardCharsets.US_ASCII));
        final int initialTermId = 100;
        final int activeTermId = 100;
        final int termOffset = 0;
        final int termLength = 64 * 1024;
        final int positionBitsToShift = positionBitsToShift(termLength);
        final long position = computePosition(activeTermId, termOffset, positionBitsToShift, initialTermId);
        final int messageCount = 10;

        final String catchupUri = new ChannelUriStringBuilder(CATCHUP_ENDPOINT)
            .sessionId(SESSION_ID)
            .initialPosition(position, initialTermId, termLength)
            .build();

        final String liveUri = new ChannelUriStringBuilder(LIVE_URI)
            .flowControl("min,t:1s")
            .sessionId(SESSION_ID)
            .initialPosition(position, initialTermId, termLength)
            .build();

        try (Aeron follower = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron leader = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Subscription lateJoinSub = follower.addSubscription(LIVE_URI, STREAM_ID);
            Publication livePub = leader.addPublication(liveUri, STREAM_ID);
            Subscription earlySub = follower.addSubscription(LIVE_ENDPOINT_EARLY, STREAM_ID))
        {
            final Publication catchupPub = leader.addPublication(catchupUri, STREAM_ID);
            lateJoinSub.addDestination(CATCHUP_ENDPOINT);
            livePub.addDestination(LIVE_ENDPOINT_EARLY);
            livePub.addDestination(LIVE_ENDPOINT_LATE);

            Tests.awaitConnected(lateJoinSub);
            Tests.awaitConnected(earlySub);
            Tests.awaitConnected(catchupPub);
            Tests.awaitConnected(livePub);

            for (int i = 0; i < messageCount; i++)
            {
                while (catchupPub.offer(msg) < 0)
                {
                    Tests.yield();
                }

                while (livePub.offer(msg) < 0)
                {
                    Tests.yield();
                }
            }

            awaitMessages(lateJoinSub, messageCount);
            awaitMessages(earlySub, messageCount);

            lateJoinSub.addDestination(LIVE_ENDPOINT_LATE);

            while (2 != lateJoinSub.imageBySessionId(SESSION_ID).activeTransportCount())
            {
                Tests.yield();
            }

            lateJoinSub.removeDestination(CATCHUP_ENDPOINT);

            CloseHelper.quietClose(catchupPub);

            for (int i = 0; i < messageCount; i++)
            {
                while (livePub.offer(msg) < 0)
                {
                    Tests.yield();
                }
            }

            awaitMessages(lateJoinSub, messageCount);
            assertEquals(livePub.position(), lateJoinSub.imageBySessionId(SESSION_ID).position());
        }
    }

    @SuppressWarnings("methodlength")
    @Test
    @InterruptAfter(15)
    @SlowTest
    void shouldSwitchFromCatchupToLiveWhenRestartingClient()
    {
        final UnsafeBuffer msg = new UnsafeBuffer("Hello World".getBytes(StandardCharsets.US_ASCII));
        final int initialTermId = 100;
        final int activeTermId = 100;
        final int termOffset = 0;
        final int termLength = 64 * 1024;
        final int positionBitsToShift = positionBitsToShift(termLength);
        final long position = computePosition(activeTermId, termOffset, positionBitsToShift, initialTermId);
        final int messageCount = 10;

        final String liveUri = new ChannelUriStringBuilder(LIVE_URI)
            .flowControl("min,t:1s")
            .sessionId(SESSION_ID)
            .initialPosition(position, initialTermId, termLength)
            .build();

        try (Aeron leader = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication livePub = leader.addPublication(liveUri, STREAM_ID);
            Subscription earlySub = leader.addSubscription(LIVE_ENDPOINT_EARLY, STREAM_ID))
        {
            livePub.addDestination(LIVE_ENDPOINT_EARLY);
            livePub.addDestination(LIVE_ENDPOINT_LATE);
            Tests.awaitConnected(earlySub);

            final int lateJoinChannelStatusId;

            try (Aeron follower = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
                Subscription lateJoinSub = follower.addSubscription(LIVE_URI, STREAM_ID))
            {
                lateJoinSub.addDestination(LIVE_ENDPOINT_LATE);
                Tests.awaitConnected(lateJoinSub);
                Tests.awaitConnected(livePub);

                lateJoinChannelStatusId = lateJoinSub.channelStatusId();

                for (int i = 0; i < messageCount; i++)
                {
                    while (livePub.offer(msg) < 0)
                    {
                        Tests.yield();
                    }
                }

                awaitMessages(earlySub, messageCount);
                awaitMessages(lateJoinSub, messageCount);
            }

            while (!LocalSocketAddressStatus.findAddresses(
                leader.countersReader(), ChannelEndpointStatus.ACTIVE, lateJoinChannelStatusId).isEmpty())
            {
                Tests.yield();
            }

            final long restartPosition = livePub.position();

            for (int i = 0; i < messageCount; i++)
            {
                while (livePub.offer(msg) < 0)
                {
                    Tests.yield();
                }
            }

            awaitMessages(earlySub, messageCount);

            try (Aeron follower = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
                Subscription lateJoinSub = follower.addSubscription(LIVE_URI, STREAM_ID))
            {
                lateJoinSub.addDestination(CATCHUP_ENDPOINT);

                final String catchupUri = new ChannelUriStringBuilder(CATCHUP_ENDPOINT)
                    .sessionId(SESSION_ID)
                    .initialPosition(restartPosition, initialTermId, termLength)
                    .build();

                final Publication catchupPub = leader.addPublication(catchupUri, STREAM_ID);
                Tests.awaitConnected(lateJoinSub);

                for (int i = 0; i < messageCount; i++)
                {
                    while (catchupPub.offer(msg) < 0)
                    {
                        Tests.yield();
                    }
                }

                awaitMessages(lateJoinSub, messageCount);

                assertEquals(livePub.position(), lateJoinSub.imageBySessionId(SESSION_ID).position());

                lateJoinSub.addDestination(LIVE_ENDPOINT_LATE);

                while (2 != lateJoinSub.imageBySessionId(SESSION_ID).activeTransportCount())
                {
                    Tests.yield();
                }

                lateJoinSub.removeDestination(CATCHUP_ENDPOINT);

                for (int i = 0; i < messageCount; i++)
                {
                    while (livePub.offer(msg) < 0)
                    {
                        Tests.yield();
                    }
                }

                awaitMessages(earlySub, messageCount);
                awaitMessages(lateJoinSub, messageCount);
            }
        }
    }

    private void awaitMessages(final Subscription sub, final int count)
    {
        final MutableInteger messageCount = new MutableInteger(0);
        final Supplier<String> msg = () -> "expected=" + count + ", actual=" + messageCount.get();

        while (count != messageCount.get())
        {
            if (0 == sub.poll((buffer, offset, length, header) -> messageCount.increment(), 10))
            {
                Tests.yieldingIdle(msg);
            }
        }
    }

}
