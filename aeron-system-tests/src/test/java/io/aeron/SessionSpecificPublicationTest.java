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
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.aeron.CommonContext.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@ExtendWith(InterruptingTestCallback.class)
class SessionSpecificPublicationTest
{
    private static final String ENDPOINT = "localhost:24325";
    private static final int SESSION_ID_1 = 1077;
    private static final int SESSION_ID_2 = 1078;
    private static final int STREAM_ID = 1007;
    private static final int MTU_1 = 4096;
    private static final int MTU_2 = 8192;
    private static final int TERM_LENGTH_1 = 64 * 1024;
    private static final int TERM_LENGTH_2 = 128 * 1024;

    static Stream<ChannelUriStringBuilder> data()
    {
        return Stream.of(
            new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(ENDPOINT),
            new ChannelUriStringBuilder().media(IPC_MEDIA));
    }

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
    private final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
        .errorHandler(mockErrorHandler)
        .dirDeleteOnStart(true)
        .spiesSimulateConnection(true)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);

    private TestMediaDriver mediaDriver;
    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        mediaDriver = TestMediaDriver.launch(mediaDriverContext, testWatcher);
        testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());
        testWatcher.ignoreErrorsMatching(s -> true);

        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, mediaDriver);
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNotCreateExclusivePublicationWhenSessionIdCollidesWithExistingPublication(
        final ChannelUriStringBuilder channelBuilder)
    {
        final String channel = channelBuilder.build();
        aeron.addSubscription(channel, STREAM_ID);

        final Publication publication = aeron.addExclusivePublication(channel, STREAM_ID);
        Tests.awaitConnected(publication);

        final int existingSessionId = publication.sessionId();
        final String invalidChannel = channelBuilder.sessionId(existingSessionId).build();

        assertThrows(RegistrationException.class, () ->
        {
            aeron.addExclusivePublication(invalidChannel, STREAM_ID);

            fail("Exception should have been thrown due to duplicate session id");
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNotCreatePublicationsSharingSessionIdWithDifferentMtu(
        final ChannelUriStringBuilder channelBuilder)
    {
        channelBuilder.sessionId(SESSION_ID_1);

        assertThrows(RegistrationException.class, () ->
        {
            aeron.addPublication(channelBuilder.mtu(MTU_1).build(), STREAM_ID);
            aeron.addPublication(channelBuilder.mtu(MTU_2).build(), STREAM_ID);

            fail("Exception should have been thrown due to non-matching mtu");
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNotCreatePublicationsSharingSessionIdWithDifferentTermLength(
        final ChannelUriStringBuilder channelBuilder)
    {
        channelBuilder.sessionId(SESSION_ID_1);

        final String channelOne = channelBuilder.termLength(TERM_LENGTH_1).build();
        final String channelTwo = channelBuilder.termLength(TERM_LENGTH_2).build();

        assertThrows(RegistrationException.class, () ->
        {
            aeron.addPublication(channelOne, STREAM_ID);
            aeron.addPublication(channelTwo, STREAM_ID);

            fail("Exception should have been thrown due to non-matching term length");
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldNotCreateNonExclusivePublicationsWithDifferentSessionIdsForTheSameEndpoint(
        final ChannelUriStringBuilder channelBuilder)
    {
        channelBuilder.endpoint(ENDPOINT);

        final String channelOne = channelBuilder.sessionId(SESSION_ID_1).build();
        final String channelTwo = channelBuilder.sessionId(SESSION_ID_2).build();

        assertThrows(RegistrationException.class, () ->
        {
            aeron.addPublication(channelOne, STREAM_ID);
            aeron.addPublication(channelTwo, STREAM_ID);

            fail("Exception should have been thrown due using different session ids");
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    @InterruptAfter(20)
    @SlowTest
    void shouldNotAddPublicationWithSameSessionUntilLingerCompletes(final ChannelUriStringBuilder builder)
    {
        final DirectBuffer msg = new UnsafeBuffer(new byte[8]);
        final String channel = builder.sessionId(SESSION_ID_1).build();

        final String subscriptionChannel = "ipc".equals(builder.media()) ? channel : SPY_PREFIX + channel;

        final Publication publication1 = aeron.addPublication(channel, STREAM_ID);
        final Subscription subscription = aeron.addSubscription(subscriptionChannel, STREAM_ID);
        final int positionLimitId = publication1.positionLimitId();
        assertEquals(CountersReader.RECORD_ALLOCATED, aeron.countersReader().getCounterState(positionLimitId));

        while (publication1.offer(msg) < 0)
        {
            Tests.yieldingIdle("Failed to offer message");
        }

        publication1.close();

        assertThrows(RegistrationException.class, () ->
        {
            aeron.addPublication(channel, STREAM_ID);

            fail("Exception should have been thrown due lingering publication keeping session id active");
        });

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {};
        while (subscription.poll(fragmentHandler, 10) <= 0)
        {
            Tests.yieldingIdle("Failed to drain message");
        }
        subscription.close();

        while (CountersReader.RECORD_ALLOCATED == aeron.countersReader().getCounterState(positionLimitId))
        {
            Tests.yieldingIdle("Publication never cleaned up");
        }

        aeron.addPublication(channel, STREAM_ID);
    }

    @ParameterizedTest
    @MethodSource("data")
    void shouldAllowTheSameSessionIdOnDifferentStreamIds(final ChannelUriStringBuilder channelBuilder)
    {
        final String channel = channelBuilder.sessionId(SESSION_ID_1).build();

        aeron.addPublication(channel, STREAM_ID);
        aeron.addPublication(channel, STREAM_ID + 1);
    }
}
