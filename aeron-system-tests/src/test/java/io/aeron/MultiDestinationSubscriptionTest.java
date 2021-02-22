/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.*;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class MultiDestinationSubscriptionTest
{
    private static final String UNICAST_ENDPOINT_A = "localhost:24325";
    private static final String UNICAST_ENDPOINT_B = "localhost:24326";

    private static final String PUB_UNICAST_URI = "aeron:udp?endpoint=localhost:24325";
    private static final String PUB_MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
    private static final String PUB_MDC_URI = "aeron:udp?control=localhost:24325|control-mode=dynamic";

    private static final String SUB_URI = "aeron:udp?control-mode=manual";
    private static final String SUB_MDC_DESTINATION_URI = "aeron:udp?endpoint=localhost:24326|control=localhost:24325";

    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR = CommonContext.getAeronDirectoryName() + File.separator;

    private final MediaDriver.Context driverContextA = new MediaDriver.Context();
    private final MediaDriver.Context driverContextB = new MediaDriver.Context();

    private Aeron clientA;
    private Aeron clientB;
    private TestMediaDriver driverA;
    private TestMediaDriver driverB;
    private Publication publicationA;
    private Publication publicationB;
    private Subscription subscription;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final FragmentHandler copyFragmentHandler = mock(FragmentHandler.class);

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private void launch(final ErrorHandler errorHandler)
    {
        final String baseDirA = ROOT_DIR + "A";

        buffer.putInt(0, 1);

        driverContextA
            .errorHandler(errorHandler)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .threadingMode(ThreadingMode.SHARED);

        driverA = TestMediaDriver.launch(driverContextA, testWatcher);
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverContextA.aeronDirectoryName()));
    }

    private void launchSecond()
    {
        final String baseDirB = ROOT_DIR + "B";

        driverContextB
            .errorHandler(Tests::onError)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .threadingMode(ThreadingMode.SHARED);

        driverB = TestMediaDriver.launch(driverContextB, testWatcher);
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverContextB.aeronDirectoryName()));
    }

    @AfterEach
    public void closeEverything()
    {
        CloseHelper.closeAll(clientA, clientB, driverA, driverB);
        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test
    @Timeout(10)
    public void subscriptionCloseShouldAlsoCloseMediaDriverPorts()
    {
        launch(Tests::onError);

        final String publicationChannelA = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint(UNICAST_ENDPOINT_A)
            .build();

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(publicationChannelA);

        CloseHelper.closeAll(subscription, clientA);

        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverContextA.aeronDirectoryName()));
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(publicationChannelA);
    }

    @Test
    @Timeout(10)
    public void addDestinationWithSpySubscriptionsShouldFailWithRegistrationException()
    {
        final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
        launch(mockErrorHandler);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        final RegistrationException registrationException = assertThrows(
            RegistrationException.class, () -> subscription.addDestination("aeron-spy:" + SUB_MDC_DESTINATION_URI));

        assertThat(registrationException.getMessage(), containsString("spies are invalid"));
    }

    @Test
    @Timeout(10)
    public void shouldSpinUpAndShutdownWithUnicast()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
    }

    @Test
    @Timeout(10)
    public void shouldSpinUpAndShutdownWithMulticast()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        final long correlationId = subscription.asyncAddDestination(PUB_MULTICAST_URI);

        publicationA = clientA.addPublication(PUB_MULTICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        assertFalse(clientA.isCommandActive(correlationId));
    }

    @Test
    @Timeout(20)
    public void shouldSpinUpAndShutdownWithDynamicMdc()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(SUB_MDC_DESTINATION_URI);

        publicationA = clientA.addPublication(PUB_MDC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
    }

    @Test
    @Timeout(10)
    public void shouldSendToSingleDestinationSubscriptionWithUnicast()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    @Test
    @Timeout(10)
    @SlowTest
    public void shouldAllowMultipleMdsSubscriptions()
    {
        final String unicastUri2 = "aeron:udp?endpoint=localhost:24326";

        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        try (Subscription subscriptionB = clientA.addSubscription(SUB_URI, STREAM_ID))
        {
            subscriptionB.addDestination(unicastUri2);

            publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);
            publicationB = clientA.addPublication(unicastUri2, STREAM_ID);

            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0)
            {
                Tests.yield();
            }

            while (subscription.poll(fragmentHandler, 1) <= 0)
            {
                Tests.yield();
            }

            // Wait a bit to ensure a message doesn't arrive.
            Tests.sleep(1000);

            assertEquals(0, subscriptionB.poll(fragmentHandler, 1));

            while (publicationB.offer(buffer, 0, buffer.capacity()) < 0)
            {
                Tests.yield();
            }

            while (subscriptionB.poll(fragmentHandler, 1) <= 0)
            {
                Tests.yield();
            }

            // Wait a bit to ensure a message doesn't arrive.
            Tests.sleep(1000);

            assertEquals(0, subscription.poll(fragmentHandler, 1));
        }
    }

    @Test
    @Timeout(10)
    public void shouldFindMdsSubscriptionWithTags()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI + "|tags=1001", STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        try (Subscription ignore = clientA.addSubscription(SUB_URI + "|tags=1002", STREAM_ID);
            Subscription subscriptionA1 = clientA.addSubscription("aeron:udp?tags=1001", STREAM_ID))
        {
            publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0)
            {
                Tests.yield();
            }

            while (subscriptionA1.poll(fragmentHandler, 1) <= 0)
            {
                Tests.yield();
            }
        }
    }

    @Test
    @Timeout(10)
    @SlowTest
    public void shouldAllowMultipleMdsSubscriptionsWithTags()
    {
        final String unicastUri2 = "aeron:udp?endpoint=localhost:24326";

        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI + "|tags=1001", STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        try (Subscription subscriptionB = clientA.addSubscription(SUB_URI + "|tags=1002", STREAM_ID);
            Subscription subscriptionA1 = clientA.addSubscription("aeron:udp?tags=1001", STREAM_ID);
            Subscription subscriptionB1 = clientA.addSubscription("aeron:udp?tags=1002", STREAM_ID))
        {
            subscriptionB.addDestination(unicastUri2);

            publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);
            publicationB = clientA.addPublication(unicastUri2, STREAM_ID);

            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0)
            {
                Tests.yield();
            }

            while (subscriptionA1.poll(fragmentHandler, 1) <= 0)
            {
                Tests.yield();
            }

            // Wait a bit to ensure a message doesn't arrive.
            Tests.sleep(1000);

            assertEquals(0, subscriptionB1.poll(fragmentHandler, 1));

            while (publicationB.offer(buffer, 0, buffer.capacity()) < 0)
            {
                Tests.yield();
            }

            while (subscriptionB1.poll(fragmentHandler, 1) <= 0)
            {
                Tests.yield();
            }

            // Wait a bit to ensure a message doesn't arrive.
            Tests.sleep(1000);

            assertEquals(0, subscriptionA1.poll(fragmentHandler, 1));
        }
    }

    @Test
    @Timeout(10)
    public void shouldSendToSingleDestinationMultipleSubscriptionsWithUnicast()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        final String tags = "1,2";

        launch(Tests::onError);

        final String subscriptionChannel = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .tags(tags)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .build();

        subscription = clientA.addSubscription(subscriptionChannel, STREAM_ID);
        final Subscription copySubscription = clientA.addSubscription(subscriptionChannel, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);
            pollForFragment(copySubscription, copyFragmentHandler);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
        verifyFragments(copyFragmentHandler, numMessagesToSend);
    }

    @Test
    @Timeout(10)
    public void shouldSendToSingleDestinationSubscriptionWithMulticast()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_MULTICAST_URI);

        publicationA = clientA.addPublication(PUB_MULTICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    @Test
    @Timeout(20)
    public void shouldSendToSingleDestinationSubscriptionWithDynamicMdc()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(SUB_MDC_DESTINATION_URI);

        publicationA = clientA.addPublication(PUB_MDC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    @Test
    @Timeout(10)
    public void shouldSendToMultipleDestinationSubscriptionWithSameStream()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        final int numMessagesToSendForA = numMessagesToSend / 2;
        final int numMessagesToSendForB = numMessagesToSend / 2;
        final String tags = "1,2";
        final int pubTag = 2;

        launch(Tests::onError);

        final String publicationChannelA = new ChannelUriStringBuilder()
            .tags(tags)
            .media(CommonContext.UDP_MEDIA)
            .endpoint(UNICAST_ENDPOINT_A)
            .build();

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(publicationChannelA);

        publicationA = clientA.addPublication(publicationChannelA, STREAM_ID);

        Tests.awaitConnected(subscription);

        for (int i = 0; i < numMessagesToSendForA; i++)
        {
            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);
        }

        final long position = publicationA.position();
        final int initialTermId = publicationA.initialTermId();
        final int positionBitsToShift = Long.numberOfTrailingZeros(publicationA.termBufferLength());
        final int termId = LogBufferDescriptor.computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
        final int termOffset = (int)(position & (publicationA.termBufferLength() - 1));

        final String publicationChannelB = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .isSessionIdTagged(true)
            .sessionId(pubTag)
            .initialTermId(initialTermId)
            .termId(termId)
            .termOffset(termOffset)
            .endpoint(UNICAST_ENDPOINT_B)
            .build();

        publicationB = clientA.addExclusivePublication(publicationChannelB, STREAM_ID);

        final String destinationChannel = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint(UNICAST_ENDPOINT_B)
            .build();

        subscription.addDestination(destinationChannel);

        for (int i = 0; i < numMessagesToSendForB; i++)
        {
            while (publicationB.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);
        }

        assertEquals(1, subscription.imageCount());
        assertEquals(2, subscription.imageAtIndex(0).activeTransportCount());
        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    @Test
    @Timeout(10)
    public void shouldMergeStreamsFromMultiplePublicationsWithSameParams()
    {
        final int numMessagesToSend = 30;
        final int numMessagesToSendForA = numMessagesToSend / 2;
        final int numMessagesToSendForB = numMessagesToSend / 2;

        launch(Tests::onError);
        launchSecond();

        final String publicationChannelA = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint(UNICAST_ENDPOINT_A)
            .build();

        final String destinationB = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint(UNICAST_ENDPOINT_B)
            .build();

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(publicationChannelA);
        subscription.addDestination(destinationB);

        publicationA = clientA.addExclusivePublication(publicationChannelA, STREAM_ID);

        final String publicationChannelB = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .initialPosition(0L, publicationA.initialTermId(), publicationA.termBufferLength())
            .sessionId(publicationA.sessionId())
            .endpoint(UNICAST_ENDPOINT_B)
            .build();

        publicationB = clientB.addExclusivePublication(publicationChannelB, STREAM_ID);
        final MutableLong position = new MutableLong(Long.MIN_VALUE);
        final Supplier<String> offerFailure = () -> "Failed to offer: " + position;

        for (int i = 0; i < numMessagesToSendForA; i++)
        {
            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);

            while ((position.value = publicationB.offer(buffer, 0, buffer.capacity())) < 0L)
            {
                Tests.yieldingWait(offerFailure);
            }

            assertEquals(0, subscription.poll(fragmentHandler, 10));
        }

        for (int i = 0; i < numMessagesToSendForB; i++)
        {
            while (publicationB.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscription, fragmentHandler);

            while (publicationA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            assertEquals(0, subscription.poll(fragmentHandler, 10));
        }

        assertEquals(1, subscription.imageCount());
        assertEquals(2, subscription.imageAtIndex(0).activeTransportCount());
        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    private void pollForFragment(final Subscription subscription, final FragmentHandler handler)
    {
        while (0 == subscription.poll(handler, 1))
        {
            Tests.yield();
        }
    }

    private void verifyFragments(final FragmentHandler fragmentHandler, final int numMessagesToSend)
    {
        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));
    }
}
