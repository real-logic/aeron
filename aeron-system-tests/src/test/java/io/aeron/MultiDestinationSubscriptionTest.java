/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.function.Supplier;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class MultiDestinationSubscriptionTest
{
    private static final String UNICAST_ENDPOINT_A = "localhost:24325";
    private static final String UNICAST_ENDPOINT_B = "localhost:24326";

    private static final String PUB_UNICAST_URI = "aeron:udp?endpoint=localhost:24325";
    private static final String PUB_MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
    private static final String PUB_MDC_URI = "aeron:udp?control=localhost:24325|control-mode=dynamic";
    private static final String PUB_IPC_URI = "aeron:ipc";

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
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

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
    void closeEverything()
    {
        CloseHelper.closeAll(clientA, clientB, driverA, driverB);
        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test
    @InterruptAfter(10)
    void subscriptionCloseShouldAlsoCloseMediaDriverPorts()
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
    @InterruptAfter(10)
    @EnabledOnOs(OS.LINUX)
    void destinationShouldInheritSocketBufferLengthsFromSubscription()
    {
        launch(Tests::onError);

        final String publicationChannelA = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint("127.0.0.1:24325")
            .build();

        subscription = clientA.addSubscription(SUB_URI + "|so-sndbuf=32768|so-rcvbuf=32768|rcv-wnd=32768", STREAM_ID);
        subscription.addDestination(publicationChannelA);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithSpySubscriptionBeforeAddPublication()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(SPY_QUALIFIER + ":" + PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithSpySubscriptionAfterAddPublication()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        subscription.addDestination(SPY_QUALIFIER + ":" + PUB_UNICAST_URI);

        Tests.awaitConnected(subscription);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithSpySubscriptionThenDisconnectOnPublicationClose()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(SPY_QUALIFIER + ":" + PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
        CloseHelper.close(publicationA);
        Tests.awaitConnections(subscription, 0);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithSpySubscriptionThenRemoveDestination()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(SPY_QUALIFIER + ":" + PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
        subscription.removeDestination(SPY_QUALIFIER + ":" + PUB_UNICAST_URI);
        Tests.awaitConnections(subscription, 0);
    }

    @Test
    @InterruptAfter(10)
    void addAndRemoveNetworkDestination()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        subscription.removeDestination(PUB_UNICAST_URI);
        Tests.awaitConnections(subscription, 0);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithIpcSubscriptionBeforeAddPublication()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_IPC_URI);

        publicationA = clientA.addPublication(PUB_IPC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithIpcSubscriptionAfterAddPublication()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        publicationA = clientA.addPublication(PUB_IPC_URI, STREAM_ID);

        subscription.addDestination(PUB_IPC_URI);

        Tests.awaitConnected(subscription);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithIpcSubscriptionThenDisconnectOnPublicationClose()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_IPC_URI);

        publicationA = clientA.addPublication(PUB_IPC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
        CloseHelper.close(publicationA);
        Tests.awaitConnections(subscription, 0);
    }

    @Test
    @InterruptAfter(10)
    void addDestinationWithIpcSubscriptionThenRemoveDestination()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_IPC_URI);

        publicationA = clientA.addPublication(PUB_IPC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
        subscription.removeDestination((PUB_IPC_URI));
        Tests.awaitConnections(subscription, 0);
    }

    @Test
    @InterruptAfter(10)
    void shouldSpinUpAndShutdownWithUnicast()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        publicationA = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
    }

    @Test
    @InterruptAfter(10)
    void shouldSpinUpAndShutdownWithMulticast()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        final long correlationId = subscription.asyncAddDestination(PUB_MULTICAST_URI);

        publicationA = clientA.addPublication(PUB_MULTICAST_URI, STREAM_ID);

        Tests.awaitConnected(subscription);

        assertFalse(clientA.isCommandActive(correlationId));
    }

    @Test
    @InterruptAfter(20)
    void shouldSpinUpAndShutdownWithDynamicMdc()
    {
        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(SUB_MDC_DESTINATION_URI);

        publicationA = clientA.addPublication(PUB_MDC_URI, STREAM_ID);

        Tests.awaitConnected(subscription);
    }

    @Test
    @InterruptAfter(10)
    void shouldSendToSingleDestinationSubscriptionWithUnicast()
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
    @InterruptAfter(10)
    @SlowTest
    void shouldAllowMultipleMdsSubscriptions()
    {
        final String unicastUri2 = "aeron:udp?endpoint=localhost:24326";

        launch(Tests::onError);

        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        final Subscription subscriptionB = clientA.addSubscription(SUB_URI, STREAM_ID);
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

    @Test
    @InterruptAfter(10)
    void shouldFindMdsSubscriptionWithTags()
    {
        launch(Tests::onError);

        final long tagA = clientA.nextCorrelationId();
        final long tagIgnored = clientA.nextCorrelationId();

        final String taggedSubUri = new ChannelUriStringBuilder(SUB_URI).tags(tagA, null).build();
        final String taggedSubUriIgnored = new ChannelUriStringBuilder(SUB_URI).tags(tagIgnored, null).build();
        final String referringSubUri = new ChannelUriStringBuilder().media("udp").tags(tagA, null).build();

        subscription = clientA.addSubscription(taggedSubUri, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        clientA.addSubscription(taggedSubUriIgnored, STREAM_ID);
        final Subscription subscriptionA1 = clientA.addSubscription(referringSubUri, STREAM_ID);
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

    @Test
    @InterruptAfter(10)
    @SlowTest
    void shouldAllowMultipleMdsSubscriptionsWithTags()
    {
        final String unicastUri2 = "aeron:udp?endpoint=localhost:24326";

        launch(Tests::onError);

        final long tagA = clientA.nextCorrelationId();
        final long tagB = clientA.nextCorrelationId();

        final String uriA = new ChannelUriStringBuilder(SUB_URI).tags(tagA, null).build();
        final String referringUriA = new ChannelUriStringBuilder().media("udp").tags(tagA, null).build();
        final String uriB = new ChannelUriStringBuilder(SUB_URI).tags(tagB, null).build();
        final String referringUriB = new ChannelUriStringBuilder().media("udp").tags(tagB, null).build();

        subscription = clientA.addSubscription(uriA, STREAM_ID);
        subscription.addDestination(PUB_UNICAST_URI);

        try (Subscription subscriptionB = clientA.addSubscription(uriB, STREAM_ID);
            Subscription subscriptionA1 = clientA.addSubscription(referringUriA, STREAM_ID);
            Subscription subscriptionB1 = clientA.addSubscription(referringUriB, STREAM_ID))
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
    @InterruptAfter(10)
    void shouldSendToSingleDestinationMultipleSubscriptionsWithUnicast()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        final long channelTag = clientA.nextCorrelationId();
        final long subTag = clientA.nextCorrelationId();

        final String subscriptionChannel = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .tags(channelTag, subTag)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .build();

        final String copyChannel = new ChannelUriStringBuilder().media("udp").tags(channelTag, subTag).build();

        subscription = clientA.addSubscription(subscriptionChannel, STREAM_ID);
        final Subscription copySubscription = clientA.addSubscription(copyChannel, STREAM_ID);
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
    @InterruptAfter(10)
    void shouldSendToSingleDestinationSubscriptionWithMulticast()
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
    @InterruptAfter(20)
    void shouldSendToSingleDestinationSubscriptionWithDynamicMdc()
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
    @InterruptAfter(10)
    void shouldSendToMultipleDestinationSubscriptionWithSameStream()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        final int numMessagesToSendForA = numMessagesToSend / 2;
        final int numMessagesToSendForB = numMessagesToSend / 2;

        launch(Tests::onError);

        final long channelTag = clientA.nextCorrelationId();
        final long pubTag = clientA.nextCorrelationId();

        final String publicationChannelA = new ChannelUriStringBuilder()
            .tags(channelTag, pubTag)
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
            .taggedSessionId(pubTag)
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
    @InterruptAfter(10)
    void shouldMergeStreamsFromMultiplePublicationsWithSameParams()
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
                Tests.yieldingIdle(offerFailure);
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
