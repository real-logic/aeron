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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class ImageAvailabilityTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:ipc?term-length=64k",
            "aeron:udp?endpoint=localhost:24325|term-length=64k",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");
    }

    private static final int STREAM_ID = 1001;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(20))
            .threadingMode(ThreadingMode.SHARED), testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context()
            .useConductorAgentInvoker(true)
            .errorHandler(Tests::onError));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldCallImageHandlers(final String channel)
    {
        final AtomicInteger unavailableImageCount = new AtomicInteger();
        final AtomicInteger availableImageCount = new AtomicInteger();
        final UnavailableImageHandler unavailableHandler = (image) -> unavailableImageCount.incrementAndGet();
        final AvailableImageHandler availableHandler = (image) -> availableImageCount.incrementAndGet();

        final String spyChannel = channel.contains("ipc") ? channel : CommonContext.SPY_PREFIX + channel;

        try (Subscription subOne = aeron.addSubscription(channel, STREAM_ID, availableHandler, unavailableHandler);
            Subscription subTwo = aeron.addSubscription(
                spyChannel, STREAM_ID, availableHandler, unavailableHandler);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            while (!subOne.isConnected() || !subTwo.isConnected() || !publication.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            final Image image = subOne.imageAtIndex(0);
            final Image spyImage = subTwo.imageAtIndex(0);

            assertFalse(image.isClosed());
            assertFalse(image.isEndOfStream());
            assertFalse(spyImage.isClosed());
            assertFalse(spyImage.isEndOfStream());

            assertEquals(2, availableImageCount.get());
            assertEquals(0, unavailableImageCount.get());

            publication.close();

            while (subOne.isConnected() || subTwo.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            assertTrue(image.isClosed());
            assertTrue(image.isEndOfStream());
            assertTrue(spyImage.isClosed());
            assertTrue(spyImage.isEndOfStream());

            assertEquals(2, availableImageCount.get());
            assertEquals(2, unavailableImageCount.get());
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldCallImageHandlersWithPublisherOnDifferentClient(final String channel)
    {
        final AtomicInteger unavailableImageCount = new AtomicInteger();
        final AtomicInteger availableImageCount = new AtomicInteger();
        final UnavailableImageHandler unavailableHandler = (image) -> unavailableImageCount.incrementAndGet();
        final AvailableImageHandler availableHandler = (image) -> availableImageCount.incrementAndGet();

        final String spyChannel = channel.contains("ipc") ? channel : CommonContext.SPY_PREFIX + channel;
        final Aeron.Context ctx = new Aeron.Context()
            .useConductorAgentInvoker(true);

        try (Aeron aeronTwo = Aeron.connect(ctx);
            Subscription subOne = aeron.addSubscription(channel, STREAM_ID, availableHandler, unavailableHandler);
            Subscription subTwo = aeron.addSubscription(
                spyChannel, STREAM_ID, availableHandler, unavailableHandler);
            Publication publication = aeronTwo.addPublication(channel, STREAM_ID))
        {
            while (!subOne.isConnected() || !subTwo.isConnected() || !publication.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            final Image image = subOne.imageAtIndex(0);
            final Image spyImage = subTwo.imageAtIndex(0);

            assertFalse(image.isClosed());
            assertFalse(image.isEndOfStream());
            assertFalse(spyImage.isClosed());
            assertFalse(spyImage.isEndOfStream());

            assertEquals(2, availableImageCount.get());
            assertEquals(0, unavailableImageCount.get());

            aeronTwo.close();

            while (subOne.isConnected() || subTwo.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            assertTrue(image.isClosed());
            assertTrue(image.isEndOfStream());
            assertTrue(spyImage.isClosed());
            assertTrue(spyImage.isEndOfStream());

            assertEquals(2, availableImageCount.get());
            assertEquals(2, unavailableImageCount.get());
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldGetEndOfStreamPosition(final String channel)
    {
        final DirectBuffer message = new UnsafeBuffer("hello word".getBytes(US_ASCII));

        try (Subscription sub = aeron.addSubscription(channel, STREAM_ID);
            Publication pub = aeron.addPublication(channel, STREAM_ID))
        {
            while (!sub.isConnected() || !pub.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            final Image image = sub.imageAtIndex(0);
            assertEquals(Long.MAX_VALUE, image.endOfStreamPosition());

            final int numMessages = 10;
            for (int i = 0; i < numMessages; i++)
            {
                while (pub.offer(message) < 0)
                {
                    Tests.yield();
                }
            }

            assertEquals(Long.MAX_VALUE, image.endOfStreamPosition());

            int messagesRemaining = numMessages;
            final FragmentHandler noopFragmentHandler = (buffer, offset, length, header) -> {};
            while (0 < messagesRemaining)
            {
                messagesRemaining -= image.poll(noopFragmentHandler, 10);
            }

            assertEquals(Long.MAX_VALUE, image.endOfStreamPosition());

            for (int i = 0; i < numMessages; i++)
            {
                while (pub.offer(message) < 0)
                {
                    Tests.yield();
                }
            }

            messagesRemaining = numMessages;
            while (5 < messagesRemaining)
            {
                messagesRemaining -= image.poll(noopFragmentHandler, 1);
            }

            CloseHelper.quietClose(pub);
            long eosPosition;
            while (Long.MAX_VALUE == (eosPosition = image.endOfStreamPosition()))
            {
                Tests.yield();
            }

            assertFalse(image.isEndOfStream());

            while (0 < messagesRemaining)
            {
                messagesRemaining -= image.poll(noopFragmentHandler, 1);
            }

            while (!image.isEndOfStream())
            {
                Tests.yield();
            }

            assertEquals(eosPosition, image.position());
        }
    }
}
