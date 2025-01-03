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
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
class TwoBufferOfferMessageTest
{
    private static final String CHANNEL = "aeron:ipc?term-length=64k";
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED);

    private TestMediaDriver driver;

    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(driverContext, testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldTransferUnfragmentedTwoPartMessage()
    {
        final UnsafeBuffer expectedBuffer = new UnsafeBuffer(new byte[256]);
        final UnsafeBuffer bufferOne = new UnsafeBuffer(expectedBuffer, 0, 32);
        final UnsafeBuffer bufferTwo = new UnsafeBuffer(expectedBuffer, 32, expectedBuffer.capacity() - 32);

        bufferOne.setMemory(0, bufferOne.capacity(), (byte)'a');
        bufferTwo.setMemory(0, bufferTwo.capacity(), (byte)'b');
        final String expectedMessage = expectedBuffer.getStringWithoutLengthAscii(0, expectedBuffer.capacity());

        final MutableReference<String> receivedMessage = new MutableReference<>();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            receivedMessage.set(buffer.getStringWithoutLengthAscii(offset, length));

        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
            {
                publishMessage(bufferOne, bufferTwo, publication);
                pollForMessage(subscription, receivedMessage, fragmentHandler);

                assertEquals(expectedMessage, receivedMessage.get());
            }

            try (Publication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
            {
                publishMessage(bufferOne, bufferTwo, publication);
                pollForMessage(subscription, receivedMessage, fragmentHandler);

                assertEquals(expectedMessage, receivedMessage.get());
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldTransferFragmentedTwoPartMessage()
    {
        final UnsafeBuffer expectedBuffer = new UnsafeBuffer(new byte[32 + driver.context().mtuLength()]);
        final UnsafeBuffer bufferOne = new UnsafeBuffer(expectedBuffer, 0, 32);
        final UnsafeBuffer bufferTwo = new UnsafeBuffer(expectedBuffer, 32, expectedBuffer.capacity() - 32);

        bufferOne.setMemory(0, bufferOne.capacity(), (byte)'a');
        bufferTwo.setMemory(0, bufferTwo.capacity(), (byte)'b');
        final String expectedMessage = expectedBuffer.getStringWithoutLengthAscii(0, expectedBuffer.capacity());

        final MutableReference<String> receivedMessage = new MutableReference<>();
        final FragmentHandler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) ->
            receivedMessage.set(buffer.getStringWithoutLengthAscii(offset, length)));

        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
            {
                publishMessage(bufferOne, bufferTwo, publication);
                pollForMessage(subscription, receivedMessage, fragmentHandler);

                assertEquals(expectedMessage, receivedMessage.get());
            }

            try (Publication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
            {
                publishMessage(bufferOne, bufferTwo, publication);
                pollForMessage(subscription, receivedMessage, fragmentHandler);

                assertEquals(expectedMessage, receivedMessage.get());
            }
        }
    }

    private static void publishMessage(
        final UnsafeBuffer bufferOne, final UnsafeBuffer bufferTwo, final Publication publication)
    {
        while (publication.offer(bufferOne, 0, bufferOne.capacity(), bufferTwo, 0, bufferTwo.capacity()) < 0L)
        {
            Tests.yield();
        }
    }

    private void pollForMessage(
        final Subscription subscription, final MutableReference<String> receivedMessage, final FragmentHandler handler)
    {
        receivedMessage.set(null);

        while (receivedMessage.get() == null)
        {
            final int fragments = subscription.poll(handler, FRAGMENT_COUNT_LIMIT);
            if (fragments == 0)
            {
                Tests.yield();
            }
        }
    }
}
