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
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControlledMessageTest
{
    private static final String CHANNEL = CommonContext.IPC_CHANNEL;
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int PAYLOAD_LENGTH = 10;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED),
        testWatcher);

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldReceivePublishedMessage()
    {
        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[PAYLOAD_LENGTH * 4]);

            for (int i = 0; i < 4; i++)
            {
                srcBuffer.setMemory(i * PAYLOAD_LENGTH, PAYLOAD_LENGTH, (byte)(65 + i));
            }

            for (int i = 0; i < 4; i++)
            {
                while (publication.offer(srcBuffer, i * PAYLOAD_LENGTH, PAYLOAD_LENGTH) < 0L)
                {
                    Tests.yield();
                }
            }

            final FragmentCollector fragmentCollector = new FragmentCollector();
            int numFragments = 0;
            do
            {
                final int fragments = subscription.controlledPoll(fragmentCollector, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    Tests.yield();
                }
                numFragments += fragments;
            }
            while (numFragments < 4);

            final UnsafeBuffer collectedBuffer = fragmentCollector.collectedBuffer();

            for (int i = 0; i < srcBuffer.capacity(); i++)
            {
                assertEquals(srcBuffer.getByte(i), collectedBuffer.getByte(i), "same at i=" + i);
            }
        }
    }

    static class FragmentCollector implements ControlledFragmentHandler
    {
        private final UnsafeBuffer collectedBuffer = new UnsafeBuffer(new byte[PAYLOAD_LENGTH * 4]);
        private int limit = 0;
        private int fragmentCount = 0;

        public UnsafeBuffer collectedBuffer()
        {
            return collectedBuffer;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            ++fragmentCount;

            Action action = Action.CONTINUE;

            if (fragmentCount == 3)
            {
                action = Action.ABORT;
            }
            else if (fragmentCount == 5)
            {
                action = Action.BREAK;
            }

            if (Action.ABORT != action)
            {
                collectedBuffer.putBytes(limit, buffer, offset, length);
                limit += length;
            }

            return action;
        }
    }
}
