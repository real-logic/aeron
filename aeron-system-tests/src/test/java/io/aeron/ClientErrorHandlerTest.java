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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.Tests.awaitConnected;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class ClientErrorHandlerTest
{
    private static final int STREAM_ID = 1001;
    private static final String CHANNEL = "aeron:ipc";

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldHaveCorrectTermBufferLength()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true);

        final ErrorHandler mockErrorHandlerOne = mock(ErrorHandler.class);
        final Aeron.Context clientCtxOne = new Aeron.Context().errorHandler(mockErrorHandlerOne);

        final ErrorHandler mockErrorHandlerTwo = mock(ErrorHandler.class);
        final Aeron.Context clientCtxTwo = new Aeron.Context()
            .errorHandler(mockErrorHandlerTwo)
            .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);

        try (TestMediaDriver ignore = TestMediaDriver.launch(ctx, testWatcher))
        {
            testWatcher.dataCollector().add(ctx.aeronDirectory());

            try (
                Aeron aeronOne = Aeron.connect(clientCtxOne);
                Aeron aeronTwo = Aeron.connect(clientCtxTwo);
                Publication publication = aeronOne.addPublication(CHANNEL, STREAM_ID);
                Subscription subscriptionOne = aeronOne.addSubscription(CHANNEL, STREAM_ID);
                Subscription subscriptionTwo = aeronTwo.addSubscription(CHANNEL, STREAM_ID))
            {
                testWatcher.dataCollector().add(ctx.aeronDirectory());

                awaitConnected(subscriptionOne);
                awaitConnected(subscriptionTwo);

                assertEquals(clientCtxOne.errorHandler(), clientCtxOne.subscriberErrorHandler());
                assertNotEquals(clientCtxTwo.errorHandler(), clientCtxTwo.subscriberErrorHandler());

                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[100]);
                while (publication.offer(buffer) < 0)
                {
                    Tests.yield();
                }

                final RuntimeException expectedException = new RuntimeException("Expected");
                final FragmentHandler handler =
                    (buffer1, offset, length, header) ->
                    {
                        throw expectedException;
                    };

                while (0 == subscriptionOne.poll(handler, 1))
                {
                    Tests.yield();
                }

                verify(mockErrorHandlerOne).onError(expectedException);

                try
                {
                    while (0 == subscriptionTwo.poll(handler, 1))
                    {
                        Tests.yield();
                    }

                    fail("Expected exception");
                }
                catch (final Exception ex)
                {
                    assertEquals(expectedException, ex);
                }

                verify(mockErrorHandlerTwo, never()).onError(any());
            }
        }
    }
}
