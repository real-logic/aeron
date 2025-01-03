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
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
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
import org.mockito.ArgumentCaptor;

import java.util.List;

import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class FragmentedMessageTest
{
    private static List<String> channels()
    {
        return asList(
            CommonContext.IPC_CHANNEL,
            "aeron:udp?endpoint=localhost:24325",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");
    }

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .errorHandler(Tests::onError)
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

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldReceivePublishedMessage(final String channel)
    {
        final FragmentAssembler assembler = new FragmentAssembler(mockFragmentHandler);

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[driver.context().mtuLength() * 4]);
            final int offset = 0;
            final int length = srcBuffer.capacity() / 4;

            for (int i = 0; i < 4; i++)
            {
                srcBuffer.setMemory(i * length, length, (byte)(65 + i));
            }

            while (publication.offer(srcBuffer, offset, srcBuffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            final int expectedFragmentsBecauseOfHeader = 5;
            int fragmentCount = 0;
            do
            {
                final int fragments = subscription.poll(assembler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    Tests.yield();
                }
                fragmentCount += fragments;
            }
            while (fragmentCount < expectedFragmentsBecauseOfHeader);

            final ArgumentCaptor<DirectBuffer> bufferArg = ArgumentCaptor.forClass(DirectBuffer.class);
            final ArgumentCaptor<Header> headerArg = ArgumentCaptor.forClass(Header.class);

            verify(mockFragmentHandler, times(1)).onFragment(
                bufferArg.capture(), eq(offset), eq(srcBuffer.capacity()), headerArg.capture());

            final DirectBuffer capturedBuffer = bufferArg.getValue();
            for (int i = 0; i < srcBuffer.capacity(); i++)
            {
                assertEquals(srcBuffer.getByte(i), capturedBuffer.getByte(i), "same at i=" + i);
            }

            assertEquals(UNFRAGMENTED, headerArg.getValue().flags());
        }
    }
}
