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
import org.agrona.DirectBuffer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class MultiSubscriberTest
{
    public static final String CHANNEL_1 = "aeron:udp?endpoint=localhost:54325|fruit=banana";
    public static final String CHANNEL_2 = "aeron:udp?endpoint=localhost:54325|fruit=apple";
    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;

    @Test(timeout = 10000)
    public void shouldReceiveMessageOnSeparateSubscriptions()
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace);

        final FragmentHandler mockFragmentHandlerOne = mock(FragmentHandler.class);
        final FragmentHandler mockFragmentHandlerTwo = mock(FragmentHandler.class);

        final FragmentAssembler adapterOne = new FragmentAssembler(mockFragmentHandlerOne);
        final FragmentAssembler adapterTwo = new FragmentAssembler(mockFragmentHandlerTwo);

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron client = Aeron.connect(new Aeron.Context());
            Publication publication = client.addPublication(CHANNEL_1, STREAM_ID);
            Subscription subscriptionOne = client.addSubscription(CHANNEL_1, STREAM_ID);
            Subscription subscriptionTwo = client.addSubscription(CHANNEL_2, STREAM_ID))
        {
            final byte[] expectedBytes = "Hello, World! here is a small message".getBytes();
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(expectedBytes);

            assertThat(subscriptionOne.poll(adapterOne, FRAGMENT_COUNT_LIMIT), is(0));
            assertThat(subscriptionTwo.poll(adapterTwo, FRAGMENT_COUNT_LIMIT), is(0));

            while (publication.offer(srcBuffer) < 0L)
            {
                Thread.yield();
            }

            while (subscriptionOne.poll(adapterOne, FRAGMENT_COUNT_LIMIT) == 0)
            {
                Thread.yield();
            }

            while (subscriptionTwo.poll(adapterTwo, FRAGMENT_COUNT_LIMIT) == 0)
            {
                Thread.yield();
            }

            verifyData(srcBuffer, mockFragmentHandlerOne);
            verifyData(srcBuffer, mockFragmentHandlerTwo);
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }

    private void verifyData(final UnsafeBuffer srcBuffer, final FragmentHandler mockFragmentHandler)
    {
        final ArgumentCaptor<DirectBuffer> bufferArg = ArgumentCaptor.forClass(DirectBuffer.class);
        final ArgumentCaptor<Integer> offsetArg = ArgumentCaptor.forClass(Integer.class);

        verify(mockFragmentHandler, times(1)).onFragment(
            bufferArg.capture(), offsetArg.capture(), eq(srcBuffer.capacity()), any(Header.class));

        final DirectBuffer capturedBuffer = bufferArg.getValue();
        final int offset = offsetArg.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            final int index = offset + i;
            assertThat("same at " + index, capturedBuffer.getByte(index), is(srcBuffer.getByte(i)));
        }
    }
}
