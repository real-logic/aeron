/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class MultiSubscriberTest
{
    public static final String CHANNEL_1 = "aeron:udp?remote=localhost:54325|fruit=banana";
    public static final String CHANNEL_2 = "aeron:udp?remote=localhost:54325|fruit=apple";
    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;

    @Test(timeout = 10000)
    public void shouldReceiveMessageOnSeparateSubscriptions() throws Exception
    {
        final MediaDriver.Context ctx = new MediaDriver.Context();

        final FragmentHandler mockFragmentHandlerOne = mock(FragmentHandler.class);
        final FragmentHandler mockFragmentHandlerTwo = mock(FragmentHandler.class);

        final FragmentAssembler adapterOne = new FragmentAssembler(mockFragmentHandlerOne);
        final FragmentAssembler adapterTwo = new FragmentAssembler(mockFragmentHandlerTwo);

        try (final MediaDriver ignore = MediaDriver.launch(ctx);
             final Aeron client = Aeron.connect(new Aeron.Context());
             final Publication publication = client.addPublication(CHANNEL_1, STREAM_ID);
             final Subscription subscriptionOne = client.addSubscription(CHANNEL_1, STREAM_ID);
             final Subscription subscriptionTwo = client.addSubscription(CHANNEL_2, STREAM_ID))
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
        final ArgumentCaptor<UnsafeBuffer> bufferArg = ArgumentCaptor.forClass(UnsafeBuffer.class);
        final ArgumentCaptor<Integer> offsetArg = ArgumentCaptor.forClass(Integer.class);

        verify(mockFragmentHandler, times(1)).onFragment(
            bufferArg.capture(), offsetArg.capture(), eq(srcBuffer.capacity()), any(Header.class));

        final UnsafeBuffer capturedBuffer = bufferArg.getValue();
        final int offset = offsetArg.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            final int index = offset + i;
            assertThat("same at " + index, capturedBuffer.getByte(index), is(srcBuffer.getByte(i)));
        }
    }
}
