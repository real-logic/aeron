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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.*;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MultiSubscriberTest
{
    public static final String CHANNEL = "udp://localhost:54325";
    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;

    @Test(timeout = 10000)
    public void shouldReceiveMessageOnSeparateSubscriptions() throws Exception
    {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.dirsDeleteOnExit(true);

        final DataHandler mockDataHandlerOne = mock(DataHandler.class);
        final DataHandler mockDataHandlerTwo = mock(DataHandler.class);

        final FragmentAssemblyAdapter adapterOne = new FragmentAssemblyAdapter(mockDataHandlerOne);
        final FragmentAssemblyAdapter adapterTwo = new FragmentAssemblyAdapter(mockDataHandlerTwo);

        try (final MediaDriver ignore = MediaDriver.launch(ctx);
             final Aeron publisherClient = Aeron.connect(new Aeron.Context());
             final Aeron subscriberClient = Aeron.connect(new Aeron.Context());
             final Publication publication = publisherClient.addPublication(CHANNEL, STREAM_ID);
             final Subscription subscriptionOne = subscriberClient.addSubscription(CHANNEL, STREAM_ID, adapterOne);
             final Subscription subscriptionTwo = subscriberClient.addSubscription(CHANNEL, STREAM_ID, adapterTwo))
        {
            final byte[] expectedBytes = "Hello, World!".getBytes();
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(expectedBytes);

            while (publication.offer(srcBuffer) < 0L)
            {
                Thread.yield();
            }

            while (subscriptionOne.poll(FRAGMENT_COUNT_LIMIT) == 0)
            {
                Thread.yield();
            }

            while (subscriptionTwo.poll(FRAGMENT_COUNT_LIMIT) == 0)
            {
                Thread.yield();
            }

            verifyData(srcBuffer, mockDataHandlerOne);
            verifyData(srcBuffer, mockDataHandlerTwo);
        }
    }

    private void verifyData(final UnsafeBuffer srcBuffer, final DataHandler mockDataHandler)
    {
        final ArgumentCaptor<UnsafeBuffer> bufferArg = ArgumentCaptor.forClass(UnsafeBuffer.class);
        final ArgumentCaptor<Integer> offsetArg = ArgumentCaptor.forClass(Integer.class);

        verify(mockDataHandler, times(1)).onData(
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
