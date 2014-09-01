/*
 * Copyright 2014 Real Logic Ltd.
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
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.driver.MediaDriver;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Theories.class)
public class FragmentedMessageTest
{
    @DataPoint
    public static final String UNICAST_URI = "udp://localhost:54325";

    @DataPoint
    public static final String MULTICAST_URI = "udp://localhost@224.20.30.39:54326";

    private static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;

    private final DataHandler mockDataHandler = mock(DataHandler.class);

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage(final String channel) throws Exception
    {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.dirsDeleteOnExit(true);

        final FragmentAssemblyAdapter adapter = new FragmentAssemblyAdapter(mockDataHandler);

        try (final MediaDriver driver = MediaDriver.launch(ctx);
             final Aeron publisherClient = Aeron.connect(new Aeron.Context());
             final Aeron subscriberClient = Aeron.connect(new Aeron.Context());
             final Publication publication = publisherClient.addPublication(channel, STREAM_ID);
             final Subscription subscription = subscriberClient.addSubscription(channel, STREAM_ID, adapter))
        {
            final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[ctx.mtuLength() * 4]);
            final int offset = 0;
            final int length = srcBuffer.capacity() / 4;

            for (int i = 0; i < 4; i++)
            {
                srcBuffer.setMemory(i * length, length, (byte)(65 + i));
            }

            while (!publication.offer(srcBuffer, offset, srcBuffer.capacity()))
            {
                Thread.yield();
            }

            int numFragments = 0;
            do
            {
                numFragments += subscription.poll(FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < 4);

            final ArgumentCaptor<AtomicBuffer> argument = ArgumentCaptor.forClass(AtomicBuffer.class);

            verify(mockDataHandler, times(1)).onData(
                argument.capture(), eq(offset), eq(srcBuffer.capacity()), anyInt(), eq(FrameDescriptor.UNFRAGMENTED));

            final AtomicBuffer capturedBuffer = argument.getValue();
            for (int i = 0; i < srcBuffer.capacity(); i++)
            {
                assertThat("same at i=" + i, capturedBuffer.getByte(i), is(srcBuffer.getByte(i)));
            }
        }
    }
}
