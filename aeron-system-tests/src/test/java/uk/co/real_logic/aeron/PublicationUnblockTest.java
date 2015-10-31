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
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@RunWith(Theories.class)
public class PublicationUnblockTest
{
    @DataPoint
    public static final String NETWORK_CHANNEL = "aeron:udp?remote=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = "aeron:ipc";

    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;

    @Theory
    @Test(timeout = 10000)
    public void shouldUnblockNonCommittedMessage(final String channel) throws Exception
    {
        final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
        final MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.publicationUnblockTimeoutNs(TimeUnit.MILLISECONDS.toNanos(10));

        try (final MediaDriver ignore = MediaDriver.launch(ctx);
             final Aeron client = Aeron.connect(new Aeron.Context());
             final Publication publicationA = client.addPublication(channel, STREAM_ID);
             final Publication publicationB = client.addPublication(channel, STREAM_ID);
             final Subscription subscription = client.addSubscription(channel, STREAM_ID))
        {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[ctx.mtuLength()]);
            final int length = 128;
            final BufferClaim bufferClaim = new BufferClaim();

            srcBuffer.setMemory(0, length, (byte)66);

            while (publicationA.tryClaim(length, bufferClaim) < 0L)
            {
                Thread.yield();
            }

            bufferClaim.buffer().setMemory(bufferClaim.offset(), length, (byte)65);
            bufferClaim.commit();

            while (publicationB.offer(srcBuffer, 0, length) < 0L)
            {
                Thread.yield();
            }

            while (publicationA.tryClaim(length, bufferClaim) < 0L)
            {
                Thread.yield();
            }

            // no commit of publicationA

            while (publicationB.offer(srcBuffer, 0, length) < 0L)
            {
                Thread.yield();
            }

            final int expectedFragments = 3;
            int numFragments = 0;
            do
            {
                numFragments += subscription.poll(mockFragmentHandler, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < expectedFragments);

            assertThat(numFragments, is(3));
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }
}
