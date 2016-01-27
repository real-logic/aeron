/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.aeron.logbuffer.ControlledFragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Theories.class)
public class ControlledMessageTest
{
    public static final String CHANNEL = CommonContext.IPC_CHANNEL;
    public static final int STREAM_ID = 1;
    public static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int PAYLOAD_LENGTH = 10;

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage() throws Exception
    {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.threadingMode(ThreadingMode.SHARED);

        try (final MediaDriver ignore = MediaDriver.launch(ctx);
             final Aeron aeron = Aeron.connect();
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
             final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
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
                    Thread.yield();
                }
            }

            final FragmentCollector fragmentCollector = new FragmentCollector();
            int numFragments = 0;
            do
            {
                numFragments += subscription.controlledPoll(fragmentCollector, FRAGMENT_COUNT_LIMIT);
            }
            while (numFragments < 4);

            final UnsafeBuffer collectedBuffer = fragmentCollector.collectedBuffer();

            for (int i = 0; i < srcBuffer.capacity(); i++)
            {
                assertThat("same at i=" + i, collectedBuffer.getByte(i), is(srcBuffer.getByte(i)));
            }
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }

    class FragmentCollector implements ControlledFragmentHandler
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
