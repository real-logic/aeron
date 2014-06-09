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
package uk.co.real_logic.aeron.util.concurrent.ringbuffer;

import uk.co.real_logic.aeron.util.concurrent.MessageHandler;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RingBufferTestUtil
{
    public static void assertMsgRead(final RingBuffer ringBuffer, final MessageHandler handler)
    {
        final int messages = ringBuffer.read(handler);
        assertThat(messages, is(greaterThanOrEqualTo(1)));
    }

    public static void skip(final RingBuffer ringBuffer, int count)
    {
        ringBuffer.read((msgTypeId, buffer, index, length) -> {}, count);
    }

    public static void assertNoMessages(final RingBuffer ringBuffer)
    {
        final int messages = ringBuffer.read((msgTypeId, buffer, index, length) -> {});
        assertThat(messages, is(0));
    }
}
