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

import uk.co.real_logic.aeron.util.concurrent.EventHandler;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RingBufferTestUtil
{

    public static void assertEventRead(final RingBuffer ringBuffer, final EventHandler handler)
    {
        int eventsRead = ringBuffer.read(handler);
        assertThat(eventsRead, is(greaterThanOrEqualTo(1)));
    }

    public static void skip(final RingBuffer ringBuffer, int count)
    {
        ringBuffer.read((eventTypeId, buffer, index, length) ->
        {
        }, count);
    }

    public static void assertNoMessages(final RingBuffer ringBuffer)
    {
        int events = ringBuffer.read((eventTypeId, buffer, index, length) ->
        {
        });
        assertThat(events, is(0));
    }
}
