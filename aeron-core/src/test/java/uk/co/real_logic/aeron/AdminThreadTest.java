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
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.RemoveReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.command.TripleMessageFlyweight;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

public class AdminThreadTest
{

    public static final String DESTINATION = "udp://localhost:40123@localhost:40124";
    private final RingBuffer sendBuffer = new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(TRAILER_SIZE + 1024)));
    private final AdminThread thread = new AdminThread(null, null, sendBuffer);

    @Test
    public void threadSendsAddChannelMessage()
    {
        threadSendsChannelMessage(() -> thread.sendAddChannel(DESTINATION, 1, 2), ADD_CHANNEL);
    }

    @Test
    public void threadSendsRemoveChannelMessage()
    {
        threadSendsChannelMessage(() -> thread.sendRemoveChannel(DESTINATION, 1, 2), REMOVE_CHANNEL);
    }

    private void threadSendsChannelMessage(final Runnable sendMessage, final int expectedEventTypeId)
    {
        sendMessage.run();

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
            channelMessage.reset(buffer, index);

            assertThat(eventTypeId, is(expectedEventTypeId));
            assertThat(channelMessage.destination(), is(DESTINATION));
            assertThat(channelMessage.sessionId(), is(1L));
            assertThat(channelMessage.channelId(), is(2L));
        });
    }

    @Test
    public void threadSendsRemoveReceiverMessage()
    {
        thread.sendRemoveReceiver(DESTINATION);

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            RemoveReceiverMessageFlyweight removeReceiverMessage = new RemoveReceiverMessageFlyweight();
            removeReceiverMessage.reset(buffer, index);

            assertThat(eventTypeId, is(REMOVE_RECEIVER));
            assertThat(removeReceiverMessage.destination(), is(DESTINATION));
        });
    }

    @Test
    public void threadSendsRequestTermBufferMessage()
    {
        thread.sendRequestTerm(1L, 2L, 3L);

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            TripleMessageFlyweight requestTermBuffer = new TripleMessageFlyweight();
            requestTermBuffer.reset(buffer, index);

            assertThat(eventTypeId, is(REQUEST_TERM));
            assertThat(requestTermBuffer.sessionId(), is(1L));
            assertThat(requestTermBuffer.channelId(), is(2L));
            assertThat(requestTermBuffer.termId(), is(3L));
        });
    }

    private void assertReadsOneMessage(final EventHandler handler)
    {
        final int messageCount = sendBuffer.read(handler);
        assertThat(messageCount, is(1));
    }

}
