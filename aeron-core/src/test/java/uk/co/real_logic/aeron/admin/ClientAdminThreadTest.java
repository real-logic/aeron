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
package uk.co.real_logic.aeron.admin;

import org.junit.Test;
import uk.co.real_logic.aeron.ClientAdminThread;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.command.TripletMessageFlyweight;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

public class ClientAdminThreadTest
{

    private static final long[] CHANNEL_IDS = { 1L, 3L, 4L };
    public static final String DESTINATION = "udp://localhost:40123@localhost:40124";
    private final RingBuffer sendBuffer = new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(TRAILER_SIZE + 1024)));
    private final ClientAdminThread thread = new ClientAdminThread(null, null, sendBuffer, null);

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
        thread.sendRemoveReceiver(DESTINATION, CHANNEL_IDS);

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            ReceiverMessageFlyweight removeReceiverMessage = new ReceiverMessageFlyweight();
            removeReceiverMessage.reset(buffer, index);

            assertThat(eventTypeId, is(REMOVE_RECEIVER));
            assertThat(removeReceiverMessage.destination(), is(DESTINATION));
            assertThat(removeReceiverMessage.channelIds(), is(CHANNEL_IDS));
        });
    }

    @Test
    public void threadSendsRequestTermBufferMessage()
    {
        thread.sendRequestTerm(1L, 2L, 3L);

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            TripletMessageFlyweight requestTermBuffer = new TripletMessageFlyweight();
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
