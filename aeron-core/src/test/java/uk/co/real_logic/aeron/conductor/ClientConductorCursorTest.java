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
package uk.co.real_logic.aeron.conductor;

import org.junit.Test;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

public class ClientConductorCursorTest
{
    private static final long[] CHANNEL_IDS = { 1L, 3L, 4L };
    private static final long SESSION_ID = 1L;
    public static final String DESTINATION = "udp://localhost:40123@localhost:40124";
    private final RingBuffer sendBuffer =
        new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(TRAILER_LENGTH + 1024)));
    private final ClientConductorCursor thread = new ClientConductorCursor(sendBuffer);

    @Test
    public void threadSendsAddChannelMessage()
    {
        threadSendsChannelMessage(() -> thread.sendAddChannel(DESTINATION, SESSION_ID, 2), ADD_CHANNEL);
    }

    @Test
    public void threadSendsRemoveChannelMessage()
    {
        threadSendsChannelMessage(() -> thread.sendRemoveChannel(DESTINATION, SESSION_ID, 2), REMOVE_CHANNEL);
    }

    private void threadSendsChannelMessage(final Runnable sendMessage, final int expectedEventTypeId)
    {
        sendMessage.run();

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
            channelMessage.wrap(buffer, index);

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
            ConsumerMessageFlyweight removeReceiverMessage = new ConsumerMessageFlyweight();
            removeReceiverMessage.wrap(buffer, index);

            assertThat(eventTypeId, is(REMOVE_CONSUMER));
            assertThat(removeReceiverMessage.destination(), is(DESTINATION));
            assertThat(removeReceiverMessage.channelIds(), is(CHANNEL_IDS));
        });
    }

    @Test
    public void threadSendsRequestTermBufferMessage()
    {
        thread.sendRequestTerm(DESTINATION, SESSION_ID, 2L, 3L);

        assertReadsOneMessage((eventTypeId, buffer, index, length) ->
        {
            CompletelyIdentifiedMessageFlyweight requestTermBuffer = new CompletelyIdentifiedMessageFlyweight();
            requestTermBuffer.wrap(buffer, index);

            assertThat(eventTypeId, is(REQUEST_CLEANED_TERM));
            assertThat(requestTermBuffer.sessionId(), is(1L));
            assertThat(requestTermBuffer.channelId(), is(2L));
            assertThat(requestTermBuffer.destination(), is(DESTINATION));
            assertThat(requestTermBuffer.termId(), is(3L));
        });
    }

    private void assertReadsOneMessage(final EventHandler handler)
    {
        final int messageCount = sendBuffer.read(handler);
        assertThat(messageCount, is(1));
    }

}
