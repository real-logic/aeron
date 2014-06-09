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
import uk.co.real_logic.aeron.util.command.*;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.MessageHandler;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class ClientConductorProxyTest
{
    public static final String DESTINATION = "udp://localhost:40123@localhost:40124";

    private static final long[] CHANNEL_IDS = {1L, 3L, 4L};
    private static final long SESSION_ID = 1L;
    private final RingBuffer mediaConductorBuffer =
        new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(TRAILER_LENGTH + 1024)));
    private final ClientConductorProxy mediaConductor = new ClientConductorProxy(mediaConductorBuffer);

    @Test
    public void threadSendsAddChannelMessage()
    {
        threadSendsChannelMessage(() -> mediaConductor.sendAddChannel(DESTINATION, SESSION_ID, 2), ADD_CHANNEL);
    }

    @Test
    public void threadSendsRemoveChannelMessage()
    {
        threadSendsChannelMessage(() -> mediaConductor.sendRemoveChannel(DESTINATION, SESSION_ID, 2), REMOVE_CHANNEL);
    }

    private void threadSendsChannelMessage(final Runnable sendMessage, final int expectedMsgTypeId)
    {
        sendMessage.run();

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
                channelMessage.wrap(buffer, index);

                assertThat(msgTypeId, is(expectedMsgTypeId));
                assertThat(channelMessage.destination(), is(DESTINATION));
                assertThat(channelMessage.sessionId(), is(1L));
                assertThat(channelMessage.channelId(), is(2L));
            }
        );
    }

    @Test
    public void threadSendsRemoveSubscriberMessage()
    {
        mediaConductor.sendRemoveSubscriber(DESTINATION, CHANNEL_IDS);

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                SubscriberMessageFlyweight removeSubscriberMessage = new SubscriberMessageFlyweight();
                removeSubscriberMessage.wrap(buffer, index);

                assertThat(msgTypeId, is(REMOVE_SUBSCRIBER));
                assertThat(removeSubscriberMessage.destination(), is(DESTINATION));
                assertThat(removeSubscriberMessage.channelIds(), is(CHANNEL_IDS));
            }
        );
    }

    @Test
    public void threadSendsRequestTermBufferMessage()
    {
        mediaConductor.sendRequestTerm(DESTINATION, SESSION_ID, 2L, 3L);

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                QualifiedMessageFlyweight requestTermBuffer = new QualifiedMessageFlyweight();
                requestTermBuffer.wrap(buffer, index);

                assertThat(msgTypeId, is(REQUEST_CLEANED_TERM));
                assertThat(requestTermBuffer.sessionId(), is(1L));
                assertThat(requestTermBuffer.channelId(), is(2L));
                assertThat(requestTermBuffer.destination(), is(DESTINATION));
                assertThat(requestTermBuffer.termId(), is(3L));
            }
        );
    }

    private void assertReadsOneMessage(final MessageHandler handler)
    {
        final int messageCount = mediaConductorBuffer.read(handler);
        assertThat(messageCount, is(1));
    }
}
