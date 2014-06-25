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
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
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
    private final RingBuffer conductorBuffer =
        new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(TRAILER_LENGTH + 1024)));
    private final ClientConductorProxy conductor = new ClientConductorProxy(conductorBuffer);

    @Test
    public void threadSendsAddChannelMessage()
    {
        threadSendsChannelMessage(() -> conductor.addPublication(DESTINATION, SESSION_ID, 2), ADD_PUBLICATION);
    }

    @Test
    public void threadSendsRemoveChannelMessage()
    {
        threadSendsChannelMessage(() -> conductor.removePublication(DESTINATION, SESSION_ID, 2), REMOVE_PUBLICATION);
    }

    private void threadSendsChannelMessage(final Runnable sendMessage, final int expectedMsgTypeId)
    {
        sendMessage.run();

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
                publicationMessage.wrap(buffer, index);

                assertThat(msgTypeId, is(expectedMsgTypeId));
                assertThat(publicationMessage.destination(), is(DESTINATION));
                assertThat(publicationMessage.sessionId(), is(1L));
                assertThat(publicationMessage.channelId(), is(2L));
            }
        );
    }

    @Test
    public void threadSendsRemoveSubscriberMessage()
    {
        conductor.removeSubscription(DESTINATION, CHANNEL_IDS);

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final SubscriptionMessageFlyweight subscriberMessage = new SubscriptionMessageFlyweight();
                subscriberMessage.wrap(buffer, index);

                assertThat(msgTypeId, is(REMOVE_SUBSCRIPTION));
                assertThat(subscriberMessage.destination(), is(DESTINATION));
                assertThat(subscriberMessage.channelIds(), is(CHANNEL_IDS));
            }
        );
    }

    @Test
    public void threadSendsRequestTermBufferMessage()
    {
        conductor.sendRequestTerm(DESTINATION, SESSION_ID, 2L, 3L);

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
                qualifiedMessage.wrap(buffer, index);

                assertThat(msgTypeId, is(CLEAN_TERM_BUFFER));
                assertThat(qualifiedMessage.sessionId(), is(1L));
                assertThat(qualifiedMessage.channelId(), is(2L));
                assertThat(qualifiedMessage.destination(), is(DESTINATION));
                assertThat(qualifiedMessage.termId(), is(3L));
            }
        );
    }

    private void assertReadsOneMessage(final MessageHandler handler)
    {
        final int messageCount = conductorBuffer.read(handler);
        assertThat(messageCount, is(1));
    }
}
