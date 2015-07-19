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
import uk.co.real_logic.aeron.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.command.RemoveMessageFlyweight;
import uk.co.real_logic.agrona.concurrent.MessageHandler;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;
import static uk.co.real_logic.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class DriverProxyTest
{
    public static final String CHANNEL = "udp://localhost:40123@localhost:40124";

    private static final int STREAM_ID = 1;
    private static final long CORRELATION_ID = 3;
    private final RingBuffer conductorBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(ByteBuffer.allocateDirect(TRAILER_LENGTH + 1024)));
    private final DriverProxy conductor = new DriverProxy(conductorBuffer);

    @Test
    public void threadSendsAddChannelMessage()
    {
        threadSendsChannelMessage(() -> conductor.addPublication(CHANNEL, STREAM_ID), ADD_PUBLICATION);
    }

    @Test
    public void threadSendsRemoveChannelMessage()
    {
        conductor.removePublication(CORRELATION_ID);
        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final RemoveMessageFlyweight message = new RemoveMessageFlyweight();
                message.wrap(buffer, index);

                assertThat(msgTypeId, is(REMOVE_PUBLICATION));
                assertThat(message.registrationId(), is(CORRELATION_ID));
            }
        );
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
                assertThat(publicationMessage.channel(), is(CHANNEL));
                assertThat(publicationMessage.streamId(), is(STREAM_ID));
            }
        );
    }

    @Test
    public void threadSendsRemoveSubscriberMessage()
    {
        conductor.removeSubscription(CORRELATION_ID);

        assertReadsOneMessage(
            (msgTypeId, buffer, index, length) ->
            {
                final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
                removeMessage.wrap(buffer, index);

                assertThat(msgTypeId, is(REMOVE_SUBSCRIPTION));
                assertThat(removeMessage.registrationId(), is(CORRELATION_ID));
            }
        );
    }

    private void assertReadsOneMessage(final MessageHandler handler)
    {
        final int messageCount = conductorBuffer.read(handler);
        assertThat(messageCount, is(1));
    }
}
