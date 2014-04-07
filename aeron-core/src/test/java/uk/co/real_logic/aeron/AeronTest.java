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

import org.junit.Rule;
import org.junit.Test;
import uk.co.real_logic.aeron.admin.ClientAdminThread;
import uk.co.real_logic.aeron.util.MappingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

public class AeronTest
{

    private static final String DESTINATION = "udp://localhost:40124";
    private static final long CHANNEL_ID = 2L;
    private static final long SESSION_ID = 3L;

    @Rule
    public SharedDirectory directory = new SharedDirectory();

    @Rule
    public AdminBuffers adminBuffers = new AdminBuffers();

    private final ChannelMessageFlyweight message = new ChannelMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight identifiedMessage = new CompletelyIdentifiedMessageFlyweight();
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(256);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    public AeronTest()
    {
        identifiedMessage.wrap(atomicSendBuffer, 0);
    }

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final Aeron aeron = newAeron();
        newChannel(aeron);
        aeron.adminThread().process();

        assertChannelMessage(adminBuffers.toMediaDriver(), ADD_CHANNEL);
    }

    @Test
    public void cannotOfferOnChannelUntilBuffersMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        assertFalse(channel.offer(sendBuffer));
        assertFalse(channel.offer(sendBuffer, 0, 1));
    }

    @Test(expected=BufferExhaustedException.class)
    public void cannotSendOnChannelUntilBuffersMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        channel.send(sendBuffer);
    }

    @Test
    public void canOfferAMessageOnceBuffersHaveBeenMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        aeron.adminThread().process();
        createTermBuffer(0L);
        aeron.adminThread().process();
        assertTrue(channel.offer(sendBuffer));
    }

    private void createTermBuffer(final long termId) throws IOException
    {
        final RingBuffer apiBuffer = adminBuffers.toApi();
        directory.createSenderTermFile(DESTINATION, SESSION_ID, CHANNEL_ID, termId);
        identifiedMessage.channelId(CHANNEL_ID)
                         .sessionId(SESSION_ID)
                         .termId(termId)
                         .destination(DESTINATION);
        assertTrue(apiBuffer.write(NEW_SEND_BUFFER_NOTIFICATION, atomicSendBuffer, 0, identifiedMessage.length()));
    }

    @Test
    public void removingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final RingBuffer buffer = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        final ClientAdminThread adminThread = aeron.adminThread();

        adminThread.process();
        skip(buffer, 1);

        channel.close();
        adminThread.process();

        assertChannelMessage(buffer, REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceRemovesItsAssociatedChannels() throws Exception
    {
        final RingBuffer buffer = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Source.Builder sourceBuilder = new Source.Builder()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceBuilder);
        final Channel channel = source.newChannel(CHANNEL_ID);
        final ClientAdminThread adminThread = aeron.adminThread();

        adminThread.process();
        skip(buffer, 1);

        source.close();
        adminThread.process();

        assertChannelMessage(buffer, REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceDoesNotRemoveOtherChannels() throws Exception
    {
        final RingBuffer buffer = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Source source = aeron.newSource(new Source.Builder()
                .sessionId(SESSION_ID)
                .destination(new Destination(DESTINATION)));
        final Source otherSource = aeron.newSource(new Source.Builder()
                .sessionId(SESSION_ID + 1)
                .destination(new Destination(DESTINATION)));
        final Channel channel = source.newChannel(CHANNEL_ID);
        final ClientAdminThread adminThread = aeron.adminThread();

        adminThread.process();
        skip(buffer, 1);

        otherSource.close();
        adminThread.process();

        final int eventsRead = buffer.read((eventTypeId, atomicBuffer, index, length) ->
        {
        });
        assertThat(eventsRead, is(0));
    }

    private Channel newChannel(final Aeron aeron)
    {
        final Source.Builder sourceBuilder = new Source.Builder()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceBuilder);
        return source.newChannel(CHANNEL_ID);
    }

    private Aeron newAeron()
    {
        final Aeron.Builder builder = new Aeron.Builder()
             .adminBufferStrategy(new MappingAdminBufferStrategy(adminBuffers.adminDir()));
        return Aeron.newSingleMediaDriver(builder);
    }

    private void assertChannelMessage(final RingBuffer mediaDriverBuffer, final int expectedEventTypeId)
    {
        int eventsRead = mediaDriverBuffer.read((eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(expectedEventTypeId));
            message.wrap(buffer, index);
            assertThat(message.destination(), is(DESTINATION));
            assertThat(message.channelId(), is(CHANNEL_ID));
            assertThat(message.sessionId(), is(SESSION_ID));
        });
        assertThat(eventsRead, is(1));
    }

    private void skip(final RingBuffer mediaDriverBuffer, int count)
    {
        int eventsRead = mediaDriverBuffer.read((eventTypeId, buffer, index, length) ->
        {
        }, count);
        assertThat(eventsRead, is(count));
    }

}
