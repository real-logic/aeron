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

import org.junit.ClassRule;
import org.junit.Test;
import uk.co.real_logic.aeron.util.MappingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ADD_CHANNEL;

public class AeronTest
{

    private static final String DESTINATION = "udp://localhost:40124";
    private static final long CHANNEL_ID = 2L;
    private static final long SESSION_ID = 3L;

    @ClassRule
    public static SharedDirectory directory = new SharedDirectory();

    @ClassRule
    public static AdminBuffers adminBuffers = new AdminBuffers();

    private final ChannelMessageFlyweight message = new ChannelMessageFlyweight();

    @Test
    public void creatingAChannelNotifiesMediaDriver() throws Exception
    {
        final Aeron.Builder builder = new Aeron.Builder()
             .adminBufferStrategy(new MappingAdminBufferStrategy(adminBuffers.adminDir()));
        final Aeron aeron = Aeron.newSingleMediaDriver(builder);

        final Source.Builder sourceBuilder = new Source.Builder()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceBuilder);

        final Channel channel = source.newChannel(CHANNEL_ID);

        aeron.adminThread().process();

        final RingBuffer mediaDriverBuffer = new ManyToOneRingBuffer(adminBuffers.toMediaDriver());
        int eventsRead = mediaDriverBuffer.read((eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ADD_CHANNEL));
            message.wrap(buffer, index);
            assertThat(message.destination(), is(DESTINATION));
            assertThat(message.channelId(), is(CHANNEL_ID));
            assertThat(message.sessionId(), is(SESSION_ID));
        });

        assertThat(eventsRead, is(1));
    }

}
