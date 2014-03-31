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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Cursor for writing into the Sender Thread's command buffer.
 */
public class SenderThreadCursor
{
    private static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer buffer;
    private final AtomicBuffer writeBuffer;
    private final CompletelyIdentifiedMessageFlyweight identifiedMessage;
    private final ChannelMessageFlyweight channelMessage;

    public SenderThreadCursor(final RingBuffer buffer)
    {
        this.buffer = buffer;
        writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

        identifiedMessage = new CompletelyIdentifiedMessageFlyweight();
        identifiedMessage.reset(writeBuffer, 0);

        channelMessage = new ChannelMessageFlyweight();
        channelMessage.reset(writeBuffer, 0);
    }

    public void addNewTermEvent(final long sessionId,
                                final long channelId,
                                final long termId)
    {
        addIdentifiedMessage(NEW_SEND_BUFFER_NOTIFICATION, sessionId, channelId, termId);
    }

    public void addRemoveTermEvent(final long sessionId,
                                   final long channelId,
                                   final long termId)
    {
        addIdentifiedMessage(REMOVE_TERM, sessionId, channelId, termId);
    }

    private void addIdentifiedMessage(final int eventTypeId,
                                      final long sessionId,
                                      final long channelId,
                                      final long termId)
    {
        identifiedMessage.sessionId(sessionId);
        identifiedMessage.channelId(channelId);
        identifiedMessage.termId(termId);
        buffer.write(eventTypeId, writeBuffer, 0, identifiedMessage.length());
    }

    public void addRemoveChannelEvent(final long sessionId,
                                      final long channelId)
    {
        channelMessage.channelId(sessionId);
        channelMessage.channelId(channelId);
        buffer.write(REMOVE_CHANNEL, writeBuffer, 0, channelMessage.length());
    }

    public void addStatusMessageEvent(final HeaderFlyweight header)
    {
        // TODO: serialize frame on to command buffer
    }

}
