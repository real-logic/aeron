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

import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

/**
 * Thread to take data in sender buffers and demux onto sending sockets
 */
public class SenderThread extends ClosableThread
{
    private final RingBuffer adminThreadCommandBuffer;
    private final AtomicArray<SenderChannel> channels;
    private final ChannelMap<SenderChannel> channelMap;

    public SenderThread(final MediaDriver.TopologyBuilder builder)
    {
        this.adminThreadCommandBuffer = builder.adminThreadCommandBuffer();
        this.channels = new AtomicArray<>();
        this.channelMap = new ChannelMap<>();  // needed for demux of SMs to SenderChannel in this thread
    }

    @Override
    public void process()
    {
        // TODO: handle commandBuffer and dispatch to onStatusMessage so that the channels can be iterated with up to
        // date flow control information before trying to send
        channels.forEach((channel) ->
        {
            if (channel.state() == SenderChannel.STATE_PENDING)
            {
                channelMap.put(channel.destination(), channel.sessionId(), channel.sessionId(), channel);
                channel.state(SenderChannel.STATE_READY_FOR_SM);
            }
            channel.process();
        });
    }

    public void addChannel(final SenderChannel channel)
    {
        channels.add(channel);
    }

    public void removeChannel(final SenderChannel channel)
    {
        channels.remove(channel);
    }

    public void onStatusMessage(final HeaderFlyweight header)
    {
        // TODO: grab channel from channelMap by looking up destination, sessionId, channelId, and termId
        // TODO: need a way to handle destination lookup... can't serialize it for each SM...
        // TODO: perhaps we generate a unique ID (hash?) for each destination when it is created on the admin thread
        final SenderChannel channel = null;

        final SenderFlowControlState state = channel.flowControlState();

        // TODO: strategy for tracking multiple receivers would go here
        state.reset(0, 0);
    }
}
