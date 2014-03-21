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

import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;

/**
 * Thread to take data in sender buffers and demux onto sending sockets
 */
public class SenderThread extends ClosableThread
{
    private final ByteBuffer commandBuffer;
    private final ByteBuffer adminCommandBuffer;

    public SenderThread(final ByteBuffer commandBuffer, final ByteBuffer adminCommandBuffer)
    {
        this.commandBuffer = commandBuffer;
        this.adminCommandBuffer = adminCommandBuffer;
    }

    public void process()
    {
        // TODO: handle data to send (with )
        // TODO: handle commands added to command buffer (call onNewSenderTerm, onStatusMessage, etc.)
    }

    public void offerNewSenderTerm(final long sessionId,
                                   final long channelId,
                                   final long termId,
                                   final ByteBuffer buffer)
    {

    }

    public void offerStatusMessage(final HeaderFlyweight header)
    {
        // TODO: serialize frame on to command buffer
    }

    public void onNewSenderTerm(final long sessionId, final long channelId, final long termId, final ByteBuffer buffer)
    {

    }

    public void onStatusMessage(final HeaderFlyweight header)
    {
        // TODO: handle Status Message, perhaps sending more data
    }
}
