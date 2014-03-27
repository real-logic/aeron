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

import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

/**
 * Separates the concern of talking the media driver protocol away from the rest of the API.
 *
 * Writes messages into the Client Admin Thread's admin buffer.
 */
public class ClientAdminCursor
{

    private final long sessionId;
    private final RingBuffer adminThreadCommandBuffer;

    public ClientAdminCursor(final long sessionId, final RingBuffer adminThreadCommandBuffer)
    {
        this.sessionId = sessionId;
        this.adminThreadCommandBuffer = adminThreadCommandBuffer;
    }

    public void sendAddChannel(final String destination, final long channelId)
    {

    }

    public void sendRemoveChannel(final String destination, final long channelId)
    {

    }

    public void sendAddReceiver(final String destination, final long[] channelIdList)
    {

    }

    public void sendRemoveReceiver(final String destination, final long[] channelIdList)
    {

    }

    public void sendRequestTerm(final String destination, final long channelId, final long termId)
    {

    }

}
