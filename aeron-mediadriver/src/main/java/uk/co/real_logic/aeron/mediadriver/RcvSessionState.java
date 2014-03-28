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

import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class RcvSessionState
{
    private final InetSocketAddress srcAddr;
    private final long sessionId;
    private final Long2ObjectHashMap<ByteBuffer> termStateMap;

    public RcvSessionState(final long sessionId, final InetSocketAddress srcAddr)
    {
        this.srcAddr = srcAddr;
        this.sessionId = sessionId;
        this.termStateMap = new Long2ObjectHashMap<>();
    }

    public void termBuffer(final long termId, final ByteBuffer termBuffer)
    {
        termStateMap.put(termId, termBuffer);
    }

    public ByteBuffer termBuffer(final long termId)
    {
        return termStateMap.get(termId);
    }

    public InetSocketAddress sourceAddress()
    {
        return srcAddr;
    }
}
