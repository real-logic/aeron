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

import uk.co.real_logic.aeron.conductor.ChannelNotifiable;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

import static uk.co.real_logic.aeron.Consumer.DataHandler;

public class ConsumerChannel extends ChannelNotifiable
{
    private final Long2ObjectHashMap<ConsumerSession> logReaders = new Long2ObjectHashMap<>();
    private final DataHandler dataHandler;

    public ConsumerChannel(final Destination destination, final long channelId, final DataHandler dataHandler)
    {
        super(destination.destination(), channelId);

        this.dataHandler = dataHandler;
    }

    public boolean matches(final String destination, final long channelId)
    {
        return this.destination.equals(destination) && this.channelId == channelId;
    }

    public int process() throws Exception
    {
        int count = 0;
        for (final ConsumerSession consumerSession : logReaders.values())
        {
            count += consumerSession.process();
        }

        return count;
    }

    protected boolean hasTerm(final long sessionId)
    {
        final ConsumerSession consumerSession = logReaders.get(sessionId);
        return consumerSession != null && consumerSession.hasTerm();
    }

    public void onBuffersMapped(final long sessionId, final long termId, final LogReader[] logReaders)
    {
        ConsumerSession session = new ConsumerSession(logReaders, sessionId, termId, dataHandler);
        this.logReaders.put(sessionId, session);
    }

    public void processBufferScan()
    {
        logReaders.values().forEach(ConsumerSession::processBufferScan);
    }

}
