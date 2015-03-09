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
package uk.co.real_logic.aeron.common.collections;

import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;

import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.agrona.collections.CollectionUtil.getOrDefault;

/**
 * Map for storing information about Aeron associations. These are keyed
 * by a triple of channel/session/stream.
 */
public class ConnectionMap<D, C>
{
    private final Map<D, BiInt2ObjectMap<C>> channelMap = new HashMap<>();

    public C get(final D channel, final int sessionId, final int streamId)
    {
        final BiInt2ObjectMap<C> idMap = channelMap.get(channel);

        if (null == idMap)
        {
            return null;
        }

        return idMap.get(sessionId, streamId);
    }

    public C put(final D channel, final int sessionId, final int streamId, final C value)
    {
        final BiInt2ObjectMap<C> idMap = getOrDefault(channelMap, channel, (ignore) -> new BiInt2ObjectMap<>());

        return idMap.put(sessionId, streamId, value);
    }

    public C remove(final D channel, final int sessionId, final int streamId)
    {
        final BiInt2ObjectMap<C> idMap = channelMap.get(channel);

        if (null == idMap)
        {
            return null;
        }

        final C value = idMap.remove(sessionId, streamId);

        if (idMap.isEmpty())
        {
            channelMap.remove(channel);
        }

        return value;
    }

    public interface ConnectionConsumer<D, T>
    {
        void accept(final D channel, final Integer sessionId, final Integer streamId, final T value);
    }

    public void forEach(final ConnectionConsumer<D, C> connectionConsumer)
    {
        channelMap.forEach(
            (channel, idMap) ->
                idMap.forEach(
                    (sessionId, streamId, value) -> connectionConsumer.accept(channel, sessionId, streamId, value)));
    }
}
