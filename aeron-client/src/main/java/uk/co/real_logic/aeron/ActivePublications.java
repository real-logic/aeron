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

import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;

import java.util.HashMap;
import java.util.Map;

import static uk.co.real_logic.agrona.collections.CollectionUtil.getOrDefault;

/**
 * Map for navigating to active {@link Publication}s. These are keyed by a triple of channel/sessionId/streamId.
 */
public class ActivePublications
{
    private final Map<String, BiInt2ObjectMap<Publication>> publicationsByChannelMap = new HashMap<>();

    public Publication get(final String channel, final int sessionId, final int streamId)
    {
        final BiInt2ObjectMap<Publication> publicationBySessionAndStreamMap = publicationsByChannelMap.get(channel);

        if (null == publicationBySessionAndStreamMap)
        {
            return null;
        }

        return publicationBySessionAndStreamMap.get(sessionId, streamId);
    }

    public Publication put(final String channel, final int sessionId, final int streamId, final Publication value)
    {
        final BiInt2ObjectMap<Publication> publicationBySessionAndStreamMap =
            getOrDefault(publicationsByChannelMap, channel, (ignore) -> new BiInt2ObjectMap<>());

        return publicationBySessionAndStreamMap.put(sessionId, streamId, value);
    }

    public Publication remove(final String channel, final int sessionId, final int streamId)
    {
        final BiInt2ObjectMap<Publication> publicationBySessionAndStreamMap = publicationsByChannelMap.get(channel);

        if (null == publicationBySessionAndStreamMap)
        {
            return null;
        }

        final Publication publication = publicationBySessionAndStreamMap.remove(sessionId, streamId);

        if (publicationBySessionAndStreamMap.isEmpty())
        {
            publicationsByChannelMap.remove(channel);
        }

        return publication;
    }
}
