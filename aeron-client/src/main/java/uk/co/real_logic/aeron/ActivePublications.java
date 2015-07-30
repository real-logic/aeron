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

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static uk.co.real_logic.agrona.collections.CollectionUtil.getOrDefault;

/**
 * Map for navigating to active {@link Publication}s.
 */
public class ActivePublications
{
    private final Map<String, Int2ObjectHashMap<Publication>> publicationsByChannelMap = new HashMap<>();

    public Publication get(final String channel, final int streamId)
    {
        final Int2ObjectHashMap<Publication> publicationByStreamIdMap = publicationsByChannelMap.get(channel);
        if (null == publicationByStreamIdMap)
        {
            return null;
        }

        return publicationByStreamIdMap.get(streamId);
    }

    public Publication put(final String channel, final int streamId, final Publication publication)
    {
        final Int2ObjectHashMap<Publication> publicationByStreamIdMap =
            getOrDefault(publicationsByChannelMap, channel, (ignore) -> new Int2ObjectHashMap<>());

        return publicationByStreamIdMap.put(streamId, publication);
    }

    public Publication remove(final String channel, final int streamId)
    {
        final Int2ObjectHashMap<Publication> publicationByStreamIdMap = publicationsByChannelMap.get(channel);
        if (null == publicationByStreamIdMap)
        {
            return null;
        }

        final Publication publication = publicationByStreamIdMap.remove(streamId);
        if (publicationByStreamIdMap.isEmpty())
        {
            publicationsByChannelMap.remove(channel);
        }

        return publication;
    }

    public void close()
    {
        publicationsByChannelMap
            .values()
            .stream()
            .flatMap((publicationByStreamIdMap) -> publicationByStreamIdMap.values().stream())
            .collect(toList())
            .forEach(Publication::release);
    }
}
