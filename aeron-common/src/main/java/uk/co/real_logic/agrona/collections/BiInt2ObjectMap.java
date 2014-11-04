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

package uk.co.real_logic.agrona.collections;

import java.util.function.Consumer;

/**
 * Map that takes a sessionId and streamId and associates with an object
 */
public class BiInt2ObjectMap<V>
{
    private final Long2ObjectHashMap<V> map;

    /**
     * Construct an empty map
     */
    public BiInt2ObjectMap()
    {
        map = new Long2ObjectHashMap<>();
    }

    /**
     * See {@link Long2ObjectHashMap#Long2ObjectHashMap(int, double)}.
     *
     * @param initialCapacity for the underlying hash map
     * @param loadFactor for the underlying hash map
     */
    public BiInt2ObjectMap(final int initialCapacity, final double loadFactor)
    {
        map = new Long2ObjectHashMap<>(initialCapacity, loadFactor);
    }

    /**
     * Put a value into the map.
     *
     * @param sessionId for the key
     * @param streamId for the key
     * @param value to put into the map
     */
    public V put(final int sessionId, final int streamId, final V value)
    {
        final long key = compoundKey(sessionId, streamId);

        return map.put(key, value);
    }

    /**
     * Retrieve a value from the map.
     *
     * @param sessionId for the key
     * @param streamId for the key
     * @return value
     */
    public V get(final int sessionId, final int streamId)
    {
        final long key = compoundKey(sessionId, streamId);

        return map.get(key);
    }

    /**
     * Remove a value from the map and return the value.
     *
     * @param sessionId for the key
     * @param streamId for the key
     * @return value removed
     */
    public V remove(final int sessionId, final int streamId)
    {
        final long key = compoundKey(sessionId, streamId);

        return map.remove(key);
    }

    /**
     * Iterate over the contents of the map
     *
     * @param consumer to apply to each value in the map
     */
    public void forEach(final Consumer<V> consumer)
    {
        map.forEach((k, v) -> consumer.accept(v));
    }

    /**
     * Handler for a map entry
     *
     * @param <V> for the map
     */
    public interface EntryConsumer<V>
    {
        /**
         * A map entry
         *
         * @param sessionId for the key
         * @param streamId for the key
         * @param value for the entry
         */
        void accept(final int sessionId, final int streamId, final V value);
    }

    /**
     * Iterate over the contents of the map
     *
     * @param consumer to apply to each value in the map
     */
    public void forEach(final EntryConsumer<V> consumer)
    {
        map.forEach(
            (compoundKey, value) ->
            {
                final int sessionId = (int)(compoundKey >>> 32);
                final int streamId = (int)(compoundKey & 0xFFFFFFFFL);
                consumer.accept(sessionId, streamId, value);
            });
    }

    /**
     * Return the number of unique entries in the map.
     *
     * @return number of unique entries in the map.
     */
    public int size()
    {
        return map.size();
    }

    /**
     * Is map empty or not.
     *
     * @return boolean indicating empty map or not
     */
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    private long compoundKey(final int sessionId, final int streamId)
    {
        return ((long)sessionId << 32) | streamId;
    }
}
