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

import java.util.Map;
import java.util.function.Function;

/**
 * Utility functions for collection objects in general.
 */
public class CollectionUtil
{
    /**
     * A getOrDefault that doesn't create garbage if its suppler is non-capturing.
     *
     * @param map to perform the lookup on.
     * @param key on which the lookup is done.
     * @param supplier of the default value if one is not found.
     * @param <K> type of the key
     * @param <V> type of the value
     * @return the value if found or a new default which as been added to the map.
     */
    public static <K, V> V getOrDefault(final Map<K, V> map, final K key, final Function<K, V> supplier)
    {
        V value = map.get(key);
        if (value == null)
        {
            value = supplier.apply(key);
            map.put(key, value);
        }

        return value;
    }
}
