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
package uk.co.real_logic.aeron.util.collections;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility functions for collection objects in general.
 */
public class CollectionUtil
{

    /**
     * A getOrDefault that doesn't create garbage if its suppler is noncapturing.
     */
    public static <K, V> V getOrDefault(Map<K, V> map, K key, Function<K, V> supplier)
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
