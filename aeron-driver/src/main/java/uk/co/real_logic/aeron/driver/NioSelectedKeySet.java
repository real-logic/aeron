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

package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.BitUtil;

import java.nio.channels.SelectionKey;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.ToIntFunction;

/**
 * Try to fix handling of HashSet for {@link java.nio.channels.Selector}. Akin to netty's SelectedSelectionKeySet.
 * Assumes single threaded usage.
 */
public class NioSelectedKeySet extends AbstractSet<SelectionKey>
{
    private static final int INITIAL_CAPACITY = 16;

    private SelectionKey[] keys;
    private int size;


    public NioSelectedKeySet()
    {
        this(INITIAL_CAPACITY);
    }

    public NioSelectedKeySet(final int initialCapacity)
    {
        size = 0;
        keys = new SelectionKey[BitUtil.findNextPositivePowerOfTwo(initialCapacity)];
    }

    public int size()
    {
        return size;
    }

    public int capacity()
    {
        return keys.length;
    }

    public boolean add(final SelectionKey selectionKey)
    {
        if (null == selectionKey)
        {
            return false;
        }

        ensureCapacity(size + 1);
        keys[size++] = selectionKey;
        return true;
    }

    public boolean remove(final Object o)
    {
        return false;
    }

    public boolean contains(final Object o)
    {
        return false;
    }

    public int forEach(final ToIntFunction<SelectionKey> function)
    {
        int handledFrames = 0;

        for (int i = size - 1; i >= 0; i--)
        {
            handledFrames += function.applyAsInt(keys[i]);
        }

        size = 0;
        return handledFrames;
    }

    public Iterator<SelectionKey> iterator()
    {
        throw new UnsupportedOperationException();
    }

    private void ensureCapacity(final int requiredCapacity)
    {
        if (requiredCapacity < 0)
        {
            final String s =
                String.format("Insufficient capacity: length=%d required=%d", keys.length, requiredCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > keys.length)
        {
            final int newCapacity = BitUtil.findNextPositivePowerOfTwo(requiredCapacity);
            final SelectionKey[] newKeys = Arrays.copyOf(keys, newCapacity);

            keys = newKeys;
        }
    }
}
