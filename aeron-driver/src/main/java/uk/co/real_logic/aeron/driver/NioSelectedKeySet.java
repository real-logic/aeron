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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.BitUtil;

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
    private int size = 0;

    /**
     * Construct a key set with default capacity
     */
    public NioSelectedKeySet()
    {
        this(INITIAL_CAPACITY);
    }

    /**
     * Construct a key set with the given capacity.
     *
     * @param initialCapacity for the key set
     */
    public NioSelectedKeySet(final int initialCapacity)
    {
        keys = new SelectionKey[BitUtil.findNextPositivePowerOfTwo(initialCapacity)];
    }

    /** {@inheritDoc} */
    public int size()
    {
        return size;
    }

    /**
     * Capacity of the current set
     *
     * @return capacity of the set
     */
    public int capacity()
    {
        return keys.length;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    public boolean remove(final Object o)
    {
        return false;
    }

    /** {@inheritDoc} */
    public boolean contains(final Object o)
    {
        return false;
    }

    /**
     * Return selected keys.
     *
     * @return selected keys
     */
    public SelectionKey[] keys()
    {
        return keys;
    }

    /**
     * Reset for next iteration.
     */
    public void reset()
    {
        size = 0;
    }

    /**
     * Iterate over the key set and apply the given function.
     *
     * @param function to apply to each {@link java.nio.channels.SelectionKey}
     * @return number of handled frames
     */
    public int forEach(final ToIntFunction<SelectionKey> function)
    {
        int handledFrames = 0;
        final SelectionKey[] keys = this.keys;

        for (int i = size - 1; i >= 0; i--)
        {
            handledFrames += function.applyAsInt(keys[i]);
        }

        size = 0;

        return handledFrames;
    }

    /** {@inheritDoc} */
    public Iterator<SelectionKey> iterator()
    {
        throw new UnsupportedOperationException();
    }

    private void ensureCapacity(final int requiredCapacity)
    {
        if (requiredCapacity < 0)
        {
            final String s = String.format("Insufficient capacity: length=%d required=%d", keys.length, requiredCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > keys.length)
        {
            final int newCapacity = BitUtil.findNextPositivePowerOfTwo(requiredCapacity);
            keys = Arrays.copyOf(keys, newCapacity);
        }
    }
}
