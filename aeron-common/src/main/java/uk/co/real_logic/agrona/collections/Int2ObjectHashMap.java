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


import uk.co.real_logic.agrona.BitUtil;

import java.util.*;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * {@link java.util.Map} implementation specialised for int keys using open addressing and
 * linear probing for cache efficient access.
 *
 * @param <V> values stored in the {@link java.util.Map}
 */
public class Int2ObjectHashMap<V>
    implements Map<Integer, V>
{
    private final double loadFactor;
    private int resizeThreshold;
    private int capacity;
    private int mask;
    private int size;

    private int[] keys;
    private Object[] values;

    // Cached to avoid allocation.
    private final ValueCollection<V> valueCollection = new ValueCollection<>();
    private final KeySet keySet = new KeySet();
    private final EntrySet<V> entrySet = new EntrySet<>();

    public Int2ObjectHashMap()
    {
        this(8, 0.6);
    }

    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     *
     * @param initialCapacity for the backing array
     * @param loadFactor      limit for resizing on puts
     */
    public Int2ObjectHashMap(final int initialCapacity, final double loadFactor)
    {
        this.loadFactor = loadFactor;
        capacity = BitUtil.findNextPositivePowerOfTwo(initialCapacity);
        mask = capacity - 1;
        resizeThreshold = (int)(capacity * loadFactor);

        keys = new int[capacity];
        values = new Object[capacity];
    }

    /**
     * Get the load factor beyond which the map will increase size.
     *
     * @return load factor for when the map should increase size.
     */
    public double getLoadFactor()
    {
        return loadFactor;
    }

    /**
     * Get the total capacity for the map to which the load factor with be a fraction of.
     *
     * @return the total capacity for the map.
     */
    public int getCapacity()
    {
        return capacity;
    }

    /**
     * Get the actual threshold which when reached the map resize.
     * This is a function of the current capacity and load factor.
     *
     * @return the threshold when the map will resize.
     */
    public int getResizeThreshold()
    {
        return resizeThreshold;
    }

    /**
     * {@inheritDoc}
     */
    public int size()
    {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty()
    {
        return 0 == size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsKey(final Object key)
    {
        requireNonNull(key, "Null keys are not permitted");

        return containsKey(((Integer)key).intValue());
    }

    /**
     * Overloaded version of {@link Map#containsKey(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return true if the key is found otherwise false.
     */
    public boolean containsKey(final int key)
    {
        int index = hash(key);

        while (null != values[index])
        {
            if (key == keys[index])
            {
                return true;
            }

            index = ++index & mask;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsValue(final Object value)
    {
        requireNonNull(value, "Null values are not permitted");

        for (final Object v : values)
        {
            if (null != v && value.equals(v))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    public V get(final Object key)
    {
        return get(((Integer)key).intValue());
    }

    /**
     * Overloaded version of {@link Map#get(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise null
     */
    @SuppressWarnings("unchecked")
    public V get(final int key)
    {
        int index = hash(key);

        Object value;
        while (null != (value = values[index]))
        {
            if (key == keys[index])
            {
                return (V)value;
            }

            index = ++index & mask;
        }

        return null;
    }

    /**
     * Get a value for a given key, or if it does ot exist then default the value via a {@link Supplier}
     * and put it in the map.
     *
     * @param key to search on.
     * @param supplier to provide a default if the get returns null.
     * @return the value if found otherwise the default.
     */
    public V getOrDefault(final int key, final Supplier<V> supplier)
    {
        V value = get(key);
        if (value == null)
        {
            value = supplier.get();
            put(key, value);
        }

        return value;
    }

    /**
     * {@inheritDoc}
     */
    public V put(final Integer key, final V value)
    {
        return put(key.intValue(), value);
    }

    /**
     * Overloaded version of {@link Map#put(Object, Object)} that takes a primitive int key.
     *
     * @param key   for indexing the {@link Map}
     * @param value to be inserted in the {@link Map}
     * @return the previous value if found otherwise null
     */
    @SuppressWarnings("unchecked")
    public V put(final int key, final V value)
    {
        requireNonNull(value, "Value cannot be null");

        V oldValue = null;
        int index = hash(key);

        while (null != values[index])
        {
            if (key == keys[index])
            {
                oldValue = (V)values[index];
                break;
            }

            index = ++index & mask;
        }

        if (null == oldValue)
        {
            ++size;
            keys[index] = key;
        }

        values[index] = value;

        if (size > resizeThreshold)
        {
            increaseCapacity();
        }

        return oldValue;
    }

    /**
     * {@inheritDoc}
     */
    public V remove(final Object key)
    {
        return remove(((Integer)key).intValue());
    }

    /**
     * Overloaded version of {@link Map#remove(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise null
     */
    @SuppressWarnings("unchecked")
    public V remove(final int key)
    {
        int index = hash(key);

        Object value;
        while (null != (value = values[index]))
        {
            if (key == keys[index])
            {
                values[index] = null;
                --size;

                compactChain(index);

                return (V)value;
            }

            index = ++index & mask;
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    public void clear()
    {
        size = 0;
        Arrays.fill(values, null);
    }

    /**
     * Compact the {@link Map} backing arrays by rehashing with a capacity just larger than current size
     * and giving consideration to the load factor.
     */
    public void compact()
    {
        final int idealCapacity = (int)Math.round(size() * (1.0d / loadFactor));
        rehash(BitUtil.findNextPositivePowerOfTwo(idealCapacity));
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(final Map<? extends Integer, ? extends V> map)
    {
        for (final Entry<? extends Integer, ? extends V> entry : map.entrySet())
        {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    public KeySet keySet()
    {
        return keySet;
    }

    /**
     * {@inheritDoc}
     */
    public Collection<V> values()
    {
        return valueCollection;
    }

    /**
     * {@inheritDoc}
     */
    public Set<Entry<Integer, V>> entrySet()
    {
        return entrySet;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append('{');

        for (final Entry<Integer, V> entry : entrySet())
        {
            sb.append(entry.getKey().intValue());
            sb.append('=');
            sb.append(entry.getValue());
            sb.append(", ");
        }

        if (sb.length() > 1)
        {
            sb.setLength(sb.length() - 2);
        }

        sb.append('}');

        return sb.toString();
    }

    private void increaseCapacity()
    {
        final int newCapacity = capacity << 1;
        if (newCapacity < 0)
        {
            throw new IllegalStateException("Max capacity reached at size=" + size);
        }

        rehash(newCapacity);
    }

    private void rehash(final int newCapacity)
    {
        if (1 != Integer.bitCount(newCapacity))
        {
            throw new IllegalStateException("New capacity must be a power of two");
        }

        capacity = newCapacity;
        mask = newCapacity - 1;
        resizeThreshold = (int)(newCapacity * loadFactor);

        final int[] tempKeys = new int[capacity];
        final Object[] tempValues = new Object[capacity];

        for (int i = 0, size = values.length; i < size; i++)
        {
            final Object value = values[i];
            if (null != value)
            {
                final int key = keys[i];
                int newHash = hash(key);
                while (null != tempValues[newHash])
                {
                    newHash = ++newHash & mask;
                }

                tempKeys[newHash] = key;
                tempValues[newHash] = value;
            }
        }

        keys = tempKeys;
        values = tempValues;
    }

    private void compactChain(int deleteIndex)
    {
        int index = deleteIndex;
        while (true)
        {
            index = ++index & mask;
            if (null == values[index])
            {
                return;
            }

            final int hash = hash(keys[index]);

            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                (hash <= deleteIndex && deleteIndex <= index))
            {
                keys[deleteIndex] = keys[index];
                values[deleteIndex] = values[index];

                values[index] = null;
                deleteIndex = index;
            }
        }
    }

    private int hash(final int key)
    {
        final int hash = (key << 1) - (key << 8);
        return hash & mask;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Internal Sets and Collections
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public class KeySet extends AbstractSet<Integer>
    {
        public int size()
        {
            return Int2ObjectHashMap.this.size();
        }

        public boolean isEmpty()
        {
            return Int2ObjectHashMap.this.isEmpty();
        }

        public boolean contains(final Object o)
        {
            return Int2ObjectHashMap.this.containsKey(o);
        }

        public boolean contains(final int key)
        {
            return Int2ObjectHashMap.this.containsKey(key);
        }

        public KeyIterator iterator()
        {
            return new KeyIterator();
        }

        public boolean remove(final Object o)
        {
            return null != Int2ObjectHashMap.this.remove(o);
        }

        public boolean remove(final int key)
        {
            return null != Int2ObjectHashMap.this.remove(key);
        }

        public void clear()
        {
            Int2ObjectHashMap.this.clear();
        }
    }

    private class ValueCollection<V> extends AbstractCollection<V>
    {
        public int size()
        {
            return Int2ObjectHashMap.this.size();
        }

        public boolean isEmpty()
        {
            return Int2ObjectHashMap.this.isEmpty();
        }

        public boolean contains(final Object o)
        {
            return Int2ObjectHashMap.this.containsValue(o);
        }

        public ValueIterator<V> iterator()
        {
            return new ValueIterator<>();
        }

        public void clear()
        {
            Int2ObjectHashMap.this.clear();
        }
    }

    private class EntrySet<V> extends AbstractSet<Entry<Integer, V>>
    {
        public int size()
        {
            return Int2ObjectHashMap.this.size();
        }

        public boolean isEmpty()
        {
            return Int2ObjectHashMap.this.isEmpty();
        }

        public Iterator<Entry<Integer, V>> iterator()
        {
            return new EntryIterator<>();
        }

        public void clear()
        {
            Int2ObjectHashMap.this.clear();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Iterators
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private abstract class AbstractIterator<T> implements Iterator<T>
    {
        private int posCounter;
        private int stopCounter;
        private boolean isPositionValid = false;
        protected final int[] keys = Int2ObjectHashMap.this.keys;
        protected final Object[] values = Int2ObjectHashMap.this.values;

        protected AbstractIterator()
        {
            int i = capacity;
            if (null != values[capacity - 1])
            {
                i = 0;
                for (int size = capacity; i < size; i++)
                {
                    if (null == values[i])
                    {
                        break;
                    }
                }
            }

            stopCounter = i;
            posCounter = i + capacity;
        }

        protected int getPosition()
        {
            return posCounter & mask;
        }

        public boolean hasNext()
        {
            for (int i = posCounter - 1; i >= stopCounter; i--)
            {
                final int index = i & mask;
                if (null != values[index])
                {
                    return true;
                }
            }

            return false;
        }

        protected void findNext()
        {
            isPositionValid = false;

            for (int i = posCounter - 1; i >= stopCounter; i--)
            {
                final int index = i & mask;
                if (null != values[index])
                {
                    posCounter = i;
                    isPositionValid = true;
                    return;
                }
            }

            throw new NoSuchElementException();
        }

        public abstract T next();

        public void remove()
        {
            if (isPositionValid)
            {
                final int position = getPosition();
                values[position] = null;
                --size;

                compactChain(position);

                isPositionValid = false;
            }
            else
            {
                throw new IllegalStateException();
            }
        }
    }

    public class ValueIterator<T> extends AbstractIterator<T>
    {
        @SuppressWarnings("unchecked")
        public T next()
        {
            findNext();

            return (T)values[getPosition()];
        }
    }

    public class KeyIterator extends AbstractIterator<Integer>
    {
        public Integer next()
        {
            return nextInt();
        }

        public int nextInt()
        {
            findNext();

            return keys[getPosition()];
        }
    }

    @SuppressWarnings("unchecked")
    public class EntryIterator<V>
        extends AbstractIterator<Entry<Integer, V>>
        implements Entry<Integer, V>
    {
        public Entry<Integer, V> next()
        {
            findNext();

            return this;
        }

        public Integer getKey()
        {
            return keys[getPosition()];
        }

        public V getValue()
        {
            return (V)values[getPosition()];
        }

        public V setValue(final V value)
        {
            requireNonNull(value);

            final int pos = getPosition();
            final Object oldValue = values[pos];
            values[pos] = value;

            return (V)oldValue;
        }
    }
}
