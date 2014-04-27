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
package uk.co.real_logic.aeron.util.concurrent;

import uk.co.real_logic.aeron.util.BitUtil;


import java.util.*;

import static uk.co.real_logic.aeron.util.UnsafeAccess.UNSAFE;

/**
 * Pad out a cacheline to the left of a tail to prevent false sharing.
 */
class Padding1
{
    protected volatile long p1, p2, p3, p4, p5, p6, p7;
}

/**
 * Value for the tail that is expected to be padded.
 */
class Tail extends Padding1
{
    protected volatile long tail;
}

/**
 * Pad out a cacheline between the tail and the head to prevent false sharing.
 */
class Padding2 extends Tail
{
    protected volatile long p1, p2, p3, p4, p5, p6, p7;
}

/**
 * Value for the head that is expected to be padded.
 */
class Head extends Padding2
{
    protected volatile long head;
}

/**
 * Pad out a cacheline between the tail and the head to prevent false sharing.
 */
class Padding3 extends Head
{
    protected volatile long p1, p2, p3, p4, p5, p6, p7;
}

public class OneToOneConcurrentArrayQueue<E>
    extends Padding3
    implements Queue<E>
{
    private static final int ARRAY_BASE;
    private static final int SHIFT_FOR_SCALE;
    private static final long TAIL_OFFSET;
    private static final long HEAD_OFFSET;

    static
    {
        try
        {
            ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class);
            SHIFT_FOR_SCALE = BitUtil.calculateShiftForScale(UNSAFE.arrayIndexScale(Object[].class));
            TAIL_OFFSET = UNSAFE.objectFieldOffset(Tail.class.getDeclaredField("tail"));
            HEAD_OFFSET = UNSAFE.objectFieldOffset(Head.class.getDeclaredField("head"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private final int capacity;
    private final int mask;
    private final E[] buffer;

    @SuppressWarnings("unchecked")
    public OneToOneConcurrentArrayQueue(final int capacity)
    {
        this.capacity = BitUtil.findNextPositivePowerOfTwo(capacity);
        mask = this.capacity - 1;
        buffer = (E[])new Object[this.capacity];
    }

    public boolean add(final E e)
    {
        if (offer(e))
        {
            return true;
        }

        throw new IllegalStateException("Queue is full");
    }

    public boolean offer(final E e)
    {
        if (null == e)
        {
            throw new NullPointerException("Null is not a valid element");
        }

        final long offset = indexToOffset((int)tail & mask);
        if (null == UNSAFE.getObjectVolatile(buffer, offset))
        {
            UNSAFE.putOrderedObject(buffer, offset, e);
            tailOrdered(tail + 1);

            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    public E poll()
    {
        final long offset = indexToOffset((int)head & mask);

        final Object e = UNSAFE.getObjectVolatile(buffer, offset);
        if (null != e)
        {
            UNSAFE.putOrderedObject(buffer, offset, null);
            headOrdered(head + 1);
        }

        return (E)e;
    }

    public E remove()
    {
        final E e = poll();
        if (null == e)
        {
            throw new NoSuchElementException("Queue is empty");
        }

        return e;
    }

    public E element()
    {
        final E e = peek();
        if (null == e)
        {
            throw new NoSuchElementException("Queue is empty");
        }

        return e;
    }

    public E peek()
    {
        return buffer[(int)head & mask];
    }

    public int size()
    {
        int size;
        do
        {
            final long currentHead = head;
            final long currentTail = tail;
            size = (int)(currentTail - currentHead);
        }
        while (size > capacity);

        return size;
    }

    public boolean isEmpty()
    {
        return tail == head;
    }

    public boolean contains(final Object o)
    {
        if (null == o)
        {
            return false;
        }

        for (long i = head, limit = tail; i < limit; i++)
        {
            final E e = buffer[(int)i & mask];
            if (o.equals(e))
            {
                return true;
            }
        }

        return false;
    }

    public Iterator<E> iterator()
    {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray()
    {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(final T[] a)
    {
        throw new UnsupportedOperationException();
    }

    public boolean remove(final Object o)
    {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(final Collection<?> c)
    {
        for (final Object o : c)
        {
            if (!contains(o))
            {
                return false;
            }
        }

        return true;
    }

    public boolean addAll(final Collection<? extends E> c)
    {
        for (final E e : c)
        {
            add(e);
        }

        return true;
    }

    public boolean removeAll(final Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(final Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public void clear()
    {
        Object value;
        do
        {
            value = poll();
        }
        while (null != value);
    }

    private void tailOrdered(final long tail)
    {
        UNSAFE.putOrderedLong(this, TAIL_OFFSET, tail);
    }

    private void headOrdered(final long head)
    {
        UNSAFE.putOrderedLong(this, HEAD_OFFSET, head);
    }

    private static long indexToOffset(final int index)
    {
        return ARRAY_BASE + ((long)index << SHIFT_FOR_SCALE);
    }
}
