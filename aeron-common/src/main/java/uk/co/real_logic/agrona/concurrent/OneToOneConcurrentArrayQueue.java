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
package uk.co.real_logic.agrona.concurrent;

import uk.co.real_logic.agrona.BitUtil;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Consumer;

import static uk.co.real_logic.agrona.UnsafeAccess.UNSAFE;

/**
 * Pad out a cacheline to the left of a tail to prevent false sharing.
 */
class Padding1
{
    protected long p1, p2, p3, p4, p5, p6, p7;
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
    protected long p8, p9, p10, p11, p12, p13, p14;
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
    protected long p15, p16, p17, p18, p19, p20, p21;
}

/**
 * One producer to one consumer concurrent queue that is array backed. The algorithm is a a variable of Fast Flow
 * adapted to work with the Java Memory Model on arrays by using {@link sun.misc.Unsafe}.
 *
 * @param <E> type of the elements stored in the {@link Queue}.
 */
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

    private final long mask;
    private final E[] buffer;

    @SuppressWarnings("unchecked")
    public OneToOneConcurrentArrayQueue(final int requestedCapacity)
    {
        final int capacity = BitUtil.findNextPositivePowerOfTwo(requestedCapacity);
        mask = capacity - 1;
        buffer = (E[])new Object[capacity];
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

        final Object[] buffer = this.buffer;
        final long currentTail = tail;
        final long offset = sequenceToOffset(currentTail);

        if (null == UNSAFE.getObjectVolatile(buffer, offset))
        {
            UNSAFE.putOrderedObject(buffer, offset, e);
            tailOrdered(currentTail + 1);

            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    public E poll()
    {
        final Object[] buffer = this.buffer;
        final long currentHead = head;
        final long offset = sequenceToOffset(currentHead);

        final Object e = UNSAFE.getObjectVolatile(buffer, offset);
        if (null != e)
        {
            UNSAFE.putOrderedObject(buffer, offset, null);
            headOrdered(currentHead + 1);
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

    @SuppressWarnings("unchecked")
    public E peek()
    {
        return (E)UNSAFE.getObjectVolatile(buffer, sequenceToOffset(head));
    }

    public int drain(final Consumer<E> elementHandler)
    {
        final long currentHead = head;
        final long currentTail = tail;
        long nextSequence = currentHead;

        try
        {
            while (nextSequence < currentTail)
            {
                elementHandler.accept(removeSequence(nextSequence++));
            }
        }
        finally
        {
            headOrdered(nextSequence);
        }

        return (int)(currentTail - currentHead);
    }

    public int size()
    {
        long currentHeadBefore;
        long currentTail;
        long currentHeadAfter = head;

        do
        {
            currentHeadBefore = currentHeadAfter;
            currentTail = tail;
            currentHeadAfter = head;

        }
        while (currentHeadAfter != currentHeadBefore);

        return (int)(currentTail - currentHeadAfter);
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

        final Object[] buffer = this.buffer;

        for (long i = head, limit = tail; i < limit; i++)
        {
            final Object e = UNSAFE.getObjectVolatile(buffer, sequenceToOffset(i));
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

    private long sequenceToOffset(final long sequence)
    {
        return ARRAY_BASE + ((sequence & mask) << SHIFT_FOR_SCALE);
    }

    @SuppressWarnings("unchecked")
    private E removeSequence(final long sequence)
    {
        final Object[] buffer = this.buffer;
        final long offset = sequenceToOffset(sequence);

        final Object e = UNSAFE.getObject(buffer, offset);
        UNSAFE.putObject(buffer, offset, null);

        return (E)e;
    }
}
