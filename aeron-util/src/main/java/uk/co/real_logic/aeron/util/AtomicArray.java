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
package uk.co.real_logic.aeron.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A dynamic array that can be used concurrently with a single writer and many readers. All operations are lock-free.
 */
public class AtomicArray<T> implements Collection<T>
{
    private static final Object[] EMPTY_ARRAY = new Object[0];
    private final AtomicReference<Object[]> arrayRef = new AtomicReference<>(EMPTY_ARRAY);

    @FunctionalInterface
    public interface ToIntFunction<T>
    {
        /**
         * Applies this function to the given argument.
         *
         * @param value the function argument
         * @return a value to indicate the number of actions that have occurred.
         */
        int apply(T value);
    }

    @FunctionalInterface
    public interface MatchFunction<T>
    {
        /**
         * Does an element match the supplied match function?
         *
         * @param value to be checked for a match.
         * @return true if the value matches otherwise false.
         */
        boolean matches(T value);
    }

    /**
     * Return the given element of the array
     *
     * @param index of the element to return
     * @return the element
     */
    @SuppressWarnings("unchecked")
    public T get(final int index)
    {
        return (T)arrayRef.get()[index];
    }

    /**
     * Find the first element that matches via a supplied {@link MatchFunction} function.
     *
     * @param function to match on.
     * @return the first element to match or null if no matches.
     */
    public T findFirst(final MatchFunction<T> function)
    {
        @SuppressWarnings("unchecked")
        final T[] array = (T[])arrayRef.get();

        for (final T e : array)
        {
            if (function.matches(e))
            {
                return e;
            }
        }

        return null;
    }

    /**
     * Iterate over each element applying a supplied function.
     *
     * @param function to be applied to each element.
     */
    public void forEach(final Consumer<? super T> function)
    {
        forEachFrom(
            0,
            (t) ->
            {
                function.accept(t);
                return 0;
            });
    }

    /**
     * For each valid element, call a function passing the element
     *
     * @param fromIndex    the index to fromIndex iterating at
     * @param function to call and pass each element to
     * @return true if side effects have occurred otherwise false.
     */
    public int forEachFrom(int fromIndex, final ToIntFunction<? super T> function)
    {
        @SuppressWarnings("unchecked")
        final T[] array = (T[])arrayRef.get();

        return forEachFrom(fromIndex, function, array);
    }

    private int forEachFrom(int fromIndex, final ToIntFunction<? super T> function, final T[] array)
    {
        if (array.length == 0)
        {
            return 0;
        }

        if (array.length <= fromIndex)
        {
            fromIndex = array.length - 1;
        }

        int actionsCount = 0;
        int i = fromIndex;
        do
        {
            actionsCount += function.apply(array[i]);

            if (++i == array.length)
            {
                i = 0;
            }
        }
        while (i != fromIndex);

        return actionsCount;
    }

    /**
     * Add given element to the array atomically.
     *
     * @param element to add
     */
    public boolean add(final T element)
    {
        if (null == element)
        {
            throw new NullPointerException("element cannot be null");
        }

        final Object[] oldArray = arrayRef.get();
        final Object[] newArray = append(oldArray, element);

        arrayRef.lazySet(newArray);

        return true;
    }

    /**
     * Remove given element from the array atomically.
     *
     * @param element to remove
     */
    public boolean remove(final Object element)
    {
        final Object[] oldArray = arrayRef.get();
        final Object[] newArray = remove(oldArray, element);

        arrayRef.lazySet(newArray);

        return oldArray != newArray;
    }


    public int size()
    {
        return arrayRef.get().length;
    }

    public Iterator<T> iterator()
    {
        return new ArrayIterator<>(arrayRef.get());
    }

    private final static class ArrayIterator<T> implements Iterator<T>
    {
        private final Object[] array;
        private int index;

        private ArrayIterator(final Object[] array)
        {
            this.array = array;
        }

        public boolean hasNext()
        {
            return index < array.length;
        }

        @SuppressWarnings("unchecked")
        public T next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }

            return (T)array[index++];
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public boolean isEmpty()
    {
        return arrayRef.get().length == 0;
    }

    public boolean contains(final Object o)
    {
        return -1 != find(arrayRef.get(), o);
    }

    public Object[] toArray()
    {
        final Object[] theArray = arrayRef.get();

        return Arrays.copyOf(theArray, theArray.length);
    }

    public <T> T[] toArray(final T[] a)
    {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(final Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(final Collection<? extends T> c)
    {
        for (final T item : c)
        {
            add(item);
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
        arrayRef.set(EMPTY_ARRAY);
    }

    public String toString()
    {
        return "AtomicArray{" +
            "arrayRef=" + Arrays.toString(arrayRef.get()) +
            '}';
    }

    private static int find(final Object[] array, final Object item)
    {
        for (int i = 0; i < array.length; i++)
        {
            if (item.equals(array[i]))
            {
                return i;
            }
        }

        return -1;
    }

    private static Object[] append(final Object[] oldArray, final Object newElement)
    {
        final Object[] newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
        newArray[oldArray.length] = newElement;

        return newArray;
    }

    private static Object[] remove(final Object[] oldArray, final Object oldElement)
    {
        if (null == oldArray || (oldArray.length == 1 && oldArray[0].equals(oldElement)))
        {
            return EMPTY_ARRAY;
        }

        int index = find(oldArray, oldElement);

        if (-1 == index)
        {
            return oldArray;
        }

        final Object[] newArray = new Object[oldArray.length - 1];
        System.arraycopy(oldArray, 0, newArray, 0, index);
        System.arraycopy(oldArray, index + 1, newArray, index, newArray.length - index);

        return newArray;
    }
}
