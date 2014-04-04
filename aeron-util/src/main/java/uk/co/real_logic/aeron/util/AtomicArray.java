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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Array with an atomic reference
 */
public class AtomicArray<T>
{
    private static final Object[] EMPTY_ARRAY = new Object[0];
    private final AtomicReference<Object[]> arrayRef = new AtomicReference<>(EMPTY_ARRAY);
    private Object[] lastMark = arrayRef.get();

    /**
     * Denotes whether the array has had elements appended or removed since the
     * last time mark was called.
     *
     * @see this#mark()
     * @return true if you've changed, false otherwise
     */
    public boolean changedSinceLastMark()
    {
        return lastMark != arrayRef.get();
    }

    /**
     * Marks a point at which to start checking changes from.
     *
     * @see this#changedSinceLastMark()
     */
    public void mark()
    {
        lastMark = arrayRef.get();
    }

    /**
     * Return the length of the {@link java.lang.Object} array
     * @return the length of the {@link java.lang.Object} array
     */
    public int length()
    {
        return arrayRef.get().length;
    }

    /**
     * Return the given element of the array
     * @param index of the element to return
     * @return the element
     */
    @SuppressWarnings("unchecked")
    public T get(final int index)
    {
        return (T)arrayRef.get()[index];
    }

    public void forEach(final Consumer<T> func)
    {
        forEach(0, func);
    }

    /**
     * For each valid element, call a function passing the element
     *
     * @param start the index to start iterating at
     * @param func to call and pass each element to
     */
    public void forEach(final int start, final Consumer<T> func)
    {
        @SuppressWarnings("unchecked")
        final T[] array = (T[])arrayRef.get();

        if (array.length == 0)
            return;

        int i = start;
        do
        {
            T element = array[i];
            if (null != element)
            {
                func.accept(element);
            }
            i = (i + 1) % array.length;
        } while (i != start);
    }

    /**
     * Add given element to the array atomically.
     * @param element to add
     */
    public void add(final T element)
    {
        final Object[] oldArray = arrayRef.get();
        final Object[] newArray = append(oldArray, element);

        arrayRef.lazySet(newArray);
    }

    /**
     * Remove given element from the array atomically.
     * @param element to remove
     */
    public void remove(final T element)
    {
        final Object[] oldArray = arrayRef.get();
        final Object[] newArray = remove(oldArray, element);

        arrayRef.lazySet(newArray);
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

        int index = -1;

        for (int i = 0; i < oldArray.length; i++)
        {
            if (oldArray[i].equals(oldElement))
            {
                index = i;
            }
        }

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
