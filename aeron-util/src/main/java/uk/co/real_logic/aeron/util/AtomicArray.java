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
     * Return the length of the {@link Object} array
     *
     * @return the length of the {@link Object} array
     */
    public int length()
    {
        return arrayRef.get().length;
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
    public void forEach(final Consumer<T> function)
    {
        forEach(0, (t) -> { function.accept(t); return 0; });
    }

    /**
     * For each valid element, call a function passing the element
     *
     * @param start the index to start iterating at
     * @param func  to call and pass each element to
     * @return true if side effects have occurred otherwise false.
     */
    public int forEach(int start, final ToIntFunction<T> func)
    {
        @SuppressWarnings("unchecked")
        final T[] array = (T[])arrayRef.get();

        return forEach(start, func, array);
    }

    private int forEach(int start, final ToIntFunction<T> func, final T[] array)
    {
        if (array.length == 0)
        {
            return 0;
        }

        if (array.length <= start)
        {
            start = array.length - 1;
        }

        int actionsCount = 0;
        int i = start;
        do
        {
            final T element = array[i];
            if (null != element)
            {
                actionsCount += func.apply(element);
            }

            i++;

            if (i == array.length)
            {
                i = 0;
            }
        }
        while (i != start);

        return actionsCount;
    }

    /**
     * Add given element to the array atomically.
     *
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
     *
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

    public String toString()
    {
        return "AtomicArray{" +
            "arrayRef=" + Arrays.toString(arrayRef.get()) +
            '}';
    }
}
