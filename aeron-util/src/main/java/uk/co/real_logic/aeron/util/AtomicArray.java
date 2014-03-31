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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Array with an atomic reference
 */
public class AtomicArray<T>
{
    private final AtomicReference<T[]> arrayRef = new AtomicReference<>();
    private final Class<T> clazz;

    public AtomicArray(final Class<T> clazz)
    {
        this.arrayRef.set(null);
        this.clazz = clazz;
    }

    /**
     * Return the underlying {@link java.util.concurrent.atomic.AtomicReference} to the array
     * @return the underlying array {@link java.util.concurrent.atomic.AtomicReference}
     */
    public AtomicReference<T[]> atomicReference()
    {
        return arrayRef;
    }

    /**
     * For each valid element, call a function passing the element
     * @param func to call and pass each element to
     */
    public void forEach(final Consumer<T> func)
    {
        final T[] array = arrayRef.get();

        if (null != array)
        {
            for (final T element : array)
            {
                if (null != element)
                {
                    func.accept(element);
                }
            }
        }
    }

    /**
     * Add given element to the array atomically.
     * @param element to add
     */
    public void add(final T element)
    {
        final T[] oldArray = arrayRef.get();
        final T[] newArray = addToEndOfArray(clazz, oldArray, element);

        arrayRef.lazySet(newArray);
    }

    /**
     * Remove given element from the array atomically.
     * @param element to remove
     */
    public void remove(final T element)
    {
        final T[] oldArray = arrayRef.get();
        final T[] newArray = removeFromArray(clazz, oldArray, element);

        arrayRef.lazySet(newArray);
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] addToEndOfArray(final Class<T> clazz, final T[] oldArray, final T newElement)
    {
        final T[] newArray;
        final int index;

        if (null == oldArray)
        {
            newArray = (T[]) Array.newInstance(clazz, 1);
            index = 0;
        }
        else
        {
            newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
            index = oldArray.length;
        }

        newArray[index] = newElement;
        return newArray;
    }

    @SuppressWarnings("unchecked")
    private static<T> T[] removeFromArray(final Class<T> clazz, final T[] oldArray, final T oldElement)
    {
        if (null == oldArray || (oldArray.length == 1 && oldArray[0].equals(oldElement)))
        {
            return null;
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

        final T[] newArray = (T[])Array.newInstance(clazz, oldArray.length - 1);
        System.arraycopy(oldArray, 0, newArray, 0, index);
        System.arraycopy(oldArray, index + 1, newArray, index, newArray.length - index);

        return newArray;
    }

}
