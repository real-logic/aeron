package io.aeron;

import java.util.ArrayList;

public abstract class ArrayListUtil
{
    private ArrayListUtil()
    {
    }

    /**
     * Removes element at i, but instead of copying all elements to the left, moves into the same slot the last
     * element. If i is the last element it is just removed. This avoids the copy costs, but spoils the list order.
     *
     * @param list      to be modified
     * @param i         removal index
     * @param lastIndex last element index
     * @param <T>       element type
     */
    public static <T> void fastUnorderedRemove(final ArrayList<T> list, final int i, final int lastIndex)
    {
        final T last = list.remove(lastIndex);
        if (i != lastIndex)
        {
            list.set(i, last);
        }
    }

    /**
     * Removes element at i, but instead of copying all elements to the left, moves into the same slot the last
     * element. If i is the last element it is just removed. This avoids the copy costs, but spoils the list order.
     *
     * @param list      to be modified
     * @param e         to be removed
     * @param <T>       element type
     * @return true if found and removed, false otherwise
     */
    public static <T> boolean fastUnorderedRemove(final ArrayList<T> list, final T e)
    {
        for (int i = 0, size = list.size(); i < size; i++)
        {
            if (e == list.get(i))
            {
                fastUnorderedRemove(list, i, size - 1);
                return true;
            }
        }
        return false;
    }
}
