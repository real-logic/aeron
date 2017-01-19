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
     * @param list to be modified
     * @param i removal index
     * @param lastIndex last element index
     * @param <T> element type
     */
    public static <T> void fastUnorderedRemove(ArrayList<T> list, int i, int lastIndex)
    {
        final T last = list.remove(lastIndex);
        if (i != lastIndex)
        {
            list.set(i, last);
        }
    }
}
