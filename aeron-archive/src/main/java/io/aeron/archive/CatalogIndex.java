/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import static java.util.Arrays.copyOf;

/**
 * {@code CatalogIndex} maps recording id to its position in the catalog file.
 */
final class CatalogIndex
{
    static final int DEFAULT_INDEX_SIZE = 10;

    static final long NULL_VALUE = -1;

    private long[] index;
    private int count;

    CatalogIndex()
    {
        index = new long[DEFAULT_INDEX_SIZE << 1];
    }

    /**
     * Add mapping between recording id and its file offset to the index.
     *
     * @param recordingId               to add.
     * @param recordingDescriptorOffset for the given id.
     * @throws IllegalArgumentException if {@code recordingId < 0 || recordingDescriptorOffset < 0}.
     * @throws IllegalArgumentException if {@code recordingId} is less than or equal to the last recording id added,
     *                                  i.e. {@code recordingId} must always increase.
     */
    void add(final long recordingId, final long recordingDescriptorOffset)
    {
        ensurePositive(recordingId, "recordingId");
        ensurePositive(recordingDescriptorOffset, "recordingDescriptorOffset");

        final int nextPosition = count << 1;
        long[] index = this.index;
        if (nextPosition > 0)
        {
            if (recordingId <= index[nextPosition - 2])
            {
                throw new IllegalArgumentException("recordingId " + recordingId +
                    " is less than or equal to the last recordingId " + index[nextPosition - 2]);
            }
            if (nextPosition == index.length)
            {
                index = expand(index);
                this.index = index;
            }
        }
        index[nextPosition] = recordingId;
        index[nextPosition + 1] = recordingDescriptorOffset;

        count++;
    }

    /**
     * Remove given recording id from the index.
     *
     * @param recordingId to remove.
     * @return recording file offset or {@link #NULL_VALUE} if not found.
     */
    long remove(final long recordingId)
    {
        ensurePositive(recordingId, "recordingId");

        final long[] index = this.index;
        final int lastPosition = lastPosition();
        final int position = find(index, recordingId, lastPosition);
        if (position < 0)
        {
            return NULL_VALUE;
        }

        final long recordingDescriptorOffset = index[position + 1];

        count--;

        // Shift data to the left
        for (int i = position; i < lastPosition; i += 2)
        {
            index[i] = index[i + 2];
            index[i + 1] = index[i + 3];
        }
        // Reset last copied element
        index[lastPosition] = 0;
        index[lastPosition + 1] = 0;

        return recordingDescriptorOffset;
    }

    /**
     * Get recording file offset by its id.
     *
     * @param recordingId to lookup.
     * @return recording file offset or {@link #NULL_VALUE} if not found.
     */
    long recordingOffset(final long recordingId)
    {
        ensurePositive(recordingId, "recordingId");

        final long[] index = this.index;
        final int lastPosition = lastPosition();
        final int position = find(index, recordingId, lastPosition);
        if (position < 0)
        {
            return NULL_VALUE;
        }

        return index[position + 1];
    }

    /**
     * Returns size of the index.
     *
     * @return number of the entries in the index.
     */
    int size()
    {
        return count;
    }

    /**
     * Position of the last inserted entry.
     *
     * @return position of the last entry or negative value if index is empty.
     */
    int lastPosition()
    {
        return (count << 1) - 2;
    }

    /**
     * Index array.
     *
     * @return index array.
     */
    long[] index()
    {
        return index;
    }

    static int find(final long[] index, final long recordingId, final int lastPosition)
    {
        if (lastPosition > 0 && recordingId >= index[0] && recordingId <= index[lastPosition])
        {
            int position = (int)((recordingId - index[0]) * lastPosition / (index[lastPosition] - index[0]));
            position = 0 == (position & 1) ? position : position + 1;

            if (recordingId == index[position])
            {
                return position;
            }
            else if (recordingId > index[position])
            {
                for (int i = position + 2; i <= lastPosition; i += 2)
                {
                    final long id = index[i];
                    if (recordingId == id)
                    {
                        return i;
                    }
                    else if (id > recordingId)
                    {
                        break;
                    }
                }
            }
            else
            {
                for (int i = position - 2; i >= 0; i -= 2)
                {
                    final long id = index[i];
                    if (recordingId == id)
                    {
                        return i;
                    }
                    else if (id < recordingId)
                    {
                        break;
                    }
                }
            }
        }
        else if (0 == lastPosition && recordingId == index[0])
        {
            return 0;
        }

        return -1;
    }

    private static long[] expand(final long[] index)
    {
        final int length = index.length;
        final int entries = length >> 1;
        final int newLength = (entries + (entries >> 1)) << 1;

        return copyOf(index, newLength);
    }

    private static void ensurePositive(final long value, final String name)
    {
        if (value < 0L)
        {
            throw new IllegalArgumentException(name + " cannot be negative: value=" + value);
        }
    }
}
