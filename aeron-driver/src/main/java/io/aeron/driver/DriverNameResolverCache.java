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
package io.aeron.driver;

import io.aeron.protocol.ResolutionEntryFlyweight;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.ArrayList;
import java.util.Arrays;

final class DriverNameResolverCache implements AutoCloseable
{
    private static final int INVALID_INDEX = -1;

    private final ArrayList<CacheEntry> entries = new ArrayList<>();
    private final long timeoutMs;

    DriverNameResolverCache(final long timeoutMs)
    {
        this.timeoutMs = timeoutMs;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
    }

    CacheEntry lookup(final String name, final byte type)
    {
        final int index = findEntryIndexByNameAndType(name, type);

        return INVALID_INDEX == index ? null : entries.get(index);
    }

    void addOrUpdateEntry(
        final byte[] name,
        final int nameLength,
        final long nowMs,
        final byte type,
        final byte[] address,
        final int port,
        final AtomicCounter cacheEntriesCounter)
    {
        final int existingEntryIndex = findEntryIndexByNameAndType(name, nameLength, type);
        final int addressLength = ResolutionEntryFlyweight.addressLength(type);
        final CacheEntry entry;

        if (INVALID_INDEX == existingEntryIndex)
        {
            entry = new CacheEntry(
                Arrays.copyOf(name, nameLength),
                type,
                nowMs,
                nowMs + timeoutMs,
                Arrays.copyOf(address, addressLength),
                port);
            entries.add(entry);
            cacheEntriesCounter.setRelease(entries.size());
        }
        else
        {
            entry = entries.get(existingEntryIndex);
            entry.timeOfLastActivityMs = nowMs;
            entry.deadlineMs = nowMs + timeoutMs;

            if (port != entry.port || !byteSubsetEquals(address, entry.address, addressLength))
            {
                entry.address = Arrays.copyOf(address, addressLength);
                entry.port = port;
            }
        }
    }

    int timeoutOldEntries(final long nowMs, final AtomicCounter cacheEntriesCounter)
    {
        int workCount = 0;

        final ArrayList<CacheEntry> listOfEntries = this.entries;
        for (int lastIndex = listOfEntries.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final CacheEntry entry = listOfEntries.get(i);

            if (entry.deadlineMs - nowMs < 0)
            {
                ArrayListUtil.fastUnorderedRemove(listOfEntries, i, lastIndex--);
                cacheEntriesCounter.setRelease(listOfEntries.size());
                workCount++;
            }
        }

        return workCount;
    }

    Iterator resetIterator()
    {
        iterator.cache = this;
        iterator.index = -1;

        return iterator;
    }

    static final class Iterator
    {
        int index = -1;
        DriverNameResolverCache cache;

        boolean hasNext()
        {
            return (index + 1) < cache.entries.size();
        }

        CacheEntry next()
        {
            return cache.entries.get(++index);
        }

        void rewindNext()
        {
            --index;
        }
    }

    private final Iterator iterator = new Iterator();

    static boolean byteSubsetEquals(final byte[] lhs, final byte[] rhs, final int length)
    {
        if (lhs.length < length || rhs.length < length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (lhs[i] != rhs[i])
            {
                return false;
            }
        }

        return true;
    }

    static boolean byteSubsetEquals(final byte[] lhs, final String rhs)
    {
        final int length = rhs.length();

        if (lhs.length < length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (lhs[i] != rhs.charAt(i))
            {
                return false;
            }
        }

        return true;
    }

    private int findEntryIndexByNameAndType(final byte[] name, final int nameLength, final byte type)
    {
        for (int i = 0; i < entries.size(); i++)
        {
            final CacheEntry entry = entries.get(i);

            if (type == entry.type && byteSubsetEquals(entry.name, name, nameLength))
            {
                return i;
            }
        }

        return INVALID_INDEX;
    }

    private int findEntryIndexByNameAndType(final String name, final byte type)
    {
        for (int i = 0; i < entries.size(); i++)
        {
            final CacheEntry entry = entries.get(i);

            if (type == entry.type && byteSubsetEquals(entry.name, name))
            {
                return i;
            }
        }

        return INVALID_INDEX;
    }

    static final class CacheEntry
    {
        long deadlineMs;
        long timeOfLastActivityMs;
        int port;
        final byte type;
        final byte[] name;
        byte[] address;

        CacheEntry(
            final byte[] name,
            final byte type,
            final long timeOfLastActivityMs,
            final long deadlineMs,
            final byte[] address,
            final int port)
        {
            this.name = name;
            this.type = type;
            this.timeOfLastActivityMs = timeOfLastActivityMs;
            this.deadlineMs = deadlineMs;
            this.address = address;
            this.port = port;
        }
    }
}
