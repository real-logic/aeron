/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import java.util.ArrayList;
import java.util.List;

/**
 * Create a pattern for determining message payload size. Use the constructors to start the pattern,
 * then use the addPatternEntry overloads to add more if necessary.
 * #getNext() will return the next size in the pattern.
 * Not thread safe, use the copy constructor to duplicate the pattern for other threads.
 */
public class MessageSizePattern
{
    /**
     * Immutable object for holding a number of messages and a size range each message could be.
     */
    final class MessageSizeEntry
    {
        final long count;
        final int minSize;
        final int maxSize;

        MessageSizeEntry(final long count, final int minSize, final int maxSize)
        {
            this.count = count;
            this.minSize = minSize;
            this.maxSize = maxSize;
        }
    }

    private int currentIndex = 0;
    private long messageCount = 0;
    /* minimum size starts at max value so it can be set lower */
    private int patternMinSize = Integer.MAX_VALUE;
    /* maximum size starts at min value so it can be set higher */
    private int patternMaxSize = 0;
    private final List<MessageSizeEntry> entries = new ArrayList<MessageSizeEntry>();

    /**
     * Instantiate a MessageSizePattern and always return the given message size.
     *
     * @param messageSize
     * @throws Exception when message size is invalid
     */
    public MessageSizePattern(final int messageSize) throws Exception
    {
        // technically not "always" when using max long value, but close enough.
        this(Long.MAX_VALUE, messageSize, messageSize);
    }

    /**
     * Instantiate a MessageSizePattern with a number of messages and their size.
     *
     * @param messageCount
     * @param messageSize
     */
    public MessageSizePattern(final long messageCount, final int messageSize) throws Exception
    {
        this(messageCount, messageSize, messageSize);
    }

    /**
     * Instantiate a MessageCount with a number of message, and random size range.
     *
     * @param messageCount
     * @param minSize
     * @param maxSize
     */
    public MessageSizePattern(final long messageCount, final int minSize, final int maxSize) throws Exception
    {
        this.messageCount = 0;
        this.currentIndex = 0;
        addPatternEntry(messageCount, minSize, maxSize);
    }

    /**
     * Create a copy of the given pattern with a different random number generator.
     *
     * @param original The instance to copy.
     */
    public MessageSizePattern(final MessageSizePattern original)
    {
        this.messageCount = 0;
        this.currentIndex = 0;
        this.patternMinSize = original.patternMinSize;
        this.patternMaxSize = original.patternMaxSize;
        entries.addAll(original.entries);
    }

    /**
     * Add a number of messages with the given size to the pattern.
     *
     * @param messageCount
     * @param size
     * @throws Exception
     */
    public void addPatternEntry(final long messageCount, final int size) throws Exception
    {
        addPatternEntry(messageCount, size, size);
    }

    /**
     * Add a number of messages with a given size range to the pattern.
     *
     * @param messages Number of messages to send for this entry, must be at least 1.
     * @param minSize  The minimum size for a range
     * @param maxSize  The maximum size for a range
     * @throws Exception When input values are invalid.
     */
    public void addPatternEntry(final long messages, final int minSize, final int maxSize) throws Exception
    {
        if (messages < 1 || minSize < 0 || maxSize < 0)
        {
            throw new Exception("Negative values or zero messages are not allowed when adding an entry.");
        }
        if (minSize > maxSize)
        {
            throw new Exception("minSize can't be larger than maxSize when adding an entry.");
        }
        if (minSize < patternMinSize)
        {
            patternMinSize = minSize;
        }
        if (maxSize > patternMaxSize)
        {
            patternMaxSize = maxSize;
        }
        final MessageSizeEntry entry = new MessageSizeEntry(messages, minSize, maxSize);
        entries.add(entry);
    }

    /**
     * Reset the message size pattern back to the beginning.
     */
    public void reset()
    {
        this.messageCount = 0;
        this.currentIndex = 0;
    }

    /**
     * Get the next message size based on the pattern.
     *
     * @return expected size of the next message.
     */
    public int getNext()
    {
        // check current entry to see if we need to get the next one
        // This needs to be here so minimum and maximum work
        if (messageCount >= entries.get(currentIndex).count)
        {
            messageCount = 0;
            // get the next entry or wrap
            currentIndex++;
            if (currentIndex == entries.size())
            {
                currentIndex = 0;
            }
        }
        messageCount++;

        int value;
        final MessageSizeEntry entry = entries.get(currentIndex);

        if (entry.minSize != entry.maxSize)
        {
            // Use thread local random number generator service.
            value = SeedableThreadLocalRandom.current().nextInt(entry.maxSize - entry.minSize + 1) + entry.minSize;
        }
        else
        {
            value = entry.minSize;
        }

        return value;
    }

    /**
     * Get the minimum value possible in the entire pattern.
     *
     * @return Minimum possible value
     */
    public int minimum()
    {
        return patternMinSize;
    }

    /**
     * Get the maximum value possible in the entire pattern.
     *
     * @return Maximum possible value
     */
    public int maximum()
    {
        return patternMaxSize;
    }

    /**
     * Get the range minimum value of the current message.
     *
     * @return
     */
    public int currentRangeMinimum()
    {
        return entries.get(currentIndex).minSize;
    }

    /**
     * Get the range maximum value of the current message.
     *
     * @return
     */
    public int currentRangeMaximum()
    {
        return entries.get(currentIndex).maxSize;
    }
}
