/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.logbuffer;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.*;

/**
 * Layout description for log buffers which contains partitions of terms with associated term meta data,
 * plus ending with overall log meta data.
 * <pre>
 *  +----------------------------+
 *  |           Term 0           |
 *  +----------------------------+
 *  |           Term 1           |
 *  +----------------------------+
 *  |           Term 2           |
 *  +----------------------------+
 *  |        Log Meta Data       |
 *  +----------------------------+
 * </pre>
 */
public class LogBufferDescriptor
{
    /**
     * The number of partitions the log is divided into.
     */
    public static final int PARTITION_COUNT = 3;

    /**
     * Section index for which buffer contains the log meta data.
     */
    public static final int LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

    /**
     * Minimum buffer length for a log term
     */
    public static final int TERM_MIN_LENGTH = 64 * 1024;

    /**
     * Maximum buffer length for a log term
     */
    public static final int TERM_MAX_LENGTH = 1024 * 1024 * 1024;

    /**
     * Minimum page size
     */
    public static final int PAGE_MIN_SIZE = 4 * 1024;

    /**
     * Maximum page size
     */
    public static final int PAGE_MAX_SIZE = 1024 * 1024 * 1024;


    // *******************************
    // *** Log Meta Data Constants ***
    // *******************************

    /**
     * Offset within the meta data where the tail values are stored.
     */
    public static final int TERM_TAIL_COUNTERS_OFFSET;

    /**
     * Offset within the log meta data where the active partition index is stored.
     */
    public static final int LOG_ACTIVE_TERM_COUNT_OFFSET;

    /**
     * Offset within the log meta data where the position of the End of Stream is stored.
     */
    public static final int LOG_END_OF_STREAM_POSITION_OFFSET;

    /**
     * Offset within the log meta data where whether the log is connected or not is stored.
     */
    public static final int LOG_IS_CONNECTED_OFFSET;

    /**
     * Offset within the log meta data where the active term id is stored.
     */
    public static final int LOG_INITIAL_TERM_ID_OFFSET;

    /**
     * Offset within the log meta data which the length field for the frame header is stored.
     */
    public static final int LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET;

    /**
     * Offset within the log meta data which the MTU length is stored.
     */
    public static final int LOG_MTU_LENGTH_OFFSET;

    /**
     * Offset within the log meta data which the correlation id is stored.
     */
    public static final int LOG_CORRELATION_ID_OFFSET;

    /**
     * Offset within the log meta data which the term length is stored.
     */
    public static final int LOG_TERM_LENGTH_OFFSET;

    /**
     * Offset within the log meta data which the page size is stored.
     */
    public static final int LOG_PAGE_SIZE_OFFSET;

    /**
     * Offset at which the default frame headers begin.
     */
    public static final int LOG_DEFAULT_FRAME_HEADER_OFFSET;

    /**
     * Maximum length of a frame header.
     */
    public static final int LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = CACHE_LINE_LENGTH * 2;

    /**
     * Total length of the log meta data buffer in bytes.
     * <pre>
     *   0                   1                   2                   3
     *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                       Tail Counter 0                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                       Tail Counter 1                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                       Tail Counter 2                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                      Active Term Count                        |
     *  +---------------------------------------------------------------+
     *  |                     Cache Line Padding                       ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     *  |                    End of Stream Position                     |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                        Is Connected                           |
     *  +---------------------------------------------------------------+
     *  |                      Cache Line Padding                      ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     *  |                 Registration / Correlation ID                 |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                        Initial Term Id                        |
     *  +---------------------------------------------------------------+
     *  |                  Default Frame Header Length                  |
     *  +---------------------------------------------------------------+
     *  |                          MTU Length                           |
     *  +---------------------------------------------------------------+
     *  |                         Term Length                           |
     *  +---------------------------------------------------------------+
     *  |                          Page Size                            |
     *  +---------------------------------------------------------------+
     *  |                      Cache Line Padding                      ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     *  |                     Default Frame Header                     ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     * </pre>
     */
    public static final int LOG_META_DATA_LENGTH;

    static
    {
        int offset = 0;
        TERM_TAIL_COUNTERS_OFFSET = offset;

        offset += (SIZE_OF_LONG * PARTITION_COUNT);
        LOG_ACTIVE_TERM_COUNT_OFFSET = offset;

        offset = (CACHE_LINE_LENGTH * 2);
        LOG_END_OF_STREAM_POSITION_OFFSET = offset;
        LOG_IS_CONNECTED_OFFSET = LOG_END_OF_STREAM_POSITION_OFFSET + SIZE_OF_LONG;

        offset += (CACHE_LINE_LENGTH * 2);
        LOG_CORRELATION_ID_OFFSET = offset;
        LOG_INITIAL_TERM_ID_OFFSET = LOG_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
        LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = LOG_INITIAL_TERM_ID_OFFSET + SIZE_OF_INT;
        LOG_MTU_LENGTH_OFFSET = LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_TERM_LENGTH_OFFSET = LOG_MTU_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_PAGE_SIZE_OFFSET = LOG_TERM_LENGTH_OFFSET + SIZE_OF_INT;

        offset += CACHE_LINE_LENGTH;
        LOG_DEFAULT_FRAME_HEADER_OFFSET = offset;

        LOG_META_DATA_LENGTH = align(offset + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH, PAGE_MIN_SIZE);
    }

    /**
     * Check that term length is valid and alignment is valid.
     *
     * @param termLength to be checked.
     * @throws IllegalStateException if the length is not as expected.
     */
    public static void checkTermLength(final int termLength)
    {
        if (termLength < TERM_MIN_LENGTH)
        {
            throw new IllegalStateException(
                "Term length less than min length of " + TERM_MIN_LENGTH + ": length=" + termLength);
        }

        if (termLength > TERM_MAX_LENGTH)
        {
            throw new IllegalStateException(
                "Term length more than max length of " + TERM_MAX_LENGTH + ": length=" + termLength);
        }

        if (!BitUtil.isPowerOfTwo(termLength))
        {
            throw new IllegalStateException("Term length not a power of 2: length=" + termLength);
        }
    }

    /**
     * Check that page size is valid and alignment is valid.
     *
     * @param pageSize to be checked.
     * @throws IllegalStateException if the size is not as expected.
     */
    public static void checkPageSize(final int pageSize)
    {
        if (pageSize < PAGE_MIN_SIZE)
        {
            throw new IllegalStateException(
                "Page size less than min size of " + PAGE_MIN_SIZE + ": page size=" + pageSize);
        }

        if (pageSize > PAGE_MAX_SIZE)
        {
            throw new IllegalStateException(
                "Page size more than max size of " + PAGE_MAX_SIZE + ": page size=" + pageSize);
        }

        if (!BitUtil.isPowerOfTwo(pageSize))
        {
            throw new IllegalStateException("Page size not a power of 2: page size=" + pageSize);
        }
    }

    /**
     * Get the value of the initial Term id used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of the initial Term id used for this log.
     */
    public static int initialTermId(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getInt(LOG_INITIAL_TERM_ID_OFFSET);
    }

    /**
     * Set the initial term at which this log begins. Initial should be randomised so that stream does not get
     * reused accidentally.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param initialTermId     value to be set.
     */
    public static void initialTermId(final UnsafeBuffer logMetaDataBuffer, final int initialTermId)
    {
        logMetaDataBuffer.putInt(LOG_INITIAL_TERM_ID_OFFSET, initialTermId);
    }

    /**
     * Get the value of the MTU length used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of the MTU length used for this log.
     */
    public static int mtuLength(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getInt(LOG_MTU_LENGTH_OFFSET);
    }

    /**
     * Set the MTU length used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param mtuLength         value to be set.
     */
    public static void mtuLength(final UnsafeBuffer logMetaDataBuffer, final int mtuLength)
    {
        logMetaDataBuffer.putInt(LOG_MTU_LENGTH_OFFSET, mtuLength);
    }

    /**
     * Get the value of the Term Length used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of the term length used for this log.
     */
    public static int termLength(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getInt(LOG_TERM_LENGTH_OFFSET);
    }

    /**
     * Set the term length used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param termLength        value to be set.
     */
    public static void termLength(final UnsafeBuffer logMetaDataBuffer, final int termLength)
    {
        logMetaDataBuffer.putInt(LOG_TERM_LENGTH_OFFSET, termLength);
    }

    /**
     * Get the value of the page size used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of the page size used for this log.
     */
    public static int pageSize(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getInt(LOG_PAGE_SIZE_OFFSET);
    }

    /**
     * Set the page size used for this log.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param pageSize          value to be set.
     */
    public static void pageSize(final UnsafeBuffer logMetaDataBuffer, final int pageSize)
    {
        logMetaDataBuffer.putInt(LOG_PAGE_SIZE_OFFSET, pageSize);
    }

    /**
     * Get the value of the correlation ID for this log relating to the command which created it.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of the correlation ID used for this log.
     */
    public static long correlationId(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getLong(LOG_CORRELATION_ID_OFFSET);
    }

    /**
     * Set the correlation ID used for this log relating to the command which created it.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param id                value to be set.
     */
    public static void correlationId(final UnsafeBuffer logMetaDataBuffer, final long id)
    {
        logMetaDataBuffer.putLong(LOG_CORRELATION_ID_OFFSET, id);
    }

    /**
     * Get whether the log is considered connected or not by the driver.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return whether the log is considered connected or not by the driver.
     */
    public static boolean isConnected(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getIntVolatile(LOG_IS_CONNECTED_OFFSET) == 1;
    }

    /**
     * Set whether the log is considered connected or not by the driver.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param isConnected       or not
     */
    public static void isConnected(final UnsafeBuffer logMetaDataBuffer, final boolean isConnected)
    {
        logMetaDataBuffer.putIntOrdered(LOG_IS_CONNECTED_OFFSET, isConnected ? 1 : 0);
    }

    /**
     * Get the value of the end of stream position.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of end of stream position
     */
    public static long endOfStreamPosition(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getLongVolatile(LOG_END_OF_STREAM_POSITION_OFFSET);
    }

    /**
     * Set the value of the end of stream position.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param position          value of the end of stream position
     */
    public static void endOfStreamPosition(final UnsafeBuffer logMetaDataBuffer, final long position)
    {
        logMetaDataBuffer.putLongOrdered(LOG_END_OF_STREAM_POSITION_OFFSET, position);
    }

    /**
     * Get the value of the active term count used by the producer of this log. Consumers may have a different
     * active term count if they are running behind. The read is done with volatile semantics.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @return the value of the active term count used by the producer of this log.
     */
    public static int activeTermCount(final UnsafeBuffer logMetaDataBuffer)
    {
        return logMetaDataBuffer.getIntVolatile(LOG_ACTIVE_TERM_COUNT_OFFSET);
    }

    /**
     * Set the value of the current active term count for the producer using memory ordered semantics.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param termCount         value of the active term count used by the producer of this log.
     */
    public static void activeTermCountOrdered(final UnsafeBuffer logMetaDataBuffer, final int termCount)
    {
        logMetaDataBuffer.putIntOrdered(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
    }

    /**
     * Compare and set the value of the current active term count.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param expectedTermCount value of the active term count expected in the log
     * @param updateTermCount   value of the active term count to be updated in the log
     * @return true if successful otherwise false.
     */
    public static boolean casActiveTermCount(
        final UnsafeBuffer logMetaDataBuffer, final int expectedTermCount, final int updateTermCount)
    {
        return logMetaDataBuffer.compareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, expectedTermCount, updateTermCount);
    }

    /**
     * Set the value of the current active partition index for the producer.
     *
     * @param logMetaDataBuffer containing the meta data.
     * @param termCount         value of the active term count used by the producer of this log.
     */
    public static void activeTermCount(final UnsafeBuffer logMetaDataBuffer, final int termCount)
    {
        logMetaDataBuffer.putInt(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
    }

    /**
     * Rotate to the next partition in sequence for the term id.
     *
     * @param currentIndex partition index
     * @return the next partition index
     */
    public static int nextPartitionIndex(final int currentIndex)
    {
        return (currentIndex + 1) % PARTITION_COUNT;
    }

    /**
     * Determine the partition index to be used given the initial term and active term ids.
     *
     * @param initialTermId at which the log buffer usage began
     * @param activeTermId  that is in current usage
     * @return the index of which buffer should be used
     */
    public static int indexByTerm(final int initialTermId, final int activeTermId)
    {
        return (activeTermId - initialTermId) % PARTITION_COUNT;
    }

    /**
     * Determine the partition index based on number of terms that have passed.
     *
     * @param termCount for the number of terms that have passed.
     * @return the partition index for the term count.
     */
    public static int indexByTermCount(final long termCount)
    {
        return (int)(termCount % PARTITION_COUNT);
    }

    /**
     * Determine the partition index given a stream position.
     *
     * @param position            in the stream in bytes.
     * @param positionBitsToShift number of times to right shift the position for term count
     * @return the partition index for the position
     */
    public static int indexByPosition(final long position, final int positionBitsToShift)
    {
        return (int)((position >>> positionBitsToShift) % PARTITION_COUNT);
    }

    /**
     * Compute the current position in absolute number of bytes.
     *
     * @param activeTermId        active term id.
     * @param termOffset          in the term.
     * @param positionBitsToShift number of times to left shift the term count
     * @param initialTermId       the initial term id that this stream started on
     * @return the absolute position in bytes
     */
    public static long computePosition(
        final int activeTermId, final int termOffset, final int positionBitsToShift, final int initialTermId)
    {
        final long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

        return (termCount << positionBitsToShift) + termOffset;
    }

    /**
     * Compute the current position in absolute number of bytes for the beginning of a term.
     *
     * @param activeTermId        active term id.
     * @param positionBitsToShift number of times to left shift the term count
     * @param initialTermId       the initial term id that this stream started on
     * @return the absolute position in bytes
     */
    public static long computeTermBeginPosition(
        final int activeTermId, final int positionBitsToShift, final int initialTermId)
    {
        final long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

        return termCount << positionBitsToShift;
    }

    /**
     * Compute the term id from a position.
     *
     * @param position            to calculate from
     * @param positionBitsToShift number of times to right shift the position
     * @param initialTermId       the initial term id that this stream started on
     * @return the term id according to the position
     */
    public static int computeTermIdFromPosition(
        final long position, final int positionBitsToShift, final int initialTermId)
    {
        return ((int)(position >>> positionBitsToShift) + initialTermId);
    }

    /**
     * Compute the total length of a log file given the term length.
     *
     * Assumes {@link #TERM_MAX_LENGTH} is 1GB and that filePageSize is 1GB or less and a power of 2.
     *
     * @param termLength   on which to base the calculation.
     * @param filePageSize to use for log.
     * @return the total length of the log file.
     */
    public static long computeLogLength(final int termLength, final int filePageSize)
    {
        if (termLength < (1024 * 1024 * 1024))
        {
            return align((termLength * PARTITION_COUNT) + LOG_META_DATA_LENGTH, filePageSize);
        }

        return (PARTITION_COUNT * (long)termLength) + align(LOG_META_DATA_LENGTH, filePageSize);
    }

    /**
     * Store the default frame header to the log meta data buffer.
     *
     * @param logMetaDataBuffer into which the default headers should be stored.
     * @param defaultHeader     to be stored.
     * @throws IllegalArgumentException if the defaultHeader is larger than {@link #LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH}
     */
    public static void storeDefaultFrameHeader(final UnsafeBuffer logMetaDataBuffer, final DirectBuffer defaultHeader)
    {
        if (defaultHeader.capacity() != HEADER_LENGTH)
        {
            throw new IllegalArgumentException(
                "Default header length not equal to HEADER_LENGTH: length=" + defaultHeader.capacity());
        }

        logMetaDataBuffer.putInt(LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET, HEADER_LENGTH);
        logMetaDataBuffer.putBytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, defaultHeader, 0, HEADER_LENGTH);
    }

    /**
     * Get a wrapper around the default frame header from the log meta data.
     *
     * @param logMetaDataBuffer containing the raw bytes for the default frame header.
     * @return a buffer wrapping the raw bytes.
     */
    public static UnsafeBuffer defaultFrameHeader(final UnsafeBuffer logMetaDataBuffer)
    {
        return new UnsafeBuffer(logMetaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET, HEADER_LENGTH);
    }

    /**
     * Apply the default header for a message in a term.
     *
     * @param logMetaDataBuffer containing the default headers.
     * @param termBuffer        to which the default header should be applied.
     * @param termOffset        at which the default should be applied.
     */
    public static void applyDefaultHeader(
        final UnsafeBuffer logMetaDataBuffer, final UnsafeBuffer termBuffer, final int termOffset)
    {
        termBuffer.putBytes(termOffset, logMetaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET, HEADER_LENGTH);
    }

    /**
     * Rotate the log and update the tail counter for the new term.
     *
     * This method is safe for concurrent use.
     *
     * @param logMetaDataBuffer for the meta data.
     * @param currentTermCount  from which to rotate.
     * @param currentTermId     to be used in the default headers.
     * @return true if log was rotated.
     */
    public static boolean rotateLog(
        final UnsafeBuffer logMetaDataBuffer, final int currentTermCount, final int currentTermId)
    {
        final int nextTermId = currentTermId + 1;
        final int nextTermCount = currentTermCount + 1;
        final int nextIndex = indexByTermCount(nextTermCount);
        final int expectedTermId = nextTermId - PARTITION_COUNT;

        long rawTail;
        do
        {
            rawTail = rawTail(logMetaDataBuffer, nextIndex);
            if (expectedTermId != termId(rawTail))
            {
                break;
            }
        }
        while (!casRawTail(logMetaDataBuffer, nextIndex, rawTail, packTail(nextTermId, 0)));

        return casActiveTermCount(logMetaDataBuffer, currentTermCount, nextTermCount);
    }

    /**
     * Set the initial value for the termId in the upper bits of the tail counter.
     *
     * @param logMetaData    contain the tail counter.
     * @param partitionIndex to be initialised.
     * @param termId         to be set.
     */
    public static void initialiseTailWithTermId(
        final UnsafeBuffer logMetaData, final int partitionIndex, final int termId)
    {
        logMetaData.putLong(TERM_TAIL_COUNTERS_OFFSET + (partitionIndex * SIZE_OF_LONG), packTail(termId, 0));
    }

    /**
     * Get the termId from a packed raw tail value.
     *
     * @param rawTail containing the termId
     * @return the termId from a packed raw tail value.
     */
    public static int termId(final long rawTail)
    {
        return (int)(rawTail >> 32);
    }

    /**
     * Read the termOffset from a packed raw tail value.
     *
     * @param rawTail    containing the termOffset.
     * @param termLength that the offset cannot exceed.
     * @return the termOffset value.
     */
    public static int termOffset(final long rawTail, final long termLength)
    {
        final long tail = rawTail & 0xFFFF_FFFFL;

        return (int)Math.min(tail, termLength);
    }

    /**
     * The termOffset as a result of the append
     *
     * @param result into which the termOffset value has been packed.
     * @return the termOffset after the append
     */
    public static int termOffset(final long result)
    {
        return (int)result;
    }

    /**
     * Pack a termId and termOffset into a raw tail value.
     *
     * @param termId     to be packed.
     * @param termOffset to be packed.
     * @return the packed value.
     */
    public static long packTail(final int termId, final int termOffset)
    {
        return ((long)termId << 32) | (termOffset & 0xFFFF_FFFFL);
    }

    /**
     * Set the raw value of the tail for the given partition.
     *
     * @param logMetaDataBuffer containing the tail counters.
     * @param partitionIndex    for the tail counter.
     * @param rawTail           to be stored
     */
    public static void rawTail(
        final UnsafeBuffer logMetaDataBuffer, final int partitionIndex, final long rawTail)
    {
        logMetaDataBuffer.putLong(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex), rawTail);
    }

    /**
     * Get the raw value of the tail for the given partition.
     *
     * @param logMetaDataBuffer containing the tail counters.
     * @param partitionIndex    for the tail counter.
     * @return the raw value of the tail for the current active partition.
     */
    public static long rawTail(final UnsafeBuffer logMetaDataBuffer, final int partitionIndex)
    {
        return logMetaDataBuffer.getLong(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex));
    }

    /**
     * Set the raw value of the tail for the given partition.
     *
     * @param logMetaDataBuffer containing the tail counters.
     * @param partitionIndex    for the tail counter.
     * @param rawTail           to be stored
     */
    public static void rawTailVolatile(
        final UnsafeBuffer logMetaDataBuffer, final int partitionIndex, final long rawTail)
    {
        logMetaDataBuffer.putLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex), rawTail);
    }

    /**
     * Get the raw value of the tail for the given partition.
     *
     * @param logMetaDataBuffer containing the tail counters.
     * @param partitionIndex    for the tail counter.
     * @return the raw value of the tail for the current active partition.
     */
    public static long rawTailVolatile(final UnsafeBuffer logMetaDataBuffer, final int partitionIndex)
    {
        return logMetaDataBuffer.getLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex));
    }

    /**
     * Get the raw value of the tail for the current active partition.
     *
     * @param logMetaDataBuffer containing the tail counters.
     * @return the raw value of the tail for the current active partition.
     */
    public static long rawTailVolatile(final UnsafeBuffer logMetaDataBuffer)
    {
        final int partitionIndex = indexByTermCount(activeTermCount(logMetaDataBuffer));
        return logMetaDataBuffer.getLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex));
    }

    /**
     * Compare and set the raw value of the tail for the given partition.
     *
     * @param logMetaDataBuffer containing the tail counters.
     * @param partitionIndex    for the tail counter.
     * @param expectedRawTail   expected current value.
     * @param updateRawTail     to be applied.
     * @return true if the update was successful otherwise false.
     */
    public static boolean casRawTail(
        final UnsafeBuffer logMetaDataBuffer,
        final int partitionIndex,
        final long expectedRawTail,
        final long updateRawTail)
    {
        final int index = TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex);
        return logMetaDataBuffer.compareAndSetLong(index, expectedRawTail, updateRawTail);
    }

    /**
     * Get the number of bits to shift when dividing or multiplying by the term buffer length.
     *
     * @param termBufferLength to compute the number of bits to shift for.
     * @return the number of bits to shift to divide or multiply by the term buffer length.
     */
    public static int positionBitsToShift(final int termBufferLength)
    {
        switch (termBufferLength)
        {
            case 64 * 1024: return 16;

            case 128 * 1024: return 17;

            case 256 * 1024: return 18;

            case 512 * 1024: return 19;

            case 1024 * 1024: return 20;

            case 2 * 1024 * 1024: return 21;

            case 4 * 1024 * 1024: return 22;

            case 8 * 1024 * 1024: return 23;

            case 16 * 1024 * 1024: return 24;

            case 32 * 1024 * 1024: return 25;

            case 64 * 1024 * 1024: return 26;

            case 128 * 1024 * 1024: return 27;

            case 256 * 1024 * 1024: return 28;

            case 512 * 1024 * 1024: return 29;

            case 1024 * 1024 * 1024: return 30;
        }

        throw new IllegalArgumentException("Invalid term buffer length: " + termBufferLength);
    }
}
