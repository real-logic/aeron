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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.InputStream;
import java.util.zip.CRC32;

public class MessageStream
{
    /* Message header offsets for verifiable message headers. */
    private static final int MAGIC_OFFSET = 0;
    private static final int MESSAGE_CHECKSUM_OFFSET = 4;
    private static final int SEQUENCE_NUMBER_OFFSET = 8;
    private final int messageOffset; /* Either 0 or 16, depending on if a verifiable message header is used. */

    private static final int HEADER_LENGTH = 16;

    private static final int MAGIC = 0x0dd01221; /* That's "2112d00d" when taken a byte at a time... */
    private static final int MAGIC_END = 0xbeba1221; /* That's "2112babe" when taken a byte at a time... */

    private static final int HEX_PRINT_WIDTH = 16;

    private final int minSize;
    private final int maxSize;
    private final boolean verifiable;

    private InputStream inputStream;
    private final boolean inputStreamIsRandom;
    private byte[] inputStreamBytes;

    private long sequenceNumber = -1;
    private long messageCount = 0;
    private boolean active = true;

    private static final ThreadLocalCRC32 MSG_CHECKSUM = new ThreadLocalCRC32();

    private static class ThreadLocalCRC32 extends ThreadLocal<CRC32>
    {
        protected CRC32 initialValue()
        {
            return new CRC32();
        }
    }

    public MessageStream(final int size) throws Exception
    {
        this(size, size, true, null);
    }

    public MessageStream(final int minSize, final int maxSize) throws Exception
    {
        this(minSize, maxSize, true, null);
    }

    public MessageStream(final int size, final InputStream inputStream) throws Exception
    {
        this(size, size, true, inputStream);
    }

    public MessageStream(final int size, final boolean verifiable, final InputStream inputStream) throws Exception
    {
        this(size, size, verifiable, inputStream);
    }

    public MessageStream(final int minSize, final int maxSize, final boolean verifiable) throws Exception
    {
        this(minSize, maxSize, verifiable, null);
    }

    public MessageStream(final int minSize, final int maxSize, final boolean verifiable,
        final InputStream inputStream) throws Exception
    {
        if (inputStream == null)
        {
            // When no input stream is supplied, use random generator.
            this.inputStream = new RandomInputStream();
            this.inputStreamIsRandom = true;
        }
        else
        {
            this.inputStream = inputStream;
            this.inputStreamIsRandom = false;
        }
        if (minSize < 0)
        {
            throw new Exception("MessageStream minimum message size must be 0 or greater.");
        }
        if (maxSize < 0)
        {
            throw new Exception("MessageStream maximum message size must be 0 or greater.");
        }
        if (maxSize < minSize)
        {
            throw new Exception("MessageStream maximum size must be greater than or equal to minimum size.");
        }
        if (verifiable && (minSize < HEADER_LENGTH))
        {
            throw new Exception("MessageStream minimum size must be at least " +
                HEADER_LENGTH + " bytes when using verifiable messages.");
        }

        this.inputStreamBytes = new byte[maxSize];

        this.minSize = minSize;
        this.maxSize = maxSize;

        this.verifiable = verifiable;
        if (this.verifiable)
        {
            this.messageOffset = HEADER_LENGTH;
        }
        else
        {
            this.messageOffset = 0;
        }

        if (this.minSize > this.maxSize)
        {
            throw new Exception("MessageStream maximum size must be greater than or equal to minimum size.");
        }
    }

    /**
     * Constructor for the subscribing side.
     */
    public MessageStream()
    {
        this.minSize = 0;
        this.maxSize = 0;
        this.messageOffset = HEADER_LENGTH;
        this.verifiable = true;
        this.inputStream = null;
        this.inputStreamIsRandom = false;
    }

    public int payloadOffset(final DirectBuffer buffer, final int offset)
    {
        if (isVerifiable(buffer, offset))
        {
            return HEADER_LENGTH;
        }
        return 0;
    }

    public void putNext(final DirectBuffer buffer, final int offset, final int length) throws Exception
    {
        if (!active)
        {
            throw new Exception("Stream has ended.");
        }

        /* Assume we've already checked and it appears we have a verifiable message. */

        /* Sequence number check first. */
        final long receivedSequenceNumber = buffer.getLong(offset + SEQUENCE_NUMBER_OFFSET);
        final long expectedSequenceNumber = sequenceNumber + 1;
        if (receivedSequenceNumber != expectedSequenceNumber)
        {
            final Exception e = new Exception("Verifiable message stream received sequence number " +
                receivedSequenceNumber + ", but was expecting " +
                expectedSequenceNumber + ". Possibly missed " +
                (receivedSequenceNumber - expectedSequenceNumber) +
                " messages.");
            /* Update expected SQN for next time. */
            sequenceNumber = receivedSequenceNumber;
            throw e;
        }

        /* Update SQN for next time. */
        sequenceNumber++;

        /* Calculate the checksum - substituting 0's for the checksum
         * field itself - and then compare it to the received checksum
         * field. */
        final CRC32 crc = MSG_CHECKSUM.get();
        crc.reset();
        int i = offset;
        for (; i < (offset + MESSAGE_CHECKSUM_OFFSET); i++)
        {
            crc.update(buffer.getByte(i));
        }
        for (; i < (offset + SEQUENCE_NUMBER_OFFSET); i++)
        {
            crc.update(0);
        }
        for (; i < (offset + length); i++)
        {
            crc.update(buffer.getByte(i));
        }

        final int msgCksum = buffer.getInt(offset + MESSAGE_CHECKSUM_OFFSET);
        if ((int)(crc.getValue()) != msgCksum)
        {
            throw new Exception("Verifiable message per-message checksum invalid; received " +
                msgCksum + " but calculated " + (int)(crc.getValue()));
        }

        messageCount++;
        /* Look for an end marker. */
        if (buffer.getInt(offset + MAGIC_OFFSET) == MAGIC_END)
        {
            active = false;
        }
    }

    public void reset(final InputStream inputStream)
    {
        reset();
        this.inputStream = inputStream;
    }

    public void reset()
    {
        /* Reset stream checksum and set things back to active, 0 messages, etc. */
        active = true;
        messageCount = 0;
        sequenceNumber = -1;
    }

    /**
     * Returns whether the MessageStream is still active
     * (ie, has not ended if, for example, a file was being sent).
     *
     * @return true if the stream is still expecting to generate
     * or receive more messages, false otherwise
     */
    public boolean isActive()
    {
        return active;
    }

    /**
     * Returns the number of messages that have been either generated (if
     * this is a publisher-side MessageStream) or inserted (if this is a
     * subscriber-side MessageStream).
     *
     * @return the number of messages that have been either generated
     * from or inserted into the MessageStream
     */
    public long getMessageCount()
    {
        return messageCount;
    }

    /**
     * Gets the current sequence number of the MessageStream; for a
     * publisher-side MessageStream, this is the sequence number of
     * the last message that was generated.  For a subscriber-side
     * MessageStream, this is the sequence number of the last message
     * inserted.  If no messages have yet been generated or inserted,
     * this will return -1.
     *
     * @return the MessageStream's current sequence number, or -1 if no
     * messages have been generated or inserted
     */
    public long getSequenceNumber()
    {
        return sequenceNumber;
    }

    /**
     * Returns true if the buffer is _probably_ a verifiable message, false otherwise.
     * This method just looks for a magic word at the beginning of the message; random
     * data might happen to reproduce one of the magic words about 1 in 2 billion
     * times.
     *
     * @param buffer Buffer with a message that may or may not be a verifiable message
     * @param offset Offset within the buffer where the message starts
     * @return true if the message appears to be a verifiable message, false otherwise
     */
    public static boolean isVerifiable(final DirectBuffer buffer, final int offset)
    {
        if ((buffer.capacity() - offset) < HEADER_LENGTH)
        {
            return false;
        }
        final int magic = buffer.getInt(offset);

        return (magic == MAGIC) || (magic == MAGIC_END);
    }

    static void printHex(final DirectBuffer buffer, final int length)
    {
        printHex(buffer, 0, length);
    }

    static void printHex(final UnsafeBuffer buffer, final int length)
    {
        printHex(buffer, 0, length);
    }

    static void printHex(final UnsafeBuffer buffer, final int offset, final int length)
    {
        int pos = 0;
        for (int i = offset; i < (offset + length); i++)
        {
            System.out.printf("%02x ", buffer.getByte(i));
            if (++pos % HEX_PRINT_WIDTH == 0)
            {
                System.out.println();
            }
        }
        System.out.println();
    }

    public static void printHex(final DirectBuffer buffer, final int offset, final int length)
    {
        int pos = 0;
        for (int i = offset; i < (offset + length); i++)
        {
            System.out.printf("%02x ", buffer.getByte(i));
            if (++pos % HEX_PRINT_WIDTH == 0)
            {
                System.out.println();
            }
        }
        System.out.println();
    }

    /**
     * Generates a message of random (within the constraints the MessageStream was
     * created with) size and writes it into the given buffer. Returns the number
     * of bytes actually written to the buffer.
     *
     * @param buffer The buffer to write a message to.
     * @return number of bytes written
     * @throws Exception
     */
    public int getNext(final UnsafeBuffer buffer) throws Exception
    {
        if (buffer.capacity() < maxSize)
        {
            throw new Exception("Buffer capacity must be at least " + maxSize + " bytes.");
        }

        final int size = SeedableThreadLocalRandom.current().nextInt(maxSize - minSize + 1) + minSize;
        return getNext(buffer, size);
    }

    /* This method exists only because of the 100-line method limit
     * in checkstyle.  The checks here really belong, functionally,
     * at the top of getNext. */
    private void checkConstraints(final UnsafeBuffer buffer, final int size) throws Exception
    {
        if (!active)
        {
            throw new Exception("Stream has ended.");
        }
        if (size < 0)
        {
            throw new Exception("Size must be >= 0.");
        }
        if (verifiable)
        {
            if (size < HEADER_LENGTH)
            {
                throw new Exception("Size must be at least " + HEADER_LENGTH + " when verifiable messages are used.");
            }
        }
        if (buffer.capacity() < size)
        {
            throw new Exception("Buffer capacity must be at least " + size + " bytes.");
        }
    }

    /**
     * Generates a message of the desired size (size must be at least 16 bytes for
     * verifiable message headers if verifiable messages are on) and writes it
     * into the given buffer. Returns the number of bytes actually written to the buffer.
     *
     * @param buffer The buffer to write a message to.
     * @param size   The length of the message to write, in bytes
     * @return number of bytes written
     * @throws Exception
     */
    public int getNext(final UnsafeBuffer buffer, final int size) throws Exception
    {
        checkConstraints(buffer, size);

        int pos;

        /* If checksums are on, begin with a message header. */
        if (verifiable)
        {
            sequenceNumber++;
            buffer.putInt(MAGIC_OFFSET, MAGIC);
            buffer.putInt(MESSAGE_CHECKSUM_OFFSET, 0); /* Initially, checksum is set to 0. */
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            pos = messageOffset;
        }
        else
        {
            pos = 0;
        }

        final int lenleft = size - pos;

        /* Try to pull out "size" bytes from the InputStream.  If we
         * can't (stream ends, etc.), then just fill in what we got;
         * we'll return the size actually written. */
        if (inputStreamBytes.length < lenleft)
        {
            inputStreamBytes = new byte[lenleft];
        }
        /* Try to read some bytes.  Maybe we'll even get some! */
        final int sizeRead = inputStream.read(inputStreamBytes, 0, lenleft);
        if (sizeRead > 0)
        {
            /* Copy what was read. */
            buffer.putBytes(pos, inputStreamBytes, 0, sizeRead);

            /* Now... if our input stream is actually random bytes, and
             * we're _not_ supposed to be doing verifiable messages, then
             * make things just a touch less random by excluding any
             * message that just happens to start with a verifiable
             * message magic value. */
            if ((sizeRead >= 4) && inputStreamIsRandom && !verifiable)
            {
                while (isVerifiable(buffer, 0))
                {
                    buffer.putInt(MAGIC_OFFSET, SeedableThreadLocalRandom.current().nextInt());
                }
            }

            pos += sizeRead;
        }
        else if (sizeRead < 0)
        {
            /* I guess the inputStream is done.  So change the
             * message type to an end message (if using verifiable
             * messages) and mark this stream done. */
            if (verifiable)
            {
                buffer.putInt(MAGIC_OFFSET, MAGIC_END);
            }
            active = false;
        }

        /* Now calculate rolling and then per-message checksums if verifiable messages are on. */
        if (verifiable)
        {
            final CRC32 msgCksum = MSG_CHECKSUM.get();
            msgCksum.reset();
            for (int i = 0; i < pos; i++)
            {
                msgCksum.update(buffer.getByte(i));
            }

            /* Write checksum into message. */
            buffer.putInt(MESSAGE_CHECKSUM_OFFSET, (int)(msgCksum.getValue()));
        }

        messageCount++;

        return pos;
    }
}
