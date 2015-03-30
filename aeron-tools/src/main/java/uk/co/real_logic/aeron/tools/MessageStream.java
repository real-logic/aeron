package uk.co.real_logic.aeron.tools;

import java.io.InputStream;
import java.util.zip.CRC32;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class MessageStream
{
    /* Message header offsets for verifiable message headers. */
    private static final int MAGIC_OFFSET = 0;
    private static final int MESSAGE_CHECKSUM_OFFSET = 4;
    private static final int SEQUENCE_NUMBER_OFFSET = 8;
    private final int message_offset; /* Either 0 or 16, depending on if a verifiable message header is used. */

    private static final int HEADER_LENGTH = 16;

    private static final int MAGIC = 0x0dd01221; /* That's "2112d00d" when taken a byte at a time... */
    private static final int MAGIC_END = 0xbeba1221; /* That's "2112babe" when taken a byte at a time... */

    private final int minSize;
    private final int maxSize;
    private final boolean verifiable;

    private InputStream inputStream;
    private byte[] inputStreamBytes;

    private long sequenceNumber = 0;
    private long messageCount = 0;
    private boolean active = true;

    private UnsafeBuffer copybuf = new UnsafeBuffer(new byte[1]);

    private static final ThreadLocalCRC32 MSG_CHECKSUM = new ThreadLocalCRC32();

    private static class ThreadLocalCRC32 extends ThreadLocal<CRC32>
    {
        @Override protected CRC32 initialValue()
        {
            return new CRC32();
        }
    }

    public MessageStream(int size) throws Exception
    {
        this(size, size, true, null);
    }

    public MessageStream(int minSize, int maxSize) throws Exception
    {
        this(minSize, maxSize, true, null);
    }

    public MessageStream(int size, InputStream inputStream) throws Exception
    {
        this(size, size, true, inputStream);
    }

    public MessageStream(int size, boolean verifiable, InputStream inputStream) throws Exception
    {
        this(size, size, verifiable, inputStream);
    }

    public MessageStream(int minSize, int maxSize, boolean verifiable) throws Exception
    {
        this(minSize, maxSize, verifiable, null);
    }

    public MessageStream(int minSize, int maxSize, boolean verifiable, InputStream inputStream) throws Exception
    {
        this.inputStream = inputStream;
        if (this.inputStream == null)
        {
            // When no input stream is supplied, use random generator.
            this.inputStream = new RandomInputStream();
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

        this.verifiable = verifiable;
        if (this.verifiable)
        {
            this.minSize = minSize + HEADER_LENGTH;
            this.maxSize = maxSize + HEADER_LENGTH;
            this.message_offset = 16;
        }
        else
        {
            this.minSize = minSize;
            this.maxSize = maxSize;
            this.message_offset = 0;
        }

        if (this.minSize > this.maxSize)
        {
            throw new Exception("MessageStream maximum size must be greater than or equal to minimum size.");
        }
    }

    /** Constructor for the subscribing side. */
    public MessageStream()
    {
        this.minSize = 0;
        this.maxSize = 0;
        this.message_offset = HEADER_LENGTH;
        this.verifiable = true;
        this.inputStream = null;
    }

    public int payloadOffset(DirectBuffer buffer, int offset)
    {
        if (isVerifiable(buffer, offset))
        {
            return HEADER_LENGTH;
        }
        return 0;
    }

    public void putNext(DirectBuffer buffer, int offset, int length) throws Exception
    {
        if (!active)
        {
            throw new Exception("Stream has ended.");
        }

        copybuf.wrap(buffer, offset, length);
        /* Assume we've already checked and it appears we have a verifiable message. */

        /* Sequence number check first. */
        final long receivedSequenceNumber = copybuf.getLong(SEQUENCE_NUMBER_OFFSET);
        if (receivedSequenceNumber != sequenceNumber)
        {
            Exception e = new Exception("Verifiable message stream received sequence number " +
                    receivedSequenceNumber + ", but was expecting " +
                    sequenceNumber + ". Possibly missed " +
                    (receivedSequenceNumber - sequenceNumber) +
                    " messages.");
            /* Update expected SQN for next time. */
            sequenceNumber = receivedSequenceNumber + 1;
            throw e;
        }

        /* Update expected SQN for next time. */
        sequenceNumber++;

        /* Save the checksum first, then blank it out. */
        int msgCksum = copybuf.getInt(MESSAGE_CHECKSUM_OFFSET);

        copybuf.putInt(MESSAGE_CHECKSUM_OFFSET, 0);

        CRC32 crc = MSG_CHECKSUM.get();
        crc.reset();
        for (int i = 0; i < length; i++)
        {
            crc.update(copybuf.getByte(i));
        }

        /* Put originally received checksum back in place. */
        copybuf.putInt(MESSAGE_CHECKSUM_OFFSET, msgCksum);

        if ((int)(crc.getValue()) != msgCksum)
        {
            throw new Exception("Verifiable message per-message checksum invalid; received " +
                    msgCksum + " but calculated " + (int)(crc.getValue()));
        }

        messageCount++;
        /* Look for an end marker. */
        if (copybuf.getInt(MAGIC_OFFSET) == MAGIC_END)
        {
            active = false;
        }
    }

    public void reset(InputStream inputStream)
    {
        reset();
        this.inputStream = inputStream;
    }

    public void reset()
    {
        /* Reset stream checksum and set things back to active, 0 messages, etc. */
        active = true;
        messageCount = 0;
        sequenceNumber = 0;
    }

    public boolean isActive()
    {
        return active;
    }

    public long getMessageCount()
    {
        return messageCount;
    }

    public long getSequenceNumber()
    {
        return sequenceNumber;
    }

    /** Returns true if the buffer is _probably_ a verifiable message, false otherwise. */
    public static boolean isVerifiable(DirectBuffer buffer, int offset)
    {
        if ((buffer.capacity() - offset) < HEADER_LENGTH)
        {
            return false;
        }
        final int magic = buffer.getInt(offset);
        if ((magic == MAGIC) || (magic == MAGIC_END))
        {
            return true;
        }
        return false;
    }

    private static final int HEX_PRINT_WIDTH = 16;

    static void printHex(DirectBuffer buffer, int length)
    {
        printHex(buffer, 0, length);
    }

    static void printHex(UnsafeBuffer buffer, int length)
    {
        printHex(buffer, 0, length);
    }

    static void printHex(UnsafeBuffer buffer, int offset, int length)
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

    public static void printHex(DirectBuffer buffer, int offset, int length)
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

    /** Returns total number of bytes written.
     * @throws Exception */
    public int getNext(UnsafeBuffer buffer) throws Exception
    {
        if (buffer.capacity() < maxSize)
        {
            throw new Exception("Buffer capacity must be at least " + maxSize + " bytes.");
        }

        int size = TLRandom.current().nextInt(maxSize - minSize + 1) + minSize;
        return getNext(buffer, size);
    }

    /* This method exists only because of the idiotic 100-line method limit
     * in checkstyle.  Otherwise it would've all gone in to the top of getNext. */
    private void checkConstraints(UnsafeBuffer buffer, int size) throws Exception
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

    /** Generates a message of the desired size (size must be at least 12 bytes for
     * verifiable message headers if verifiable messages are on).
     * @throws Exception */
    public int getNext(UnsafeBuffer buffer, int size) throws Exception
    {
        checkConstraints(buffer, size);

        int pos;

        /* If checksums are on, begin with a message header. */
        if (verifiable)
        {
            buffer.putInt(MAGIC_OFFSET, MAGIC);
            buffer.putInt(MESSAGE_CHECKSUM_OFFSET, 0); /* Initially, checksum is set to 0. */
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            sequenceNumber++;
            pos = message_offset;
        }
        else
        {
            pos = 0;
        }

        int lenleft = size - pos;

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
            pos += sizeRead;
        }
        else
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
            CRC32 msgCksum = MSG_CHECKSUM.get();
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
