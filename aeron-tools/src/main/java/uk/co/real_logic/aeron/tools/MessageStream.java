package uk.co.real_logic.aeron.tools;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.zip.CRC32;

public class MessageStream
{

	/* Message header offsets for verifiable message headers. */
	private static final int MAGIC_OFFSET = 0;
	private static final int MESSAGE_CHECKSUM_OFFSET = 4;
	private static final int STREAM_CHECKSUM_OFFSET = 8;
	private final int message_offset; /* Either 0 or 12, depending on if a verifiable message header is used. */

	private static final int HEADER_LENGTH = 12;

	private static final int MAGIC = 0x0dd01221; /* That's "2112d00d" when taken a byte at a time... */

	private final int minSize;
	private final int maxSize;
	private final CRC32 stream_checksum;
	private final boolean verifiable;
	
	private long messageCount = 0;

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
		this(size, size, true);
	}

	public MessageStream(int minSize, int maxSize) throws Exception
	{
		this(minSize, maxSize, true);
	}

	public MessageStream(int minSize, int maxSize, boolean verifiable) throws Exception
	{
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

		this.verifiable = verifiable;
		if (this.verifiable)
		{
			this.minSize = minSize + HEADER_LENGTH;
			this.maxSize = maxSize + HEADER_LENGTH;
			this.stream_checksum = new CRC32();
			this.message_offset = 12;
		}
		else
		{
			this.minSize = minSize;
			this.maxSize = maxSize;
			this.stream_checksum = null;
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
		this.stream_checksum = new CRC32();
		this.verifiable = true;
	}
	
	public void putNext(DirectBuffer buffer, int offset, int length) throws Exception
	{
		UnsafeBuffer copybuf = new UnsafeBuffer(buffer, offset, length);
		/* Assume we've already checked and it appears we have a verifiable message. */
		
		/* Save the checksums first, then blank them out. */
		int msgCksum = copybuf.getInt(MESSAGE_CHECKSUM_OFFSET);
		int streamCksum = copybuf.getInt(STREAM_CHECKSUM_OFFSET);
		
		copybuf.putInt(MESSAGE_CHECKSUM_OFFSET, 0);
		copybuf.putInt(STREAM_CHECKSUM_OFFSET, 0);

		CRC32 crc = MSG_CHECKSUM.get();
		crc.reset();
		for (int i = 0; i < length; i++)
		{
			crc.update(copybuf.getByte(i));
			stream_checksum.update(copybuf.getByte(i));
		}
		if ((int)(crc.getValue()) != msgCksum)
		{
			/* Put originally received checksums back in place. */
			copybuf.putInt(MESSAGE_CHECKSUM_OFFSET, msgCksum);
			copybuf.putInt(STREAM_CHECKSUM_OFFSET, streamCksum);
			throw new Exception("Verifiable message per-message checksum invalid; received " + 
					msgCksum + " but calculated " + (int)(crc.getValue()));
		}
		
		if ((int)(stream_checksum.getValue()) != streamCksum)
		{
			/* Put originally received checksums back in place. */
			copybuf.putInt(MESSAGE_CHECKSUM_OFFSET, msgCksum);
			copybuf.putInt(STREAM_CHECKSUM_OFFSET, streamCksum);
			throw new Exception("Verifiable message stream checksum invalid; received " + 
					streamCksum + " but calculated " + (int)(stream_checksum.getValue()));
		}
		
		/* Put checksums back in place. */
		copybuf.putInt(MESSAGE_CHECKSUM_OFFSET, msgCksum);
		copybuf.putInt(STREAM_CHECKSUM_OFFSET, streamCksum);
		messageCount++;
	}
	
	public long getMessageCount()
	{
		return messageCount;
	}

	/** Returns true if the buffer is _probably_ a verifiable message, false otherwise. */
	public static boolean isVerifiable(DirectBuffer buffer, int offset)
	{
		if ((buffer.capacity() - offset) < HEADER_LENGTH)
		{
			return false;
		}
		if (buffer.getInt(offset) == MAGIC)
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

		int pos;

		/* If checksums are on, begin with a message header. */
		if (verifiable)
		{
			buffer.putInt(MAGIC_OFFSET, MAGIC);
			buffer.putLong(MESSAGE_CHECKSUM_OFFSET, 0); /* Initially, checksums are set to 0. */
			pos = message_offset;
		}
		else
		{
			pos = 0;
		}
		
		/* Use a random message length. */
		int lenleft = TLRandom.current().nextInt(maxSize - minSize + 1) + minSize - message_offset - pos;

		//System.out.println("lenleft is " + lenleft);
		
		/* Write random bytes until the end of the message. */
		while (lenleft >= 4)
		{
			buffer.putInt(pos, TLRandom.current().nextInt());
			pos += 4;
			lenleft -= 4;
		}
		while (lenleft > 0)
		{
			int lastnum = TLRandom.current().nextInt();
			buffer.putByte(pos, (byte)(lastnum >> (lenleft << 3)));
			lenleft--;
			pos++;
		}
		
		/* Now calculate rolling and then per-message checksums if verifiable messages are on. */
		if (verifiable)
		{
			CRC32 msgCksum = MSG_CHECKSUM.get();
			msgCksum.reset();
			for (int i = 0; i < pos; i++)
			{
				stream_checksum.update(buffer.getByte(i));
				msgCksum.update(buffer.getByte(i));
			}
						
			/* Write checksums into message. */
			buffer.putInt(STREAM_CHECKSUM_OFFSET, (int)(stream_checksum.getValue()));
			buffer.putInt(MESSAGE_CHECKSUM_OFFSET, (int)(msgCksum.getValue()));
		}

		//System.out.println("After checksums, returning pos " + pos + ":");
		//printHex(buffer, pos);
		messageCount++;
		return pos;
	}
}
