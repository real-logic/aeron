package uk.co.real_logic.aeron.util;

import java.nio.ByteBuffer;

/**
 * Interface for common ByteBuffer operations between the client and the media driver.
 */
public interface BufferStrategy
{
    ByteBuffer lookupSenderTerm(final long sessionId, final long channelId, final long termId) throws Exception;
}
