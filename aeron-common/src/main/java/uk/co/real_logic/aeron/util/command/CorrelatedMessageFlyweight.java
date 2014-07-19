package uk.co.real_logic.aeron.util.command;

import uk.co.real_logic.aeron.util.Flyweight;

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_LONG;

/**
 *  * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Correlation ID                        |
 * |                                                               |
 * +---------------------------------------------------------------+
 */
public class CorrelatedMessageFlyweight extends Flyweight
{
    public static final int CORRELATION_ID_FIELD_OFFSET = 0;

    public static final int LENGTH = SIZE_OF_LONG;

    /**
     * return correlation id field
     *
     * @return correlation id field
     */
    public long correlationId()
    {
        return atomicBuffer().getLong(offset() + CORRELATION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set correlation id field
     *
     * @param correlationId field value
     * @return flyweight
     */
    public CorrelatedMessageFlyweight correlationId(final long correlationId)
    {
        atomicBuffer().putLong(offset() + CORRELATION_ID_FIELD_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}
