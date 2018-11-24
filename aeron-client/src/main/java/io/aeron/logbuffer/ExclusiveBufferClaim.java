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

import java.nio.ByteOrder;

import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * {@link ExclusiveBufferClaim} use should be replaced with {@link BufferClaim}.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 * <p>
 * If the claimed space is no longer required it can be aborted by calling {@link #abort()}.
 * <p>
 * <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-frame">Data Frame</a>
 */
@Deprecated
public class ExclusiveBufferClaim extends BufferClaim
{
    /**
     * Write the provided value into the reserved space at the end of the data frame header.
     * <p>
     * Note: The value will be written in {@link ByteOrder#LITTLE_ENDIAN} format.
     *
     * @param value to be stored in the reserve space at the end of a data frame header.
     * @return this for fluent API semantics.
     * @see io.aeron.protocol.DataHeaderFlyweight
     */
    public ExclusiveBufferClaim reservedValue(final long value)
    {
        buffer.putLong(RESERVED_VALUE_OFFSET, value, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Set the value of the header flags field.
     *
     * @param flags value to be set in the header.
     * @return this for a fluent API.
     * @see io.aeron.protocol.DataHeaderFlyweight
     */
    public ExclusiveBufferClaim flags(final byte flags)
    {
        buffer.putByte(FLAGS_FIELD_OFFSET, flags);
        return this;
    }

    /**
     * Set the value of the header type field. The lower 16 bits are valid.
     *
     * @param type value to be set in the header.
     * @return this for a fluent API.
     * @see io.aeron.protocol.DataHeaderFlyweight
     */
    public ExclusiveBufferClaim headerType(final int type)
    {
        buffer.putShort(TYPE_FIELD_OFFSET, (short)type, LITTLE_ENDIAN);
        return this;
    }
}

