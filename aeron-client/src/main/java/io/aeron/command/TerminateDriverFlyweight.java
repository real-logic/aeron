/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.command;

import org.agrona.DirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Command message flyweight to ask the driver process to terminate.
 *
 * @see ControlProtocolEvents
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Token Length                          |
 *  +---------------------------------------------------------------+
 *  |                         Token Buffer                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class TerminateDriverFlyweight extends CorrelatedMessageFlyweight
{
    private static final int TOKEN_LENGTH_OFFSET = CORRELATION_ID_FIELD_OFFSET + SIZE_OF_LONG;

    /**
     * Offset of the token buffer
     *
     * @return offset of the token buffer
     */
    public int tokenBufferOffset()
    {
        return TOKEN_LENGTH_OFFSET + SIZE_OF_INT;
    }

    /**
     * Length of the token buffer in bytes
     *
     * @return length of token buffer in bytes
     */
    public int tokenBufferLength()
    {
        return buffer.getInt(offset + TOKEN_LENGTH_OFFSET);
    }

    /**
     * Fill the token buffer.
     *
     * @param tokenBuffer containing the optional token for the request.
     * @param tokenOffset within the tokenBuffer at which the token begins.
     * @param tokenLength of the token in the tokenBuffer.
     * @return flyweight
     */
    public TerminateDriverFlyweight tokenBuffer(
        final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        buffer.putInt(TOKEN_LENGTH_OFFSET, tokenLength);
        if (null != tokenBuffer && tokenLength > 0)
        {
            buffer.putBytes(tokenBufferOffset(), tokenBuffer, tokenOffset, tokenLength);
        }

        return this;
    }

    /**
     * Get the length of the current message.
     * <p>
     * NB: must be called after the data is written in order to be accurate.
     *
     * @return the length of the current message
     */
    public int length()
    {
        return tokenBufferOffset() + buffer.getInt(offset + TOKEN_LENGTH_OFFSET);
    }
}
