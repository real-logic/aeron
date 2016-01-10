/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.command;

import uk.co.real_logic.aeron.ErrorCode;
import uk.co.real_logic.aeron.Flyweight;
import uk.co.real_logic.agrona.BitUtil;

import java.nio.ByteOrder;

/**
 * Control message flyweight for any errors sent from driver to clients
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |              Offending Command Correlation ID                 |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                         Error Code                            |
 * +---------------------------------------------------------------+
 * |                   Error Message Length                        |
 * +---------------------------------------------------------------+
 * |                       Error Message                          ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 */
public class ErrorResponseFlyweight extends Flyweight
{
    private static final int OFFENDING_COMMAND_CORRELATION_ID_OFFSET = 0;
    private static final int ERROR_CODE_OFFSET = OFFENDING_COMMAND_CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int ERROR_MESSAGE_OFFSET = ERROR_CODE_OFFSET + BitUtil.SIZE_OF_INT;

    /**
     * Return correlation ID of the offending command.
     *
     * @return correlation ID of the offending command
     */
    public long offendingCommandCorrelationId()
    {
        return buffer().getLong(OFFENDING_COMMAND_CORRELATION_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set correlation ID of the offending command.
     *
     * @param correlationId of the offending command
     * @return flyweight
     */
    public ErrorResponseFlyweight offendingCommandCorrelationId(final long correlationId)
    {
        buffer().putLong(OFFENDING_COMMAND_CORRELATION_ID_OFFSET, correlationId, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Error code for the command.
     *
     * @return error code for the command
     */
    public ErrorCode errorCode()
    {
        return ErrorCode.get(buffer().getInt(ERROR_CODE_OFFSET, ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Set the error code for the command.
     *
     * @param code for the error
     * @return flyweight
     */
    public ErrorResponseFlyweight errorCode(final ErrorCode code)
    {
        buffer().putInt(ERROR_CODE_OFFSET, code.value(), ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Error message associated with the error.
     *
     * @return error message
     */
    public String errorMessage()
    {
        return buffer().getStringUtf8(ERROR_MESSAGE_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Set the error message
     *
     * @param message to associate with the error
     * @return flyweight
     */
    public ErrorResponseFlyweight errorMessage(final String message)
    {
        buffer().putStringUtf8(ERROR_MESSAGE_OFFSET, message, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Length of the error response in bytes.
     *
     * @return length of the error response
     */
    public int length()
    {
        return buffer().getInt(ERROR_MESSAGE_OFFSET, ByteOrder.LITTLE_ENDIAN) + ERROR_MESSAGE_OFFSET + BitUtil.SIZE_OF_INT;
    }
}
