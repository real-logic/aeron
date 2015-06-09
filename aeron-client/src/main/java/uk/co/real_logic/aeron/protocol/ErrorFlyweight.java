/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.protocol;

import uk.co.real_logic.aeron.ErrorCode;
import uk.co.real_logic.aeron.Flyweight;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for Error codec.
 */
public class ErrorFlyweight extends HeaderFlyweight
{
    /** Length of the Error Header */
    public static final int HEADER_LENGTH = 12;

    private static final int ERROR_CODE_FIELD_OFFSET = 1;
    private static final int OFFENDING_HDR_FRAME_LENGTH_FIELD_OFFSET = 8;
    private static final int OFFENDING_HDR_OFFSET = 12;

    /**
     * The error code field
     *
     * @return error code field
     */
    public ErrorCode errorCode()
    {
        return ErrorCode.get(uint8Get(offset() + ERROR_CODE_FIELD_OFFSET));
    }

    /**
     * Set error code field
     *
     * @param code field value
     * @return flyweight
     */
    public ErrorFlyweight errorCode(final ErrorCode code)
    {
        uint8Put(offset() + ERROR_CODE_FIELD_OFFSET, code.value());

        return this;
    }

    /**
     * The offending header frame length field
     *
     * @return offending header frame length field
     */
    public int offendingHeaderFrameLength()
    {
        return buffer().getInt(offset() + OFFENDING_HDR_FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set offending header frame length field
     *
     * @param length of offending header frame
     * @return flyweight
     */
    public ErrorFlyweight offendingHeaderFrameLength(final int length)
    {
        buffer().putInt(offset() + OFFENDING_HDR_FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Return offset in buffer for offending header
     *
     * @return offset of offending header in the buffer
     */
    public int offendingHeaderOffset()
    {
        return offset() + OFFENDING_HDR_OFFSET;
    }

    /**
     * Copy the offending header into this error header
     *
     * sets offending header frame length
     *
     * @param header to include as the offending header
     * @param maxLength of the offending header to include
     * @return flyweight
     */
    public ErrorFlyweight offendingHeader(final HeaderFlyweight header, final int maxLength)
    {
        final int length = Math.min(header.frameLength(), maxLength);

        return offendingFlyweight(header, length);
    }

    /**
     * Copy the offending action from a flyweight into this error header. If you are using a HeaderFlyweight
     * then {@link #offendingHeader(HeaderFlyweight, int)}, this is for inter-thread messaging
     *
     * sets offending flyweight frame length
     *
     * @param offendingFlyweight to include as the offending flyweight
     * @param length of the offending flyweight to include
     * @return flyweight
     */
    public ErrorFlyweight offendingFlyweight(final Flyweight offendingFlyweight, final int length)
    {
        offendingHeaderFrameLength(length);
        copyFlyweight(offendingFlyweight, offendingHeaderOffset(), length);

        return this;
    }

    /**
     * The offset in buffer for the error message
     *
     * Requires that offending header frame length field already be set
     *
     * @return offset of error string in the buffer
     */
    public int errorMessageOffset()
    {
        return offendingHeaderOffset() + offendingHeaderFrameLength();
    }

    /**
     * Copy the error string into the header
     *
     * Requires the offending header to have already been set
     *
     * @param errorMessage bytes to include
     * @return this flyweight
     */
    public ErrorFlyweight errorMessage(final byte[] errorMessage)
    {
        buffer().putBytes(errorMessageOffset(), errorMessage, 0, errorMessage.length);

        return this;
    }

    /**
     * The length of the error string in the header in bytes
     *
     * @return length of error string in bytes
     */
    public int errorStringLength()
    {
        return frameLength() - offendingHeaderFrameLength() - ErrorFlyweight.HEADER_LENGTH;
    }

    /**
     * The error string as a byte array
     *
     * @return byte array representation of the error string
     */
    public byte[] errorMessageAsBytes()
    {
        final int len = errorStringLength();
        final byte[] bytes = new byte[len];

        buffer().getBytes(errorMessageOffset(), bytes, 0, len);

        return bytes;
    }

    /**
     * returns The error String.
     *
     * @return the error String
     */
    public String errorMessage()
    {
        return new String(errorMessageAsBytes());
    }
}
