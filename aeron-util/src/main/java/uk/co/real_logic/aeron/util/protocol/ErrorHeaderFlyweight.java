/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util.protocol;

import java.nio.ByteOrder;

/**
 */
public class ErrorHeaderFlyweight extends HeaderFlyweight
{
    private static final int ERROR_CODE_FIELD_OFFSET = 1;
    private static final int OFFENDING_HDR_FRAME_LENGTH_FIELD_OFFSET = 8;
    private static final int OFFENDING_HDR_OFFSET = 12;

    /**
     * return error code field
     *
     * @return error code field
     */
    public short errorCode()
    {
        return uint8Get(offset + ERROR_CODE_FIELD_OFFSET);
    }

    /**
     * set error code field
     *
     * @param code field value
     * @return flyweight
     */
    public ErrorHeaderFlyweight errorCode(final short code)
    {
        uint8Put(offset + ERROR_CODE_FIELD_OFFSET, code);
        return this;
    }

    /**
     * return offending header frame length field
     *
     * @return offending header frame length field
     */
    public int offendingHeaderFrameLength()
    {
        return (int)uint32Get(offset + OFFENDING_HDR_FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set offending header frame length field
     *
     * @param length of offending header frame
     * @return flyweight
     */
    public ErrorHeaderFlyweight offendingHeaderFrameLength(final int length)
    {
        uint32Put(offset + OFFENDING_HDR_FRAME_LENGTH_FIELD_OFFSET, length, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    /**
     * Return offset in buffer for offending header
     *
     * @return offset of offending header in the buffer
     */
    public int offendingHeaderOffset()
    {
        return offset + OFFENDING_HDR_OFFSET;
    }

    /**
     * copy the offending header into this error header
     *
     * sets offending header frame length
     *
     * @param header to include as the offending header
     * @param maxLength of the offending header to include
     * @return flyweight
     */
    public ErrorHeaderFlyweight offendingHeader(final HeaderFlyweight header, final int maxLength)
    {
        final int len = Math.min(header.frameLength(), maxLength);

        offendingHeaderFrameLength(len);
        copyFlyweight(header, offendingHeaderOffset(), len);
        return this;
    }

    /**
     * return offset in buffer for error string
     *
     * Requires that offending header frame length field already be set
     *
     * @return offset of error string in the buffer
     */
    public int errorStringOffset()
    {
        return offendingHeaderOffset() + offendingHeaderFrameLength();
    }

    /**
     * copy the error string into the header
     *
     * Requires the offending header to have already been set
     *
     * @param errorString bytes to include
     * @return flyweight
     */
    public ErrorHeaderFlyweight errorString(final byte[] errorString)
    {
        atomicBuffer.putBytes(errorStringOffset(), errorString, 0, errorString.length);
        return this;
    }
}
