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
package uk.co.real_logic.aeron;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * Description of the command and control file used between driver and clients
 *
 * File Layout
 * <pre>
 *  +----------------------------+
 *  |      Aeron CnC Version     |
 *  +----------------------------+
 *  |          Meta Data         |
 *  +----------------------------+
 *  |      to-driver Buffer      |
 *  +----------------------------+
 *  |      to-clients Buffer     |
 *  +----------------------------+
 *  |     Counter Labels Buffer  |
 *  +----------------------------+
 *  |     Counter Values Buffer  |
 *  +----------------------------+
 * </pre>
 *
 * Meta Data Layout (CnC Version 3)
 * <pre>
 *  +----------------------------+
 *  |   to-driver buffer length  |
 *  +----------------------------+
 *  |  to-clients buffer length  |
 *  +----------------------------+
 *  |    labels buffer length    |
 *  +----------------------------+
 *  |    values buffer length    |
 *  +----------------------------+
 *  |   Client Liveness Timeout  |
 *  |                            |
 *  +----------------------------+
 * </pre>
 */
public class CncFileDescriptor
{
    public static final String CNC_FILE = "cnc";

    public static final int CNC_VERSION = 3;

    public static final int CNC_VERSION_FIELD_OFFSET;
    public static final int META_DATA_OFFSET;

    /* Meta Data Offsets (offsets within the meta data section) */

    public static final int TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int COUNTER_LABELS_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int COUNTER_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;

    static
    {
        CNC_VERSION_FIELD_OFFSET = 0;
        META_DATA_OFFSET = CNC_VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

        TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET = 0;
        TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET = TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        COUNTER_LABELS_BUFFER_LENGTH_FIELD_OFFSET = TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        COUNTER_VALUES_BUFFER_LENGTH_FIELD_OFFSET = COUNTER_LABELS_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET = COUNTER_VALUES_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    }

    public static final int META_DATA_LENGTH = CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;

    public static final int END_OF_META_DATA_OFFSET = align(BitUtil.SIZE_OF_INT + META_DATA_LENGTH, BitUtil.CACHE_LINE_LENGTH);

    /**
     * Compute the length of the cnc file and return it.
     *
     * @param totalLengthOfBuffers in bytes
     * @return cnc file length in bytes
     */
    public static int computeCncFileLength(final int totalLengthOfBuffers)
    {
        return END_OF_META_DATA_OFFSET + totalLengthOfBuffers;
    }

    public static int cncVersionOffset(final int baseOffset)
    {
        return baseOffset + CNC_VERSION_FIELD_OFFSET;
    }

    public static int toDriverBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + META_DATA_OFFSET + TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int toClientsBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + META_DATA_OFFSET + TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int counterLabelsBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + META_DATA_OFFSET + COUNTER_LABELS_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int counterValuesBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + META_DATA_OFFSET + COUNTER_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int clientLivenessTimeoutOffset(final int baseOffset)
    {
        return baseOffset + META_DATA_OFFSET + CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
    }

    public static UnsafeBuffer createMetaDataBuffer(final ByteBuffer buffer)
    {
        return new UnsafeBuffer(buffer, 0, BitUtil.SIZE_OF_INT + META_DATA_LENGTH);
    }

    public static void fillMetaData(
        final UnsafeBuffer cncMetaDataBuffer,
        final int toDriverBufferLength,
        final int toClientsBufferLength,
        final int counterLabelsBufferLength,
        final int counterValuesBufferLength,
        final long clientLivenessTimeout)
    {
        cncMetaDataBuffer.putInt(cncVersionOffset(0), CncFileDescriptor.CNC_VERSION);
        cncMetaDataBuffer.putInt(toDriverBufferLengthOffset(0), toDriverBufferLength);
        cncMetaDataBuffer.putInt(toClientsBufferLengthOffset(0), toClientsBufferLength);
        cncMetaDataBuffer.putInt(counterLabelsBufferLengthOffset(0), counterLabelsBufferLength);
        cncMetaDataBuffer.putInt(counterValuesBufferLengthOffset(0), counterValuesBufferLength);
        cncMetaDataBuffer.putLong(clientLivenessTimeoutOffset(0), clientLivenessTimeout);
    }

    public static UnsafeBuffer createToDriverBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        return new UnsafeBuffer(buffer, END_OF_META_DATA_OFFSET, metaDataBuffer.getInt(toDriverBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createToClientsBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_META_DATA_OFFSET + metaDataBuffer.getInt(toDriverBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(toClientsBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createCounterLabelsBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_META_DATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(counterLabelsBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createCounterValuesBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_META_DATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0)) +
            metaDataBuffer.getInt(counterLabelsBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(counterValuesBufferLengthOffset(0)));
    }

    public static long clientLivenessTimeout(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(clientLivenessTimeoutOffset(0));
    }
}
