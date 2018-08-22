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
package io.aeron;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.agrona.BitUtil.*;

/**
 * Description of the command and control file used between driver and clients.
 * <p>
 * File Layout
 * <pre>
 *  +-----------------------------+
 *  |          Meta Data          |
 *  +-----------------------------+
 *  |      to-driver Buffer       |
 *  +-----------------------------+
 *  |      to-clients Buffer      |
 *  +-----------------------------+
 *  |   Counters Metadata Buffer  |
 *  +-----------------------------+
 *  |    Counters Values Buffer   |
 *  +-----------------------------+
 *  |          Error Log          |
 *  +-----------------------------+
 * </pre>
 * <p>
 * Meta Data Layout (CnC Version 13)
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                      Aeron CnC Version                        |
 *  +---------------------------------------------------------------+
 *  |                   to-driver buffer length                     |
 *  +---------------------------------------------------------------+
 *  |                  to-clients buffer length                     |
 *  +---------------------------------------------------------------+
 *  |               Counters Metadata buffer length                 |
 *  +---------------------------------------------------------------+
 *  |                Counters Values buffer length                  |
 *  +---------------------------------------------------------------+
 *  |                   Error Log buffer length                     |
 *  +---------------------------------------------------------------+
 *  |                   Client Liveness Timeout                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Driver Start Timestamp                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Driver PID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class CncFileDescriptor
{
    public static final String CNC_FILE = "cnc.dat";

    public static final int CNC_VERSION = 14;

    public static final int CNC_VERSION_FIELD_OFFSET;
    public static final int TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
    public static final int ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;
    public static final int START_TIMESTAMP_FIELD_OFFSET;
    public static final int PID_FIELD_OFFSET;

    static
    {
        CNC_VERSION_FIELD_OFFSET = 0;
        TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET = CNC_VERSION_FIELD_OFFSET + SIZE_OF_INT;
        TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET = TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET = TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET = ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        START_TIMESTAMP_FIELD_OFFSET = CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET + SIZE_OF_LONG;
        PID_FIELD_OFFSET = START_TIMESTAMP_FIELD_OFFSET + SIZE_OF_LONG;
    }

    public static final int META_DATA_LENGTH = PID_FIELD_OFFSET + SIZE_OF_LONG;
    public static final int END_OF_METADATA_OFFSET = align(META_DATA_LENGTH, (CACHE_LINE_LENGTH * 2));

    /**
     * Compute the length of the cnc file and return it.
     *
     * @param totalLengthOfBuffers in bytes
     * @param alignment for file length to adhere to
     * @return cnc file length in bytes
     */
    public static int computeCncFileLength(final int totalLengthOfBuffers, final int alignment)
    {
        return align(END_OF_METADATA_OFFSET + totalLengthOfBuffers, alignment);
    }

    public static int cncVersionOffset(final int baseOffset)
    {
        return baseOffset + CNC_VERSION_FIELD_OFFSET;
    }

    public static int toDriverBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int toClientsBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int countersMetaDataBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int countersValuesBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int clientLivenessTimeoutOffset(final int baseOffset)
    {
        return baseOffset + CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
    }

    public static int errorLogBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;
    }

    public static int startTimestampOffset(final int baseOffset)
    {
        return baseOffset + START_TIMESTAMP_FIELD_OFFSET;
    }

    public static int pidOffset(final int baseOffset)
    {
        return baseOffset + PID_FIELD_OFFSET;
    }

    public static void fillMetaData(
        final UnsafeBuffer cncMetaDataBuffer,
        final int toDriverBufferLength,
        final int toClientsBufferLength,
        final int counterMetaDataBufferLength,
        final int counterValuesBufferLength,
        final long clientLivenessTimeout,
        final int errorLogBufferLength,
        final long startTimestamp,
        final long pid)
    {
        cncMetaDataBuffer.putInt(toDriverBufferLengthOffset(0), toDriverBufferLength);
        cncMetaDataBuffer.putInt(toClientsBufferLengthOffset(0), toClientsBufferLength);
        cncMetaDataBuffer.putInt(countersMetaDataBufferLengthOffset(0), counterMetaDataBufferLength);
        cncMetaDataBuffer.putInt(countersValuesBufferLengthOffset(0), counterValuesBufferLength);
        cncMetaDataBuffer.putInt(errorLogBufferLengthOffset(0), errorLogBufferLength);
        cncMetaDataBuffer.putLong(clientLivenessTimeoutOffset(0), clientLivenessTimeout);
        cncMetaDataBuffer.putLong(startTimestampOffset(0), startTimestamp);
        cncMetaDataBuffer.putLong(pidOffset(0), pid);
    }

    public static void signalCncReady(final UnsafeBuffer cncMetaDataBuffer)
    {
        cncMetaDataBuffer.putIntVolatile(cncVersionOffset(0), CNC_VERSION);
    }

    public static UnsafeBuffer createMetaDataBuffer(final ByteBuffer buffer)
    {
        return new UnsafeBuffer(buffer, 0, META_DATA_LENGTH);
    }

    public static UnsafeBuffer createToDriverBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        return new UnsafeBuffer(buffer, END_OF_METADATA_OFFSET, metaDataBuffer.getInt(toDriverBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createToClientsBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET + metaDataBuffer.getInt(toDriverBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(toClientsBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createCountersMetaDataBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createCountersValuesBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0)) +
            metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(countersValuesBufferLengthOffset(0)));
    }

    public static UnsafeBuffer createErrorLogBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0)) +
            metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0)) +
            metaDataBuffer.getInt(countersValuesBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(errorLogBufferLengthOffset(0)));
    }

    public static long clientLivenessTimeout(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(clientLivenessTimeoutOffset(0));
    }

    public static long startTimestamp(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(startTimestampOffset(0));
    }

    public static long pid(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(pidOffset(0));
    }
}
