/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.SemanticVersion;
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
 * Meta Data Layout {@link #CNC_VERSION}
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

    /**
     * Version of the CnC file using semantic versioning ({@link SemanticVersion}) stored in an integer.
     */
    public static final int CNC_VERSION = SemanticVersion.compose(0, 2, 0);

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

    /**
     * Fill the CnC file with metadata to define its sections.
     *
     * @param cncMetaDataBuffer           that wraps the metadata section of the CnC file.
     * @param toDriverBufferLength        for sending commands to the driver.
     * @param toClientsBufferLength       for broadcasting events to the clients.
     * @param counterMetaDataBufferLength buffer length for counters metadata.
     * @param counterValuesBufferLength   buffer length for counter values.
     * @param clientLivenessTimeoutNs     timeout value in nanoseconds for client liveness and inter-service interval.
     * @param errorLogBufferLength        for recording the distinct error log.
     * @param startTimestampMs            epoch at which the driver started.
     * @param pid                         for the process hosting the driver.
     */
    public static void fillMetaData(
        final UnsafeBuffer cncMetaDataBuffer,
        final int toDriverBufferLength,
        final int toClientsBufferLength,
        final int counterMetaDataBufferLength,
        final int counterValuesBufferLength,
        final long clientLivenessTimeoutNs,
        final int errorLogBufferLength,
        final long startTimestampMs,
        final long pid)
    {
        cncMetaDataBuffer.putInt(toDriverBufferLengthOffset(0), toDriverBufferLength);
        cncMetaDataBuffer.putInt(toClientsBufferLengthOffset(0), toClientsBufferLength);
        cncMetaDataBuffer.putInt(countersMetaDataBufferLengthOffset(0), counterMetaDataBufferLength);
        cncMetaDataBuffer.putInt(countersValuesBufferLengthOffset(0), counterValuesBufferLength);
        cncMetaDataBuffer.putInt(errorLogBufferLengthOffset(0), errorLogBufferLength);
        cncMetaDataBuffer.putLong(clientLivenessTimeoutOffset(0), clientLivenessTimeoutNs);
        cncMetaDataBuffer.putLong(startTimestampOffset(0), startTimestampMs);
        cncMetaDataBuffer.putLong(pidOffset(0), pid);
    }

    /**
     * Compute the length of the cnc file from metaDataBuffer and return it.
     *
     * @param metaDataBuffer within the CnC file.
     * @param alignment for file length to adhere to
     * @return cnc file length in bytes
     */
    public static int computeCncFileLengthFromMetaDataBuffer(final DirectBuffer metaDataBuffer, final int alignment)
    {
        final int toDriverBufferLength = metaDataBuffer.getInt(toDriverBufferLengthOffset(0));
        final int toClientsBufferLength = metaDataBuffer.getInt(toClientsBufferLengthOffset(0));
        final int counterMetaDataBufferLength = metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0));
        final int counterValuesBufferLength = metaDataBuffer.getInt(countersValuesBufferLengthOffset(0));
        final int errorBufferLength = metaDataBuffer.getInt(errorLogBufferLengthOffset(0));
        return CncFileDescriptor.computeCncFileLength(
            toDriverBufferLength +
                toClientsBufferLength +
                counterMetaDataBufferLength +
                counterValuesBufferLength +
                errorBufferLength,
                alignment
                );
    }

    /**
     * Signal the the CnC file is ready for use by client by writing the version into the CnC file.
     *
     * @param cncMetaDataBuffer for the CnC file.
     */
    public static void signalCncReady(final UnsafeBuffer cncMetaDataBuffer)
    {
        cncMetaDataBuffer.putIntVolatile(cncVersionOffset(0), CNC_VERSION);
    }

    /**
     * Create the buffer which wraps the area in the CnC file for the metadata about the CnC file itself.
     * @param buffer for the CnC file
     * @return the buffer which wraps the area in the CnC file for the metadata about the CnC file itself.
     */
    public static UnsafeBuffer createMetaDataBuffer(final ByteBuffer buffer)
    {
        return new UnsafeBuffer(buffer, 0, META_DATA_LENGTH);
    }

    /**
     * Create the buffer which wraps the area in the CnC file for the command buffer from clients to the driver.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the command buffer from clients to the driver.
     */
    public static UnsafeBuffer createToDriverBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        return new UnsafeBuffer(buffer, END_OF_METADATA_OFFSET, metaDataBuffer.getInt(toDriverBufferLengthOffset(0)));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the broadcast buffer from the driver to clients.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the broadcast buffer from the driver to clients.
     */
    public static UnsafeBuffer createToClientsBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET + metaDataBuffer.getInt(toDriverBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(toClientsBufferLengthOffset(0)));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the counters metadata.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the counters metadata.
     */
    public static UnsafeBuffer createCountersMetaDataBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0)));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the counter values.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the counter values.
     */
    public static UnsafeBuffer createCountersValuesBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0)) +
            metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(countersValuesBufferLengthOffset(0)));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the error log.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the error log.
     */
    public static UnsafeBuffer createErrorLogBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = END_OF_METADATA_OFFSET +
            metaDataBuffer.getInt(toDriverBufferLengthOffset(0)) +
            metaDataBuffer.getInt(toClientsBufferLengthOffset(0)) +
            metaDataBuffer.getInt(countersMetaDataBufferLengthOffset(0)) +
            metaDataBuffer.getInt(countersValuesBufferLengthOffset(0));

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(errorLogBufferLengthOffset(0)));
    }

    /**
     * Get the timeout in nanoseconds for tracking client liveness and inter-service timeout.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the timeout in milliseconds for tracking client liveness.
     */
    public static long clientLivenessTimeoutNs(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(clientLivenessTimeoutOffset(0));
    }

    /**
     * Get the start timestamp in milliseconds for the media driver.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the start timestamp in milliseconds for the media driver.
     */
    public static long startTimestampMs(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(startTimestampOffset(0));
    }

    /**
     * Get the process PID hosting the driver.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the process PID hosting the driver.
     */
    public static long pid(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(pidOffset(0));
    }

    /**
     * Check the version of the CnC file is compatible with application.
     *
     * @param cncVersion of the CnC file.
     * @throws AeronException if the major versions are not compatible.
     */
    public static void checkVersion(final int cncVersion)
    {
        if (SemanticVersion.major(CNC_VERSION) != SemanticVersion.major(cncVersion))
        {
            throw new AeronException("CnC version not compatible:" +
                " app=" + SemanticVersion.toString(CNC_VERSION) +
                " file=" + SemanticVersion.toString(cncVersion));
        }
    }
}
