/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.cluster.codecs.mark.VarAsciiEncodingEncoder;
import org.agrona.CloseHelper;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * Used to indicate if a cluster service is running and what configuration it is using. Errors encountered by
 * the service are recorded within this file by a {@link org.agrona.concurrent.errors.DistinctErrorLog}.
 */
public final class ClusterMarkFile implements AutoCloseable
{
    /**
     * Major version.
     */
    public static final int MAJOR_VERSION = 0;
    /**
     * Minor version.
     */
    public static final int MINOR_VERSION = 3;
    /**
     * Patch version.
     */
    public static final int PATCH_VERSION = 0;
    /**
     * Full semantic version.
     */
    public static final int SEMANTIC_VERSION = SemanticVersion.compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
    /**
     * Length of the {@code header} section.
     */
    public static final int HEADER_LENGTH = 8 * 1024;
    /**
     * Special version to indicate that component failed to start.
     */
    public static final int VERSION_FAILED = -1;
    /**
     * Min length for the error log buffer.
     */
    public static final int ERROR_BUFFER_MIN_LENGTH = 1024 * 1024;
    /**
     * Max allowed length for the error log buffer.
     */
    public static final int ERROR_BUFFER_MAX_LENGTH = Integer.MAX_VALUE - HEADER_LENGTH;
    /**
     * File extension used by the mark file.
     */
    public static final String FILE_EXTENSION = ".dat";
    /**
     * File extension used by the link file.
     */
    public static final String LINK_FILE_EXTENSION = ".lnk";
    /**
     * Mark file name.
     */
    public static final String FILENAME = "cluster-mark" + FILE_EXTENSION;
    /**
     * Link file name.
     */
    public static final String LINK_FILENAME = "cluster-mark" + LINK_FILE_EXTENSION;
    /**
     * Service mark file name.
     */
    public static final String SERVICE_FILENAME_PREFIX = "cluster-mark-service-";

    private final MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
    private final MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
    private final MarkFile markFile;
    private final UnsafeBuffer buffer;
    private final UnsafeBuffer errorBuffer;

    /**
     * Create new {@link MarkFile} for a cluster service but check if an existing service is active.
     *
     * @param file              full qualified file to the {@link MarkFile}.
     * @param type              of cluster component the {@link MarkFile} represents.
     * @param errorBufferLength for storing the error log.
     * @param epochClock        for checking liveness against.
     * @param timeoutMs         for the activity check on an existing {@link MarkFile}.
     */
    public ClusterMarkFile(
        final File file,
        final ClusterComponentType type,
        final int errorBufferLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        if (errorBufferLength < ERROR_BUFFER_MIN_LENGTH || errorBufferLength > ERROR_BUFFER_MAX_LENGTH)
        {
            throw new IllegalArgumentException("Invalid errorBufferLength: " + errorBufferLength);
        }

        final boolean markFileExists = file.exists();
        final int totalFileLength = HEADER_LENGTH + errorBufferLength;

        markFile = new MarkFile(
            file,
            markFileExists,
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            totalFileLength,
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (VERSION_FAILED == version && markFileExists)
                {
                    System.err.println("mark file version -1 indicates error on previous startup.");
                }
                else if (SemanticVersion.major(version) != MAJOR_VERSION)
                {
                    throw new ClusterException("mark file major version " + SemanticVersion.major(version) +
                        " does not match software: " + MAJOR_VERSION);
                }
            },
            null);

        buffer = markFile.buffer();
        errorBuffer = new UnsafeBuffer(this.buffer, HEADER_LENGTH, errorBufferLength);

        headerEncoder.wrap(buffer, 0);
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

        if (markFileExists)
        {
            if (buffer.capacity() != totalFileLength)
            {
                throw new ClusterException(
                    "ClusterMarkFile capacity=" + buffer.capacity() + " < expectedCapacity=" + totalFileLength);
            }

            final int existingErrorBufferLength = headerDecoder.errorBufferLength();
            final UnsafeBuffer existingErrorBuffer = new UnsafeBuffer(
                buffer, headerDecoder.headerLength(), existingErrorBufferLength);

            saveExistingErrors(file, existingErrorBuffer, type, CommonContext.fallbackLogger());
            existingErrorBuffer.setMemory(0, existingErrorBufferLength, (byte)0);
        }
        else
        {
            headerEncoder.candidateTermId(NULL_VALUE);
        }

        final ClusterComponentType existingType = headerDecoder.componentType();

        if (existingType != ClusterComponentType.NULL && existingType != type)
        {
            if (existingType != ClusterComponentType.BACKUP || ClusterComponentType.CONSENSUS_MODULE != type)
            {
                throw new ClusterException(
                    "existing Mark file type " + existingType + " not same as required type " + type);
            }
        }

        headerEncoder.componentType(type);
        headerEncoder.headerLength(HEADER_LENGTH);
        headerEncoder.errorBufferLength(errorBufferLength);
        headerEncoder.pid(SystemUtil.getPid());
        headerEncoder.startTimestamp(epochClock.time());
    }

    /**
     * Construct to read the status of an existing {@link MarkFile} for a cluster component.
     *
     * @param directory  in which the mark file exists.
     * @param filename   for the {@link MarkFile}.
     * @param epochClock to be used for checking liveness.
     * @param timeoutMs  to wait for file to exist.
     * @param logger     to which debug information will be written if an issue occurs.
     */
    public ClusterMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        this(new MarkFile(
            directory,
            filename,
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (SemanticVersion.major(version) != MAJOR_VERSION)
                {
                    throw new ClusterException(
                        "mark file major version " + SemanticVersion.major(version) +
                        " does not match software: " + MAJOR_VERSION);
                }
            },
            logger));
    }

    ClusterMarkFile(final MarkFile markFile)
    {
        this.markFile = markFile;
        buffer = markFile.buffer();
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
        errorBuffer = new UnsafeBuffer(buffer, headerDecoder.headerLength(), headerDecoder.errorBufferLength());
    }

    /**
     * Get the parent directory containing the mark file.
     *
     * @return parent directory of the mark file.
     * @see MarkFile#parentDirectory()
     */
    public File parentDirectory()
    {
        return markFile.parentDirectory();
    }

    /**
     * Determines if this path name matches the service mark file name pattern.
     *
     * @param path       to examine.
     * @param attributes ignored, only needed for BiPredicate signature matching.
     * @return true if the name matches.
     */
    public static boolean isServiceMarkFile(final Path path, final BasicFileAttributes attributes)
    {
        final String fileName = path.getFileName().toString();
        return fileName.startsWith(SERVICE_FILENAME_PREFIX) && fileName.endsWith(FILE_EXTENSION);
    }

    /**
     * Determines if this path name matches the consensus module file name pattern.
     *
     * @param path       to examine.
     * @param attributes ignored, only needed for BiPredicate signature matching.
     * @return true if the name matches.
     */
    public static boolean isConsensusModuleMarkFile(final Path path, final BasicFileAttributes attributes)
    {
        return path.getFileName().toString().equals(FILENAME);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(markFile);
    }

    /**
     * Check if the {@link MarkFile} is closed.
     *
     * @return true if the {@link MarkFile} is closed.
     */
    public boolean isClosed()
    {
        return markFile.isClosed();
    }

    /**
     * Get the current value of a candidate term id if a vote is placed in an election.
     *
     * @return the current candidate term id within an election after voting or {@link Aeron#NULL_VALUE} if
     * no voting phase of an election is currently active.
     */
    public long candidateTermId()
    {
        return buffer.getLongVolatile(MarkFileHeaderDecoder.candidateTermIdEncodingOffset());
    }

    /**
     * Cluster member id either assigned statically or as the result of dynamic membership join.
     *
     * @return cluster member id either assigned statically or as the result of dynamic membership join.
     */
    public int memberId()
    {
        return buffer.getInt(MarkFileHeaderDecoder.memberIdEncodingOffset());
    }

    /**
     * Member id assigned as part of dynamic join of a cluster.
     *
     * @param memberId assigned as part of dynamic join of a cluster.
     */
    public void memberId(final int memberId)
    {
        buffer.putInt(MarkFileHeaderEncoder.memberIdEncodingOffset(), memberId);
    }

    /**
     * Identity of the cluster instance so multiple clusters can run on the same driver.
     *
     * @return id of the cluster instance so multiple clusters can run on the same driver.
     */
    public int clusterId()
    {
        return buffer.getInt(MarkFileHeaderDecoder.clusterIdEncodingOffset());
    }

    /**
     * Identity of the cluster instance so multiple clusters can run on the same driver.
     *
     * @param clusterId of the cluster instance so multiple clusters can run on the same driver.
     */
    public void clusterId(final int clusterId)
    {
        buffer.putInt(MarkFileHeaderEncoder.clusterIdEncodingOffset(), clusterId);
    }

    /**
     * Signal the cluster component has concluded successfully and ready to start.
     */
    public void signalReady()
    {
        markFile.signalReady(SEMANTIC_VERSION);
    }

    /**
     * Signal the cluster component has failed to conclude and cannot start.
     */
    public void signalFailedStart()
    {
        markFile.signalReady(VERSION_FAILED);
    }

    /**
     * Update the activity timestamp as a proof of life.
     *
     * @param nowMs activity timestamp as a proof of life.
     */
    public void updateActivityTimestamp(final long nowMs)
    {
        if (!markFile.isClosed())
        {
            markFile.timestampOrdered(nowMs);
        }
    }

    /**
     * Read the activity timestamp of the cluster component with volatile semantics.
     *
     * @return the activity timestamp of the cluster component with volatile semantics.
     */
    public long activityTimestampVolatile()
    {
        return markFile.timestampVolatile();
    }

    /**
     * The encoder for writing the {@link MarkFile} header.
     *
     * @return the encoder for writing the {@link MarkFile} header.
     */
    public MarkFileHeaderEncoder encoder()
    {
        return headerEncoder;
    }

    /**
     * The decoder for reading the {@link MarkFile} header.
     *
     * @return the decoder for reading the {@link MarkFile} header.
     */
    public MarkFileHeaderDecoder decoder()
    {
        return headerDecoder;
    }

    /**
     * The direct buffer which wraps the region of the {@link MarkFile} which contains the error log.
     *
     * @return the direct buffer which wraps the region of the {@link MarkFile} which contains the error log.
     */
    public AtomicBuffer errorBuffer()
    {
        return errorBuffer;
    }

    /**
     * Save the existing errors from a {@link MarkFile} to a {@link PrintStream} for logging.
     *
     * @param markFile    which contains the error buffer.
     * @param errorBuffer which wraps the error log.
     * @param type        type of the mark file being checked.
     * @param logger      to which the existing errors will be printed.
     */
    public static void saveExistingErrors(
        final File markFile,
        final AtomicBuffer errorBuffer,
        final ClusterComponentType type,
        final PrintStream logger)
    {
        CommonContext.saveExistingErrors(markFile, errorBuffer, logger, type.name());
    }

    /**
     * Check if the header length is sufficient for encoding the provided properties.
     *
     * @param aeronDirectory for the media driver.
     * @param controlChannel for the consensus module.
     * @param ingressChannel from the cluster clients.
     * @param serviceName    for the application service.
     * @param authenticator  for the application service.
     */
    public static void checkHeaderLength(
        final String aeronDirectory,
        final String controlChannel,
        final String ingressChannel,
        final String serviceName,
        final String authenticator)
    {
        final int length =
            MarkFileHeaderEncoder.BLOCK_LENGTH +
            (5 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
            (null == aeronDirectory ? 0 : aeronDirectory.length()) +
            (null == controlChannel ? 0 : controlChannel.length()) +
            (null == ingressChannel ? 0 : ingressChannel.length()) +
            (null == serviceName ? 0 : serviceName.length()) +
            (null == authenticator ? 0 : authenticator.length());

        if (length > HEADER_LENGTH)
        {
            throw new ClusterException(
                "ClusterMarkFile headerLength=" + length + " > headerLengthCapacity=" + HEADER_LENGTH);
        }
    }

    /**
     * The filename to be used for the mark file given a service id.
     *
     * @param serviceId of the service the {@link ClusterMarkFile} represents.
     * @return the filename to be used for the mark file given a service id.
     */
    public static String markFilenameForService(final int serviceId)
    {
        return SERVICE_FILENAME_PREFIX + serviceId + FILE_EXTENSION;
    }

    /**
     * The filename to be used for the link file given a service id.
     *
     * @param serviceId of the service the {@link ClusterMarkFile} represents.
     * @return the filename to be used for the link file given a service id.
     */
    public static String linkFilenameForService(final int serviceId)
    {
        return SERVICE_FILENAME_PREFIX + serviceId + LINK_FILE_EXTENSION;
    }

    /**
     * The control properties for communicating between the consensus module and the services.
     *
     * @return the control properties for communicating between the consensus module and the services.
     */
    public ClusterNodeControlProperties loadControlProperties()
    {
        final MarkFileHeaderDecoder decoder = new MarkFileHeaderDecoder();
        decoder.wrap(
            headerDecoder.buffer(),
            headerDecoder.offset(),
            MarkFileHeaderDecoder.BLOCK_LENGTH,
            MarkFileHeaderDecoder.SCHEMA_VERSION);

        return new ClusterNodeControlProperties(
            decoder.memberId(),
            decoder.serviceStreamId(),
            decoder.consensusModuleStreamId(),
            decoder.aeronDirectory(),
            decoder.controlChannel());
    }

    /**
     * Forces any changes made to the mark file's content to be written to the storage device containing the mapped
     * file.
     * @since 1.44.0
     */
    public void force()
    {
        if (!markFile.isClosed())
        {
            final MappedByteBuffer mappedByteBuffer = markFile.mappedByteBuffer();
            mappedByteBuffer.force();
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ClusterMarkFile{" +
            "semanticVersion=" + SemanticVersion.toString(SEMANTIC_VERSION) +
            ", markFile=" + markFile.markFile() +
            '}';
    }
}
