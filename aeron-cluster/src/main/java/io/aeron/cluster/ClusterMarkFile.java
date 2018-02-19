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
package io.aeron.cluster;

import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.cluster.codecs.mark.VarAsciiEncodingEncoder;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.MarkFile;
import org.agrona.SystemUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.Objects;
import java.util.function.Consumer;

public class ClusterMarkFile implements AutoCloseable
{
    public static final String FILENAME = "cluster-mark.dat";
    public static final int ALIGNMENT = 1024;

    private final MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
    private final MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
    private final MarkFile markFile;
    private final UnsafeBuffer buffer;

    public ClusterMarkFile(
        final File file,
        final ClusterComponentType type,
        final int totalFileLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        this.markFile = new MarkFile(
            file,
            file.exists(),
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            totalFileLength,
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("Mark file version " + version +
                        " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
                }
            },
            null);

        this.buffer = markFile.buffer();

        headerEncoder.wrap(buffer, 0);
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

        final ClusterComponentType existingType = headerDecoder.componentType();

        if (existingType != ClusterComponentType.NULL && existingType != type)
        {
            throw new IllegalStateException(
                "existing Mark file type " + existingType + " not same as required type " + type);
        }

        headerEncoder.componentType(type);
        headerEncoder.pid(SystemUtil.getPid());
    }

    public ClusterMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        markFile = new MarkFile(
            directory,
            filename,
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("Mark file version " + version +
                        " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
                }
            },
            logger);

        this.buffer = markFile.buffer();
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
    }

    public void close()
    {
        CloseHelper.close(markFile);
    }

    public void signalReady()
    {
        markFile.signalReady(MarkFileHeaderDecoder.SCHEMA_VERSION);
    }

    public void updateActivityTimestamp(final long nowMs)
    {
        markFile.timestampOrdered(nowMs);
    }

    public MarkFileHeaderEncoder encoder()
    {
        return headerEncoder;
    }

    public MarkFileHeaderDecoder decoder()
    {
        return headerDecoder;
    }

    public static int alignedTotalFileLength(
        final int alignment,
        final String aeronDirectory,
        final String archiveChannel,
        final String serviceControlChannel,
        final String ingressChannel,
        final String serviceName,
        final String authenticator)
    {
        Objects.requireNonNull(aeronDirectory);
        Objects.requireNonNull(archiveChannel);
        Objects.requireNonNull(serviceControlChannel);

        return BitUtil.align(
            MarkFileHeaderEncoder.BLOCK_LENGTH +
                (6 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
                aeronDirectory.length() +
                archiveChannel.length() +
                serviceControlChannel.length() +
                (null == ingressChannel ? 0 : ingressChannel.length()) +
                (null == serviceName ? 0 : serviceName.length()) +
                (null == authenticator ? 0 : authenticator.length()), alignment);
    }
}
