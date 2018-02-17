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

import io.aeron.cluster.codecs.cnc.ClusterComponentType;
import io.aeron.cluster.codecs.cnc.CncHeaderDecoder;
import io.aeron.cluster.codecs.cnc.CncHeaderEncoder;
import io.aeron.cluster.codecs.cnc.VarAsciiEncodingEncoder;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.CncFile;
import org.agrona.SystemUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.Objects;
import java.util.function.Consumer;

public class ClusterCncFile implements AutoCloseable
{
    public static final String FILENAME = "cnc.dat";
    public static final int ALIGNMENT = 1024;

    private final CncHeaderDecoder cncHeaderDecoder;
    private final CncHeaderEncoder cncHeaderEncoder;
    private final CncFile cncFile;
    private final UnsafeBuffer cncBuffer;

    public ClusterCncFile(
        final File file,
        final ClusterComponentType type,
        final int totalFileLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        this.cncFile = new CncFile(
            file,
            file.exists(),
            CncHeaderDecoder.versionEncodingOffset(),
            CncHeaderDecoder.activityTimestampEncodingOffset(),
            totalFileLength,
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != CncHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("CnC file version " + version +
                        " does not match software:" + CncHeaderDecoder.SCHEMA_VERSION);
                }
            },
            null);

        this.cncBuffer = cncFile.buffer();

        cncHeaderDecoder = new CncHeaderDecoder();
        cncHeaderEncoder = new CncHeaderEncoder();

        cncHeaderEncoder.wrap(cncBuffer, 0);
        cncHeaderDecoder.wrap(cncBuffer, 0, CncHeaderDecoder.BLOCK_LENGTH, CncHeaderDecoder.SCHEMA_VERSION);

        final ClusterComponentType existingType = cncHeaderDecoder.componentType();

        if (existingType != ClusterComponentType.NULL && existingType != type)
        {
            throw new IllegalStateException(
                "existing CnC file type " + existingType + " not same as required type " + type);
        }

        cncHeaderEncoder.componentType(type);
        cncHeaderEncoder.pid(SystemUtil.getPid());
    }

    public ClusterCncFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        cncFile = new CncFile(
            directory,
            filename,
            CncHeaderDecoder.versionEncodingOffset(),
            CncHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != CncHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("CnC file version " + version +
                        " does not match software:" + CncHeaderDecoder.SCHEMA_VERSION);
                }
            },
            logger);

        this.cncBuffer = cncFile.buffer();

        cncHeaderDecoder = new CncHeaderDecoder();
        cncHeaderEncoder = null;

        cncHeaderDecoder.wrap(cncBuffer, 0, CncHeaderDecoder.BLOCK_LENGTH, CncHeaderDecoder.SCHEMA_VERSION);
    }

    public void close()
    {
        CloseHelper.quietClose(cncFile);
    }

    public void signalCncReady()
    {
        cncFile.signalCncReady(CncHeaderEncoder.SCHEMA_VERSION);
    }

    public void updateActivityTimestamp(final long nowMs)
    {
        cncFile.timestampOrdered(nowMs);
    }

    public CncHeaderEncoder encoder()
    {
        return cncHeaderEncoder;
    }

    public CncHeaderDecoder decoder()
    {
        return cncHeaderDecoder;
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
            CncHeaderEncoder.BLOCK_LENGTH +
                (6 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
                aeronDirectory.length() +
                archiveChannel.length() +
                serviceControlChannel.length() +
                (null == ingressChannel ? 0 : ingressChannel.length()) +
                (null == serviceName ? 0 : serviceName.length()) +
                (null == authenticator ? 0 : authenticator.length()), alignment);
    }
}
