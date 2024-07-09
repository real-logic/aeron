/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.tooling;

import io.aeron.CommonContext;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.CncFileDescriptor.*;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Utility functions for the driver tooling.
 */
public class ToolingUtils
{
    /**
     * Map an existing file as a read only buffer.
     *
     * @param location of file to map.
     * @return the mapped file.
     */
    public static MappedByteBuffer mapExistingFileReadOnly(final File location)
    {
        if (!location.exists())
        {
            final String msg = "file not found: " + location.getAbsolutePath();
            throw new IllegalStateException(msg);
        }

        MappedByteBuffer mappedByteBuffer = null;
        try (RandomAccessFile file = new RandomAccessFile(location, "r");
            FileChannel channel = file.getChannel())
        {
            mappedByteBuffer = channel.map(READ_ONLY, 0, channel.size());
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return mappedByteBuffer;
    }

    /**
     * Map an existing CnC file.
     *
     * @return the {@link CountersReader} over the CnC file.
     */
    public static CountersReader mapCounters()
    {
        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final MappedByteBuffer cncByteBuffer = mapExistingFileReadOnly(cncFile);
        final DirectBuffer cncMetaData = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaData.getInt(cncVersionOffset(0));

        checkVersion(cncVersion);

        return new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
            createCountersValuesBuffer(cncByteBuffer, cncMetaData),
            US_ASCII);
    }

    /**
     * Map an existing CnC file.
     *
     * @param cncFileVersion to set as value of file.
     * @return the {@link CountersReader} over the CnC file.
     */
    public static CountersReader mapCounters(final MutableInteger cncFileVersion)
    {
        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final MappedByteBuffer cncByteBuffer = mapExistingFileReadOnly(cncFile);
        final DirectBuffer cncMetaData = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaData.getInt(cncVersionOffset(0));

        cncFileVersion.set(cncVersion);
        checkVersion(cncVersion);

        return new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
            createCountersValuesBuffer(cncByteBuffer, cncMetaData),
            US_ASCII);
    }
}
