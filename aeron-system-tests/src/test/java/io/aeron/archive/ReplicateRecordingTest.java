/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.aeron.Aeron.NULL_VALUE;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReplicateRecordingTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int FRAGMENT_LIMIT = 10;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SRC_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
    private static final String SRC_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8090";
    private static final String DST_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8091";
    private static final String SRC_EVENTS_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:8030";
    private static final String DST_EVENTS_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:8031";

    private static final int RECORDING_STREAM_ID = 33;
    private static final String RECORDING_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(TERM_BUFFER_LENGTH)
        .build();

    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private ArchivingMediaDriver srcArchivingMediaDriver;
    private ArchivingMediaDriver dstArchivingMediaDriver;
    private Aeron aeron;
    private AeronArchive aeronArchive;

    @Before
    public void before()
    {
        final String srcAeronDirectoryName = CommonContext.generateRandomDirName();
        final String dstAeronDirectoryName = CommonContext.generateRandomDirName();

        srcArchivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(srcAeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(false)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(srcAeronDirectoryName)
                .controlChannel(SRC_CONTROL_CHANNEL)
                .recordingEventsChannel(SRC_EVENTS_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "src-archive"))
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        dstArchivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(dstAeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(false)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(dstAeronDirectoryName)
                .controlChannel(DST_CONTROL_CHANNEL)
                .recordingEventsChannel(DST_EVENTS_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "dst-archive"))
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(dstAeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .controlRequestChannel(DST_CONTROL_CHANNEL)
                .aeron(aeron));
    }

    @After
    public void after()
    {
        CloseHelper.close(aeronArchive);
        CloseHelper.close(aeron);
        CloseHelper.close(dstArchivingMediaDriver);
        CloseHelper.close(srcArchivingMediaDriver);

        dstArchivingMediaDriver.archive().context().deleteArchiveDirectory();
        srcArchivingMediaDriver.archive().context().deleteArchiveDirectory();
    }

    @Test
    public void shouldThrowExceptionWhenDstRecordingIdUnknown()
    {
        final long unknownId = 7L;
        final boolean liveMerge = false;
        try
        {
            aeronArchive.replicate(
                NULL_VALUE, unknownId, REPLAY_CHANNEL, SRC_CONTROL_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.UNKNOWN_RECORDING, ex.errorCode());
            assertTrue(ex.getMessage().endsWith(Long.toString(unknownId)));
            return;
        }

        fail("expected archive exception");
    }

    @Test
    public void shouldStopReplication()
    {
        final boolean liveMerge = false;
        final long replicationId = aeronArchive.replicate(
            NULL_VALUE, NULL_VALUE, REPLAY_CHANNEL, SRC_CONTROL_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);

        Thread.yield();

        aeronArchive.stopReplication(replicationId);
    }
}
