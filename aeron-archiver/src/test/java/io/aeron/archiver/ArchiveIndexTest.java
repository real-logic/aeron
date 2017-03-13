/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.aeron.archiver;

import io.aeron.archiver.messages.ArchiveDescriptorDecoder;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;

import java.io.*;

public class ArchiveIndexTest
{
    static final UnsafeBuffer UB =
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(ArchiveIndex.INDEX_RECORD_SIZE, 64));
    static final ArchiveDescriptorDecoder DECODER = new ArchiveDescriptorDecoder();
    static final StreamInstance STREAM_INSTANCE_A = new StreamInstance("sourceA", 6, "channelG", 1);
    static final StreamInstance STREAM_INSTANCE_B = new StreamInstance("sourceV", 7, "channelH", 2);
    static final StreamInstance STREAM_INSTANCE_C = new StreamInstance("sourceB", 8, "channelK", 3);
    static final StreamInstance STREAM_INSTANCE_D = new StreamInstance("sourceN", 9, "channelJ", 4);
    private static File archiveFolder;

    static int streamInstanceAId;
    static int streamInstanceBId;
    static int streamInstanceCId;
    @BeforeClass
    public static void setup() throws Exception
    {
        DECODER.wrap(UB,
                     ArchiveIndex.INDEX_FRAME_LENGTH,
                     ArchiveDescriptorDecoder.BLOCK_LENGTH,
                     ArchiveDescriptorDecoder.SCHEMA_VERSION);
        archiveFolder = TestUtil.makeTempFolder();
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder);)
        {
            streamInstanceAId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_A, 4096, 0);
            streamInstanceBId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_B, 4096, 0);
            streamInstanceCId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_C, 4096, 0);
        }
    }

    @AfterClass
    public static void teardown()
    {
        IoUtil.delete(archiveFolder, false);
    }

    @Test
    public void shouldReloadExistingIndex() throws Exception
    {
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            verifyArchiveForId(archiveIndex, streamInstanceAId, STREAM_INSTANCE_A);
            verifyArchiveForId(archiveIndex, streamInstanceBId, STREAM_INSTANCE_B);
            verifyArchiveForId(archiveIndex, streamInstanceCId, STREAM_INSTANCE_C);
        }
    }

    private void verifyArchiveForId(final ArchiveIndex archiveIndex, final int id, final StreamInstance streamInstance)
        throws IOException
    {
        UB.byteBuffer().clear();
        archiveIndex.readArchiveDescriptor(id, UB.byteBuffer());
        DECODER.limit(ArchiveIndex.INDEX_FRAME_LENGTH + ArchiveDescriptorDecoder.BLOCK_LENGTH);
        Assert.assertEquals(id, DECODER.streamInstanceId());
        Assert.assertEquals(streamInstance.sessionId(), DECODER.sessionId());
        Assert.assertEquals(streamInstance.streamId(), DECODER.streamId());
        Assert.assertEquals(streamInstance.source(), DECODER.source());
        Assert.assertEquals(streamInstance.channel(), DECODER.channel());
    }

    @Test
    public void shouldAppendToExistingIndex() throws Exception
    {
        final int newStreamInstanceId;
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newStreamInstanceId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_D, 4096, 0);
        }

        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            verifyArchiveForId(archiveIndex, streamInstanceAId, STREAM_INSTANCE_A);
            verifyArchiveForId(archiveIndex, newStreamInstanceId, STREAM_INSTANCE_D);
        }
    }

    @Test
    public void shouldAllowMultipleInstancesForSameStream() throws Exception
    {
        final int newStreamInstanceId;
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newStreamInstanceId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_A, 4096, 0);
            Assert.assertNotEquals(streamInstanceAId, newStreamInstanceId);
        }
    }
}
