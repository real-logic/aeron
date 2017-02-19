/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

import org.agrona.IoUtil;
import org.junit.*;

import java.io.File;

public class ArchiveIndexTest
{
    static final StreamInstance STREAM_INSTANCE_A = new StreamInstance("sourceA", 1, "channel1", 1);
    static final StreamInstance STREAM_INSTANCE_B = new StreamInstance("sourceA", 2, "channel1", 2);
    static final StreamInstance STREAM_INSTANCE_C = new StreamInstance("sourceA", 3, "channel1", 3);
    static final StreamInstance STREAM_INSTANCE_D = new StreamInstance("sourceA", 4, "channel1", 4);
    private static File archiveFolder;

    static int streamInstanceAId;
    static int streamInstanceBId;
    static int streamInstanceCId;
    @BeforeClass
    public static void setup() throws Exception
    {
        archiveFolder = ImageArchivingSessionTest.makeTempFolder();
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder);)
        {
            streamInstanceAId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_A);
            streamInstanceBId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_B);
            streamInstanceCId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_C);
        }
    }

    @AfterClass
    public static void teardown()
    {
        IoUtil.delete(archiveFolder, true);
    }

    @Test
    public void shouldReloadExistingIndex() throws Exception
    {
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            Assert.assertEquals(streamInstanceAId, archiveIndex.getStreamInstanceId(STREAM_INSTANCE_A).getInt(0));
            Assert.assertEquals(streamInstanceBId, archiveIndex.getStreamInstanceId(STREAM_INSTANCE_B).getInt(0));
            Assert.assertEquals(streamInstanceCId, archiveIndex.getStreamInstanceId(STREAM_INSTANCE_C).getInt(0));
        }
    }

    @Test
    public void shouldAppendToExistingIndex() throws Exception
    {
        final int newStreamInstanceId;
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newStreamInstanceId = archiveIndex.addNewStreamInstance(STREAM_INSTANCE_D);

            Assert.assertEquals(newStreamInstanceId, archiveIndex.getStreamInstanceId(STREAM_INSTANCE_D).getInt(0));
            Assert.assertEquals(STREAM_INSTANCE_D, archiveIndex.getStreamInstance(newStreamInstanceId));
        }

        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            Assert.assertEquals(newStreamInstanceId, archiveIndex.getStreamInstanceId(STREAM_INSTANCE_D).getInt(0));
            Assert.assertEquals(STREAM_INSTANCE_D, archiveIndex.getStreamInstance(newStreamInstanceId));
        }
    }
}
