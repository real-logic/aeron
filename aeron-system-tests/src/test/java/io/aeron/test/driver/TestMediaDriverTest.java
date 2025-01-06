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
package io.aeron.test.driver;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TestMediaDriverTest
{
    @Test
    void countersReaderReturnsTheSameInstanceForTheEntireLifetimeOfTheDriver()
    {
        final File aeronDirectory;
        final MediaDriver.Context context = new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(false);
        try (TestMediaDriver driver = TestMediaDriver.launch(context, null))
        {
            aeronDirectory = driver.context().aeronDirectory();
            assertNotNull(aeronDirectory);

            final CountersReader countersReader = driver.counters();
            assertNotNull(countersReader);
            assertSame(countersReader, driver.counters());
        }

        assertTrue(aeronDirectory.exists());
        IoUtil.delete(aeronDirectory, false);
        assertFalse(aeronDirectory.exists());
    }

    @Test
    void connectToCMediaDriverWithoutSpecifyingAeronDir()
    {
        assumeTrue(TestMediaDriver.shouldRunCMediaDriver());

        final MediaDriver.Context context = new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(false);
        assertEquals(CommonContext.getAeronDirectoryName(), context.aeronDirectoryName());
        try (TestMediaDriver mediaDriver = CTestMediaDriver.launch(context, false, null);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName())))
        {
            final File aeronDirectory = aeron.context().aeronDirectory();
            assertNotNull(aeronDirectory);
            assertTrue(aeronDirectory.exists());
            assertNotNull(mediaDriver);
        }
        finally
        {
            context.deleteDirectory();
        }
    }
}
