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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.CncFileDescriptor;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class DriverShouldStartIfAeronDirectoryExistsTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    void test(final @TempDir Path tempDir) throws IOException
    {
        final Path aeronDir = tempDir.resolve("aeron-existing-dir");
        Files.createDirectories(aeronDir);
        assertTrue(Files.exists(aeronDir));
        final Path cncFile = aeronDir.resolve(CncFileDescriptor.CNC_FILE);
        assertTrue(Files.notExists(cncFile));

        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDir.toString()), systemTestWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            assertTrue(aeron.context().cncFile().exists());
        }
    }
}
