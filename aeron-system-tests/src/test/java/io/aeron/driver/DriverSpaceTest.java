/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.ErrorCode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledOnOs(OS.LINUX)
public class DriverSpaceTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    void shouldThrowExceptionWithCorrectErrorCodeForLackOfSpace()
    {
        final Path tempfsDir = new File("/mnt/tmp_aeron_dir").toPath();
        try
        {
            assumeTrue(Files.exists(tempfsDir), () -> tempfsDir + " does not exist");
            assumeTrue(Files.isDirectory(tempfsDir));
            assumeTrue(Files.isWritable(tempfsDir));
            final FileStore fileStore = Files.getFileStore(tempfsDir);
            assumeTrue(fileStore.getUsableSpace() < (32 * 1024 * 1024), "Skipping as file system is too large");
        }
        catch (final IOException e)
        {
            assumeTrue(false);
        }

        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(tempfsDir.resolve("aeron-no-space").toString())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .performStorageChecks(true);

        try (TestMediaDriver driver = TestMediaDriver.launch(context, systemTestWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            try
            {
                aeron.addPublication("aeron:ipc?term-length=16m", 10001);
                fail("RegistrationException was not thrown");
            }
            catch (final RegistrationException ex)
            {
                assertEquals(ErrorCode.STORAGE_SPACE, ex.errorCode());
            }
        }
    }
}
