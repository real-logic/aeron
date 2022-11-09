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
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class DriverSpaceTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @ParameterizedTest
    @MethodSource("storageCheckOptions")
    void shouldThrowExceptionWithCorrectErrorCodeForLackOfSpace(
        final boolean performStorageChecks, final boolean useSparseFiles) throws IOException
    {
        assumeTrue(performStorageChecks || OS.LINUX != OS.current() ||
            !useSparseFiles && TestMediaDriver.shouldRunCMediaDriver(),
            "With storage checks disabled the file-system operations on Linux do not fail with an error unless" +
            " useSparseFiles=false and the C media driver is used");

        final Path tempfsDir;
        switch (OS.current())
        {
            case WINDOWS:
                tempfsDir = new File("T:/tmp_aeron_dir").toPath();
                break;
            case MAC:
                tempfsDir = new File("/Volumes/tmp_aeron_dir").toPath();
                break;
            default:
                tempfsDir = new File("/mnt/tmp_aeron_dir").toPath();
                break;
        }
        assumeTrue(Files.exists(tempfsDir), () -> tempfsDir + " does not exist");
        assumeTrue(Files.isDirectory(tempfsDir), () -> tempfsDir + " is not a directory");
        assumeTrue(Files.isWritable(tempfsDir), () -> tempfsDir + " is not writable");

        try
        {
            final FileStore fileStore = Files.getFileStore(tempfsDir);
            assumeTrue(fileStore.getUsableSpace() < (32 * 1024 * 1024), "Skipping as file system is too large");
        }
        catch (final IOException e)
        {
            abort("File store not accessible");
        }

        final Path aeronDir = tempfsDir.resolve("aeron-no-space");
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .performStorageChecks(performStorageChecks)
            .termBufferSparseFile(useSparseFiles);

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
                final Path publicationsDir = aeronDir.resolve("publications");
                assertTrue(Files.exists(publicationsDir));
                try (Stream<Path> files = Files.list(publicationsDir))
                {
                    assertEquals(
                        Collections.emptyList(), files.collect(Collectors.toList()), "Log file was not deleted");
                }
            }
        }
    }

    private static List<Arguments> storageCheckOptions()
    {
        return Arrays.asList(
            Arguments.of(true /* performStorageChecks */, false /* useSparseFiles */),
            Arguments.of(true /* performStorageChecks */, true /* useSparseFiles */),
            Arguments.of(false /* performStorageChecks */, false /* useSparseFiles */),
            Arguments.of(false /* performStorageChecks */, true /* useSparseFiles */));
    }
}
