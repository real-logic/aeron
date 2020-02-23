/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.test;

import org.agrona.IoUtil;
import org.agrona.collections.Object2ObjectHashMap;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.File;
import java.util.Map;

public class MediaDriverTestWatcher implements TestWatcher, DriverOutputConsumer
{
    private final Map<String, StdOutputFiles> outputFilesByAeronDirectoryName = new Object2ObjectHashMap<>();

    public void testFailed(final ExtensionContext context, final Throwable cause)
    {
        if (TestMediaDriver.shouldRunCMediaDriver())
        {
            System.out.println("C Media Driver tests failed");
            outputFilesByAeronDirectoryName.forEach((aeronDirectoryName, files) ->
            {
                System.out.println("Media Driver: " + aeronDirectoryName);
                System.out.println("  stdout: " + files.stdout + ", stderr: " + files.stderr);
            });
        }
    }

    public void testSuccessful(final ExtensionContext context)
    {
        if (TestMediaDriver.shouldRunCMediaDriver())
        {
            outputFilesByAeronDirectoryName.forEach(
                (aeronDirectoryName, files) ->
                {
                    IoUtil.delete(files.stdout, false);
                    IoUtil.delete(files.stderr, false);
                });
        }
    }

    public void outputFiles(final String aeronDirectoryName, final File stdoutFile, final File stderrFile)
    {
        outputFilesByAeronDirectoryName.put(aeronDirectoryName, new StdOutputFiles(stdoutFile, stderrFile));
    }

    static final class StdOutputFiles
    {
        private final File stderr;
        private final File stdout;

        StdOutputFiles(final File stdout, final File stderr)
        {
            this.stderr = stderr;
            this.stdout = stdout;
        }
    }
}
