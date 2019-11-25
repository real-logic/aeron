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
package io.aeron.test;

import org.agrona.IoUtil;
import org.agrona.collections.Object2ObjectHashMap;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.util.Map;

public class MediaDriverTestWatcher extends TestWatcher implements DriverOutputConsumer
{
    private boolean hasFailed = false;
    private final Map<String, StdOutputFiles> outputFilesByAeronDirectoryName = new Object2ObjectHashMap<>();

    protected void failed(final Throwable e, final Description description)
    {
        hasFailed = true;
    }

    protected void finished(final Description description)
    {
        if (hasFailed && !outputFilesByAeronDirectoryName.isEmpty())
        {
            System.out.println("C Media Driver tests failed");
            outputFilesByAeronDirectoryName.forEach((aeronDirectoryName, files) ->
            {
                System.out.println("Media Driver: " + aeronDirectoryName);
                System.out.println("  stdout: " + files.stdout + ", stderr: " + files.stderr);
            });
        }
        else
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

    private static final class StdOutputFiles
    {
        private final File stderr;
        private final File stdout;

        private StdOutputFiles(final File stderr, final File stdout)
        {
            this.stderr = stderr;
            this.stdout = stdout;
        }
    }
}
