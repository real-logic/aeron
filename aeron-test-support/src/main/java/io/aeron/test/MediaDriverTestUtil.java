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
package io.aeron.test;

import io.aeron.test.driver.TestMediaDriver;
import org.agrona.IoUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

class MediaDriverTestUtil
{
    private final Map<String, ProcessDetails> outputFilesByAeronDirectoryName = new LinkedHashMap<>();

    public void testFailed()
    {
        dumpMediaDriverDiagnostics();
    }

    private void dumpMediaDriverDiagnostics()
    {
        if (TestMediaDriver.shouldRunCMediaDriver())
        {
            outputFilesByAeronDirectoryName.forEach(
                (aeronDirectoryName, files) ->
                {
                    try
                    {
                        System.out.println();
                        System.out.println(
                            "Media Driver: " + aeronDirectoryName +
                            ", exit code: " + files.exitValue +
                            " (" + files.exitMessage + ")");
                        System.out.println();
                        Tests.printDirectoryContents(aeronDirectoryName, System.out);
                        System.out.println();
                        printEnvironment(files.environment, System.out);
                        System.out.println();
                        System.out.println("*** STDOUT ***");
                        Files.copy(files.stdout.toPath(), System.out);
                        System.out.println();
                        System.out.println("*** STDERR ***");
                        Files.copy(files.stderr.toPath(), System.out);
                        System.out.println("====");
                    }
                    catch (final IOException ex)
                    {
                        throw new RuntimeException("Failed to output logs to stdout", ex);
                    }
                });

            deleteFiles();
        }
    }

    private void printEnvironment(final Map<String, String> environment, final PrintStream out)
    {
        environment.forEach(
            (name, value) ->
            {
                out.print(name);
                out.print('=');
                out.print(value);
                out.println();
            });
    }

    public void testSuccessful()
    {
        if (TestMediaDriver.shouldRunCMediaDriver())
        {
            deleteFiles();
        }
    }

    public void afterTestExecution(final ExtensionContext context)
    {
        if (null == context.getRequiredTestMethod().getAnnotation(IgnoreStdErr.class))
        {
            verifyNoStdError();
        }
    }

    private void deleteFiles()
    {
        outputFilesByAeronDirectoryName.forEach(
            (aeronDirectoryName, files) ->
            {
                IoUtil.delete(files.stdout, true);
                IoUtil.delete(files.stderr, true);
            });
    }

    public void outputFiles(final String aeronDirectoryName, final File stdoutFile, final File stderrFile)
    {
        outputFilesByAeronDirectoryName.put(aeronDirectoryName, new ProcessDetails(stdoutFile, stderrFile));
    }

    public void exitCode(final String aeronDirectoryName, final int exitValue, final String exitMessage)
    {
        outputFilesByAeronDirectoryName.get(aeronDirectoryName).exitValue(exitValue, exitMessage);
    }

    public void environmentVariables(final String aeronDirectoryName, final Map<String, String> environment)
    {
        outputFilesByAeronDirectoryName.get(aeronDirectoryName).environment(environment);
    }

    public void verifyNoStdError()
    {
        outputFilesByAeronDirectoryName.values().forEach(
            processDetails -> Assertions.assertEquals(0, processDetails.stderr.length(), "stderr contains data"));
    }

    static final class ProcessDetails
    {
        private final File stderr;
        private final File stdout;
        private int exitValue;
        private String exitMessage;
        private Map<String, String> environment;

        ProcessDetails(final File stdout, final File stderr)
        {
            this.stderr = stderr;
            this.stdout = stdout;
        }

        public void exitValue(final int exitValue, final String exitMessage)
        {
            this.exitValue = exitValue;
            this.exitMessage = exitMessage;
        }

        public void environment(final Map<String, String> environment)
        {
            this.environment = environment;
        }
    }
}
