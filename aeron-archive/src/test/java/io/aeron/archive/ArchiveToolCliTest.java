/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.driver.MediaDriver;
import io.aeron.test.TestContexts;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.util.Arrays;

import static io.aeron.archive.Catalog.PAGE_SIZE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.NULL_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArchiveToolCliTest
{
    private static final int MTU_LENGTH = PAGE_SIZE * 4;
    private static final int TERM_LENGTH = MTU_LENGTH * 8;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 4;

    private File archiveDir;
    private long currentTimeMillis = 0;
    private final EpochClock epochClock = () -> currentTimeMillis += 100;
    private ArchivingMediaDriver mediaDriver;

    @BeforeEach
    void before(
        @TempDir final File aeronDir,
        @TempDir final File markFileDir,
        @TempDir final File archiveDir)
    {
        this.archiveDir = archiveDir;

        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
            catalog.addNewRecording(0, NULL_POSITION, 15, NULL_TIMESTAMP, 0,
                SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 2, 2, "ch2", "ch2?tag=OK", "src2");
        }

        final MediaDriver.Context driverContext = new MediaDriver
            .Context()
            .aeronDirectoryName(aeronDir.getAbsolutePath());

        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverContext.aeronDirectoryName())
            .archiveDir(archiveDir)
            .markFileDir(markFileDir);

        mediaDriver = ArchivingMediaDriver.launch(driverContext, archiveContext);
    }

    @AfterEach
    void after()
    {
        CloseHelper.close(mediaDriver);
    }

    @Test
    void describeRecordingShouldDescribeExistingRecording()
    {
        final OutputConsole console = runArchiveTool("describe", "1");
        assertTrue(console.systemOutText().contains("|recordingId=1|"));
    }

    @Test
    void describeRecordingShouldNotDescribeUnknownRecording()
    {
        final OutputConsole console = runArchiveTool("describe", "10");
        assertFalse(console.systemOutText().contains("|recordingId="));
    }

    @Test
    void describeRecordingsShouldShowAllRecordings()
    {
        final OutputConsole console = runArchiveTool("describe");

        final String consoleText = console.systemOutText();
        assertTrue(consoleText.contains("|recordingId=0|"));
        assertTrue(consoleText.contains("|recordingId=1|"));
        assertTrue(consoleText.contains("|recordingId=2|"));
        assertTrue(consoleText.contains("|recordingId=3|"));
    }

    @Test
    void describeRecordingsShouldNotShowInvalidatedRecordings()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            catalog.invalidateRecording(1);
            catalog.invalidateRecording(3);
        }

        final OutputConsole console = runArchiveTool("describe");

        final String consoleText = console.systemOutText();
        assertTrue(consoleText.contains("|recordingId=0|"));
        assertFalse(consoleText.contains("|recordingId=1|"));
        assertTrue(consoleText.contains("|recordingId=2|"));
        assertFalse(consoleText.contains("|recordingId=3|"));
    }

    @Test
    void describeAllRecordingsShouldShowAllRecordings()
    {
        final OutputConsole console = runArchiveTool("describe-all");

        final String consoleText = console.systemOutText();
        assertTrue(consoleText.contains("|recordingId=0|"));
        assertTrue(consoleText.contains("|recordingId=1|"));
        assertTrue(consoleText.contains("|recordingId=2|"));
        assertTrue(consoleText.contains("|recordingId=3|"));
    }

    @Test
    void describeAllRecordingsShouldAlsoShowInvalidatedRecordings()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, 1024, epochClock, null, null))
        {
            catalog.invalidateRecording(1);
            catalog.invalidateRecording(3);
        }

        final OutputConsole console = runArchiveTool("describe-all");

        assertTrue(outputMatches(console, ".*?\\|recordingId=0\\|.*?\\|VALID"));
        assertTrue(outputMatches(console, ".*?\\|recordingId=1\\|.*?\\|INVALID"));
        assertTrue(outputMatches(console, ".*?\\|recordingId=2\\|.*?\\|VALID"));
        assertTrue(outputMatches(console, ".*?\\|recordingId=3\\|.*?\\|INVALID"));
    }

    private OutputConsole runArchiveTool(final String... args)
    {
        final PrintStream originalSystemOut = System.out;
        try
        {
            final OutputConsole outputConsole = new OutputConsole();
            System.setOut(outputConsole.systemOut());

            final String[] mainArgs = new String[args.length + 1];
            mainArgs[0] = archiveDir.getAbsolutePath();
            System.arraycopy(args, 0, mainArgs, 1, args.length);

            ArchiveTool.main(mainArgs);
            return outputConsole;
        }
        finally
        {
            System.setOut(originalSystemOut);
        }
    }


    private static boolean outputMatches(final OutputConsole console, final String expected)
    {
        return Arrays.stream(console.systemOutText().split(System.lineSeparator()))
            .filter(s -> s.matches(expected))
            .count() == 1;
    }

    private static final class OutputConsole
    {
        private final OutputStream outputBytes = new ByteArrayOutputStream();
        private final PrintStream sysOut = new PrintStream(outputBytes);

        public PrintStream systemOut()
        {
            return sysOut;
        }

        public String systemOutText()
        {
            return outputBytes.toString();
        }
    }
}
