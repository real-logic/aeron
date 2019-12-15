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
package io.aeron.archive;

import java.io.File;
import java.util.Scanner;

import static io.aeron.archive.ArchiveTool.*;

/**
 * Tool for inspecting and performing administrative tasks on an {@link Archive} and its contents which is described in
 * the {@link Catalog}.
 */
public class CatalogTool
{
    @SuppressWarnings("MethodLength")
    public static void main(final String[] args)
    {
        if (args.length == 0 || args.length > 3)
        {
            printHelp();
            System.exit(-1);
        }

        final File archiveDir = new File(args[0]);
        if (!archiveDir.exists())
        {
            System.err.println("ERR: Archive folder not found: " + archiveDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        if (args.length == 2 && args[1].equals("describe"))
        {
            describe(System.out, archiveDir);
        }
        else if (args.length == 3 && args[1].equals("describe"))
        {
            describeRecording(System.out, archiveDir, Long.parseLong(args[2]));
        }
        else if (args.length >= 2 && args[1].equals("dump"))
        {
            dump(System.out, archiveDir, args.length >= 3 ? Long.parseLong(args[2]) : Long.MAX_VALUE,
                CatalogTool::continueOnFrameLimit);
        }
        else if (args.length == 2 && args[1].equals("errors"))
        {
            printErrors(System.out, archiveDir);
        }
        else if (args.length == 2 && args[1].equals("pid"))
        {
            System.out.println(pid(archiveDir));
        }
        else if (args.length >= 2 && args[1].equals("verify"))
        {
            if (args.length == 2)
            {

                verify(System.out, archiveDir, false, CatalogTool::truncateFileOnPageStraddle);
            }
            else if (args.length == 3)
            {
                if (verifyAllSegmentFiles(args[2]))
                {
                    verify(System.out, archiveDir, true, CatalogTool::truncateFileOnPageStraddle);
                }
                else
                {
                    verifyRecording(System.out, archiveDir, Long.parseLong(args[2]), false,
                        CatalogTool::truncateFileOnPageStraddle);
                }
            }
            else if (args.length == 4)
            {
                verifyRecording(System.out, archiveDir, Long.parseLong(args[2]), verifyAllSegmentFiles(args[3]),
                    CatalogTool::truncateFileOnPageStraddle);
            }
        }
        else if (args.length == 2 && args[1].equals("count-entries"))
        {
            System.out.println(countEntries(archiveDir));
        }
        else if (args.length == 2 && args[1].equals("max-entries"))
        {
            System.out.println(maxEntries(archiveDir));
        }
        else if (args.length == 3 && args[1].equals("max-entries"))
        {
            System.out.println(maxEntries(archiveDir, Long.parseLong(args[2])));
        }
        else if (args.length == 2 && args[1].equals("migrate"))
        {
            System.out.print("WARNING: please ensure archive is not running and that backups have been taken of " +
                "archive directory before attempting migration(s).");
            if (readContinueAnswer("Continue? (y/n)"))
            {
                migrate(System.out, archiveDir);
            }
        }
    }

    private static boolean verifyAllSegmentFiles(final String arg)
    {
        return "-a".equals(arg);
    }

    private static boolean truncateFileOnPageStraddle(final File maxSegmentFile)
    {
        return readContinueAnswer(String.format("Last fragment in the segment file: %s straddles the page boundary,%n" +
                "i.e. it is not possible to verify if it was written correctly.%n%n" +
                "Please choose the corrective action: (y) - to truncate the file and " +
                "(n) - to do nothing",
            maxSegmentFile.getAbsolutePath()));
    }

    private static boolean continueOnFrameLimit(final Long frameLimit)
    {
        return readContinueAnswer(String.format("Specified frame limit %d reached. Continue? (y/n)", frameLimit));
    }

    private static boolean readContinueAnswer(final String msg)
    {
        System.out.printf("%n" + msg + ": ");
        final String answer = new Scanner(System.in).nextLine();

        return answer.isEmpty() || answer.equalsIgnoreCase("y") || answer.equalsIgnoreCase("yes");
    }

    private static void printHelp()
    {
        System.out.println("Usage: <archive-dir> <command>");
        System.out.println("  describe <optional recordingId>: prints out descriptor(s) in the catalog.");
        System.out.println("  dump <optional data fragment limit per recording>: prints descriptor(s)");
        System.out.println("     in the catalog and associated recorded data.");
        System.out.println("  errors: prints errors for the archive and media driver.");
        System.out.println("  pid: prints just PID of archive.");
        System.out.println("  verify <optional recordingId> <optional '-a'>: verifies descriptor(s) in the catalog");
        System.out.println("     checking recording files availability and contents. Only the last segment file is");
        System.out.println("     checked unless flag 'all' is specified, i.e. meaning check all files.");
        System.out.println("     Faulty entries are marked as unusable.");
        System.out.println("  count-entries: queries the number of recording entries in the catalog.");
        System.out.println("  max-entries <optional number of entries>: gets or increases the maximum number of");
        System.out.println("     recording entries the catalog can store.");
        System.out.println("  migrate: migrate previous archive MarkFile, Catalog, and recordings from previous");
        System.out.println("     to the latest version.");
    }
}
