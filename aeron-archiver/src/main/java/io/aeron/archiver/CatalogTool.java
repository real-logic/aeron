package io.aeron.archiver;

import java.io.File;

public class CatalogTool
{
    public static void main(final String[] args)
    {
        final String archiveDirPath = System.getProperty("io.aeron.archive.dir", "archive");
        final File archiveDir = new File(archiveDirPath);
        if (!archiveDir.exists())
        {
            System.err.println("ERR: Archive folder not found: " + archiveDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        if (args.length == 0 || args.length > 2)
        {
            printHelp();
            System.exit(-1);
        }

        if (args.length == 1 && args[0].equals("describe"))
        {
            try (Catalog catalog = new Catalog(archiveDir, null, 0))
            {
                catalog.forEach((e, d) -> System.out.println(d));
            }
        }
        else if (args.length == 2 && args[0].equals("describe"))
        {
            try (Catalog catalog = new Catalog(archiveDir, null, 0))
            {
                catalog.forEntry(Long.valueOf(args[1]), (e, d) -> System.out.println(d));
            }
        }
    }

    private static void printHelp()
    {
        System.out.println("Usage:");
        System.out.println("  describe: prints out all descriptors in the file. Optionally specify a recording id as" +
            " second argument to describe a single recording.");
        System.out.println("  verify: verifies all descriptors in the file, checking recording files availability %n" +
            "and contents. Faulty entries are marked as unusable. Optionally specify a recording id as second%n" +
            "argument to verify a single recording.");
        System.out.println("  reverify: verify a descriptor and, if successful, mark it as usable.");
    }
}
