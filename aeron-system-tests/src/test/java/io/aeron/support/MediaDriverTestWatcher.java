package io.aeron.support;

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
