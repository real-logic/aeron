package io.aeron.support;

import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NativeTestMediaDriver implements TestMediaDriver
{
    private final Process aeronmdProcess;
    private final MediaDriver.Context context;

    private NativeTestMediaDriver(Process aeronmdProcess, MediaDriver.Context context)
    {
        this.aeronmdProcess = aeronmdProcess;
        this.context = context;
    }

    @Override
    public void close() throws Exception
    {
        aeronmdProcess.destroy();
        if (!aeronmdProcess.waitFor(10, TimeUnit.SECONDS))
        {
            aeronmdProcess.destroyForcibly();
            throw new Exception("Failed to shutdown cleaning, forcing close");
        }

        if (context.dirDeleteOnShutdown())
        {
            final File aeronDirectory = new File(context.aeronDirectoryName());
            IoUtil.delete(aeronDirectory, false);
        }
    }

    public static NativeTestMediaDriver launch(MediaDriver.Context context)
    {
        final String aeronmdPath = System.getProperty(TestMediaDriver.AERON_TEST_SYSTEM_AERONMD_PATH);
        final File f = new File(aeronmdPath);

        if (!f.exists())
        {
            throw new RuntimeException("Unable to find native media driver binary: " + f.getAbsolutePath());
        }

        IoUtil.ensureDirectoryExists(
            new File(context.aeronDirectoryName()).getParentFile(), "Aeron C Media Driver directory");

        final ProcessBuilder pb = new ProcessBuilder(f.getAbsolutePath())
            .redirectOutput(new File("/tmp/out.txt"))
            .redirectError(new File("/tmp/err.txt"));

        pb.environment().put("AERON_TERM_BUFFER_LENGTH", String.valueOf(context.publicationTermBufferLength()));
        pb.environment().put("AERON_THREADING_MODE", context.threadingMode().name());
        pb.environment().put("AERON_DIR", context.aeronDirectoryName());
        pb.environment().put("AERON_TIMER_INTERVAL", String.valueOf(context.timerIntervalNs()));

        try
        {
            System.out.println("Launching C driver: " + f.getAbsolutePath());
            return new NativeTestMediaDriver(pb.start(), context);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
