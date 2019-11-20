package io.aeron.support;

import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NativeTestMediaDriver implements TestMediaDriver
{
    private static final String DEFAULT_AERONMD_LOCATION =
        System.getProperty("user.dir") + "/../cppbuild/Release/binaries/aeronmd";
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
        final String aeronmdPath = System.getProperty("aeron.test.system.aeronmd.path", DEFAULT_AERONMD_LOCATION);
        File f = new File(aeronmdPath);

        if (!f.exists())
        {
            throw new RuntimeException("Unable to find native media driver binary: " + f.getAbsolutePath());
        }

        final ProcessBuilder pb = new ProcessBuilder(f.getAbsolutePath())
//            .inheritIO();
            .redirectOutput(new File("/dev/null"))
            .redirectError(new File("/dev/null"));

//        final ProcessBuilder pb = new ProcessBuilder(f.getAbsolutePath())
//            .inheritIO();
//            .redirectOutput(new File("out.txt"))
//            .redirectError(new File("err.txt"));

        pb.environment().put("AERON_DIR_DELETE_ON_START", String.valueOf(context.dirDeleteOnShutdown()));
        pb.environment().put("AERON_TERM_BUFFER_LENGTH", String.valueOf(context.publicationTermBufferLength()));
        pb.environment().put("AERON_THREADING_MODE", context.threadingMode().name());
        pb.environment().put("AERON_DIR", context.aeronDirectoryName());

        try
        {
            return new NativeTestMediaDriver(pb.start(), context);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
