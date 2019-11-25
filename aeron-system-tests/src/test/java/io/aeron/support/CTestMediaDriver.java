package io.aeron.support;

import io.aeron.CommonContext;
import io.aeron.driver.DefaultMulticastFlowControlSupplier;
import io.aeron.driver.DefaultUnicastFlowControlSupplier;
import io.aeron.driver.FlowControlSupplier;
import io.aeron.driver.MaxMulticastFlowControlSupplier;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class CTestMediaDriver implements TestMediaDriver
{
    private static final File NULL_FILE = System.getProperty("os.name").startsWith("Windows") ?
        new File("NUL") : new File("/dev/null");
    private static final Map<Class, String> C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE =
        new IdentityHashMap<>();

    static
    {
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.put(
            DefaultMulticastFlowControlSupplier.class, "aeron_max_multicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.put(
            MaxMulticastFlowControlSupplier.class, "aeron_max_multicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.put(
            MinMulticastFlowControlSupplier.class, "aeron_min_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.put(
            DefaultUnicastFlowControlSupplier.class, "aeron_unicast_flow_control_strategy_supplier");
    }

    private final Process aeronmdProcess;
    private final MediaDriver.Context context;

    private CTestMediaDriver(
        final Process aeronmdProcess,
        final MediaDriver.Context context)
    {
        this.aeronmdProcess = aeronmdProcess;
        this.context = context;
    }

    @Override
    public void close()
    {
        terminateDriver();
        try
        {
            if (!aeronmdProcess.waitFor(10, TimeUnit.SECONDS))
            {
                aeronmdProcess.destroyForcibly();
                throw new RuntimeException("Failed to shutdown cleaning, forcing close");
            }
        }
        catch (final InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting for shutdown", e);
        }

        if (context.dirDeleteOnShutdown())
        {
            final File aeronDirectory = new File(context.aeronDirectoryName());
            IoUtil.delete(aeronDirectory, false);
        }
    }

    private void terminateDriver()
    {
        CommonContext.requestDriverTermination(new File(context.aeronDirectoryName()), null, 0, 0);
    }

    public static CTestMediaDriver launch(
        final MediaDriver.Context context,
        final DriverOutputConsumer driverOutputConsumer)
    {
        final String aeronmdPath = System.getProperty(TestMediaDriver.AERON_TEST_SYSTEM_AERONMD_PATH);
        final File f = new File(aeronmdPath);

        if (!f.exists())
        {
            throw new RuntimeException("Unable to find native media driver binary: " + f.getAbsolutePath());
        }

        IoUtil.ensureDirectoryExists(
            new File(context.aeronDirectoryName()).getParentFile(), "Aeron C Media Driver directory");

        final ProcessBuilder pb = new ProcessBuilder(f.getAbsolutePath());

        pb.environment().put("AERON_CLIENT_LIVENESS_TIMEOUT", String.valueOf(context.clientLivenessTimeoutNs()));
        pb.environment().put("AERON_DIR", context.aeronDirectoryName());
        pb.environment().put("AERON_DRIVER_TERMINATION_VALIDATOR", "allow");
        pb.environment().put("AERON_TERM_BUFFER_LENGTH", String.valueOf(context.publicationTermBufferLength()));
        pb.environment().put(
            "AERON_PUBLICATION_UNBLOCK_TIMEOUT", String.valueOf(context.publicationUnblockTimeoutNs()));
        pb.environment().put("AERON_SPIES_SIMULATE_CONNECTION", String.valueOf(context.spiesSimulateConnection()));
        if (null != context.threadingMode())
        {
            pb.environment().put("AERON_THREADING_MODE", context.threadingMode().name());
        }
        pb.environment().put("AERON_TIMER_INTERVAL", String.valueOf(context.timerIntervalNs()));
        pb.environment().put("AERON_UNTETHERED_RESTING_TIMEOUT", String.valueOf(context.untetheredRestingTimeoutNs()));
        pb.environment().put(
            "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT", String.valueOf(context.untetheredWindowLimitTimeoutNs()));
        setFlowControlStrategy(pb.environment(), context);

        try
        {
            final File stdoutFile;
            final File stderrFile;

            if (null == driverOutputConsumer)
            {
                stdoutFile = NULL_FILE;
                stderrFile = NULL_FILE;
            }
            else
            {
                stdoutFile = File.createTempFile("CTestMediaDriver-", ".out");
                stderrFile = File.createTempFile("CTestMediaDriver-", ".err");
                driverOutputConsumer.outputFiles(context.aeronDirectoryName(), stdoutFile, stderrFile);
            }

            pb.redirectOutput(stdoutFile).redirectError(stderrFile);

            return new CTestMediaDriver(pb.start(), context);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void setFlowControlStrategy(final Map<String, String> environment, final MediaDriver.Context context)
    {
        final FlowControlSupplier multicastFlowControlSupplier = context.multicastFlowControlSupplier();
        final String multicastFlowControlStrategyName = getFlowControlStrategyName(multicastFlowControlSupplier);
        if (null != multicastFlowControlStrategyName)
        {
            environment.put("AERON_MULTICAST_FLOWCONTROL_SUPPLIER", multicastFlowControlStrategyName);
        }
        else if (null != multicastFlowControlSupplier)
        {
            throw new RuntimeException("No equivalent C multicast flow control strategy for: " +
                multicastFlowControlSupplier.getClass().getSimpleName());
        }

        final FlowControlSupplier unicastFlowControlSupplier = context.unicastFlowControlSupplier();
        final String unicastFlowControlStrategyName = getFlowControlStrategyName(unicastFlowControlSupplier);
        if (null != unicastFlowControlStrategyName)
        {
            environment.put("AERON_UNICAST_FLOWCONTROL_SUPPLIER", unicastFlowControlStrategyName);
        }
        else if (null != unicastFlowControlSupplier)
        {
            throw new RuntimeException("No equivalent C unicast flow control strategy for: " +
                multicastFlowControlSupplier.getClass().getSimpleName());
        }
    }

    private static String getFlowControlStrategyName(FlowControlSupplier multicastFlowControlSupplier)
    {
        return null == multicastFlowControlSupplier ?
            null : C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.get(multicastFlowControlSupplier.getClass());
    }

    @Override
    public MediaDriver.Context context()
    {
        return context;
    }

    @Override
    public String aeronDirectoryName()
    {
        return context.aeronDirectoryName();
    }
}
