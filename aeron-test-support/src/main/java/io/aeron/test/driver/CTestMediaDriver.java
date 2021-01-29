/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.test.driver;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.*;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

public final class CTestMediaDriver implements TestMediaDriver
{
    private static final File NULL_FILE = SystemUtil.isWindows() ? new File("NUL") : new File("/dev/null");
    private static final Map<Class<? extends FlowControlSupplier>, String> C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE =
        new IdentityHashMap<>();
    private static final ThreadLocal<Map<MediaDriver.Context, Map<String, String>>> C_DRIVER_ADDITIONAL_ENV_VARS =
        ThreadLocal.withInitial(IdentityHashMap::new);

    static
    {
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            DefaultMulticastFlowControlSupplier.class, "aeron_max_multicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            MaxMulticastFlowControlSupplier.class, "aeron_max_multicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            MinMulticastFlowControlSupplier.class, "aeron_min_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            DefaultUnicastFlowControlSupplier.class, "aeron_unicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            TaggedMulticastFlowControlSupplier.class, "aeron_tagged_flow_control_strategy_supplier");
    }

    private final Process aeronMediaDriverProcess;
    private final MediaDriver.Context context;
    private final DriverOutputConsumer driverOutputConsumer;

    private CTestMediaDriver(
        final Process aeronMediaDriverProcess,
        final MediaDriver.Context context,
        final DriverOutputConsumer driverOutputConsumer)
    {
        this.aeronMediaDriverProcess = aeronMediaDriverProcess;
        this.context = context;
        this.driverOutputConsumer = driverOutputConsumer;
    }

    public void close()
    {
        try
        {
            terminateDriver();
            if (!aeronMediaDriverProcess.waitFor(10, TimeUnit.SECONDS))
            {
                aeronMediaDriverProcess.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                throw new RuntimeException("Failed to shutdown cleanly, forced close");
            }

            if (null != driverOutputConsumer)
            {
                driverOutputConsumer.exitCode(context.aeronDirectoryName(), aeronMediaDriverProcess.exitValue());
            }
        }
        catch (final InterruptedException ex)
        {
            throw new RuntimeException("Interrupted while waiting for shutdown", ex);
        }
    }

    public CountersReader counters()
    {
        final Aeron.Context context = new Aeron.Context()
            .aeronDirectoryName(this.context.aeronDirectoryName()).conclude();
        return new CountersReader(context.countersMetaDataBuffer(), context.countersValuesBuffer());
    }

    private void terminateDriver()
    {
        CommonContext.requestDriverTermination(new File(context.aeronDirectoryName()), null, 0, 0);
    }

    public static CTestMediaDriver launch(
        final MediaDriver.Context context, final DriverOutputConsumer driverOutputConsumer)
    {
        final String aeronDirPath = System.getProperty(TestMediaDriver.AERONMD_PATH_PROP_NAME);
        final File aeronBinary = new File(aeronDirPath);

        if (!aeronBinary.exists())
        {
            throw new RuntimeException("Unable to find native media driver binary: " + aeronBinary.getAbsolutePath());
        }

        context.concludeAeronDirectory();
        IoUtil.ensureDirectoryExists(context.aeronDirectory().getParentFile(), "Aeron C Media Driver directory");

        final HashMap<String, String> environment = new HashMap<>();

        environment.put("AERON_CLIENT_LIVENESS_TIMEOUT", String.valueOf(context.clientLivenessTimeoutNs()));
        environment.put("AERON_IMAGE_LIVENESS_TIMEOUT", String.valueOf(context.imageLivenessTimeoutNs()));
        environment.put("AERON_DIR", context.aeronDirectoryName());
        environment.put("AERON_DRIVER_TERMINATION_VALIDATOR", "allow");
        environment.put("AERON_DIR_DELETE_ON_START", Boolean.toString(context.dirDeleteOnStart()));
        environment.put("AERON_DIR_DELETE_ON_SHUTDOWN", Boolean.toString(context.dirDeleteOnShutdown()));
        environment.put("AERON_TERM_BUFFER_SPARSE_FILE", Boolean.toString(context.termBufferSparseFile()));
        environment.put("AERON_TERM_BUFFER_LENGTH", String.valueOf(context.publicationTermBufferLength()));
        environment.put("AERON_IPC_TERM_BUFFER_LENGTH", String.valueOf(context.ipcTermBufferLength()));
        environment.put(
            "AERON_PUBLICATION_UNBLOCK_TIMEOUT", String.valueOf(context.publicationUnblockTimeoutNs()));
        environment.put(
            "AERON_PUBLICATION_CONNECTION_TIMEOUT", String.valueOf(context.publicationConnectionTimeoutNs()));
        environment.put("AERON_SPIES_SIMULATE_CONNECTION", Boolean.toString(context.spiesSimulateConnection()));
        environment.put("AERON_PERFORM_STORAGE_CHECKS", Boolean.toString(context.performStorageChecks()));
        if (null != context.threadingMode())
        {
            environment.put("AERON_THREADING_MODE", context.threadingMode().name());
        }
        environment.put("AERON_TIMER_INTERVAL", String.valueOf(context.timerIntervalNs()));
        environment.put("AERON_UNTETHERED_RESTING_TIMEOUT", String.valueOf(context.untetheredRestingTimeoutNs()));
        environment.put(
            "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT", String.valueOf(context.untetheredWindowLimitTimeoutNs()));

        if (null != context.receiverGroupTag())
        {
            environment.put("AERON_RECEIVER_GROUP_TAG", context.receiverGroupTag().toString());
        }
        environment.put("AERON_FLOW_CONTROL_GROUP_TAG", String.valueOf(context.flowControlGroupTag()));
        environment.put(
            "AERON_FLOW_CONTROL_GROUP_MIN_SIZE", String.valueOf(context.flowControlGroupMinSize()));
        environment.put("AERON_PRINT_CONFIGURATION", Boolean.toString(context.printConfigurationOnStart()));

        if (null != context.resolverName())
        {
            environment.put("AERON_DRIVER_RESOLVER_NAME", context.resolverName());
        }
        if (null != context.resolverInterface())
        {
            environment.put("AERON_DRIVER_RESOLVER_INTERFACE", context.resolverInterface());
            environment.put("AERON_NAME_RESOLVER_SUPPLIER", "driver");
        }
        if (null != context.resolverBootstrapNeighbor())
        {
            environment.put("AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR", context.resolverBootstrapNeighbor());
        }

        setFlowControlStrategy(environment, context);
        setLogging(environment);
        C_DRIVER_ADDITIONAL_ENV_VARS.get().getOrDefault(context, emptyMap()).forEach(environment::put);

        try
        {
            File stdoutFile = NULL_FILE;
            File stderrFile = NULL_FILE;

            if (null != driverOutputConsumer)
            {
                stdoutFile = File.createTempFile("CTestMediaDriver-", ".out");
                final String tmpName = stdoutFile.getName().substring(0, stdoutFile.getName().length() - 4) + ".err";
                stderrFile = new File(stdoutFile.getParent(), tmpName);
                driverOutputConsumer.outputFiles(context.aeronDirectoryName(), stdoutFile, stderrFile);
                driverOutputConsumer.environmentVariables(context.aeronDirectoryName(), environment);
            }

            final ProcessBuilder pb = new ProcessBuilder(aeronBinary.getAbsolutePath());
            pb.environment().putAll(environment);
            pb.redirectOutput(stdoutFile).redirectError(stderrFile);
            final Process process = pb.start();
            Thread.yield();

            return new CTestMediaDriver(process, context, driverOutputConsumer);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return null;
        }
    }

    private static void setLogging(final Map<String, String> environment)
    {
        environment.put("AERON_EVENT_LOG", "admin");

        final String driverAgentPath = System.getProperty(DRIVER_AGENT_PATH_PROP_NAME);
        if (null == driverAgentPath)
        {
            return;
        }

        final File driverAgent = new File(driverAgentPath);
        if (!driverAgent.exists())
        {
            throw new RuntimeException(
                "Unable to find driver agent file at: " + DRIVER_AGENT_PATH_PROP_NAME + "=" + driverAgentPath);
        }

        environment.put("LD_PRELOAD", driverAgent.getAbsolutePath());
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
                multicastFlowControlSupplier.getClass().getName());
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
                multicastFlowControlSupplier.getClass().getName());
        }
    }

    private static String getFlowControlStrategyName(final FlowControlSupplier flowControlSupplier)
    {
        return null == flowControlSupplier ?
            null : C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.get(flowControlSupplier.getClass());
    }

    public MediaDriver.Context context()
    {
        return context;
    }

    public String aeronDirectoryName()
    {
        return context.aeronDirectoryName();
    }

    public AgentInvoker sharedAgentInvoker()
    {
        throw new UnsupportedOperationException("Not supported in C media driver");
    }

    public static void enableLossGenerationOnReceive(
        final MediaDriver.Context context,
        final double rate,
        final long seed,
        final boolean loseDataMessages,
        final boolean loseControlMessages)
    {
        int receiveMessageTypeMask = 0;
        receiveMessageTypeMask |= loseDataMessages ? 1 << HeaderFlyweight.HDR_TYPE_DATA : 0;
        receiveMessageTypeMask |= loseControlMessages ? 1 << HeaderFlyweight.HDR_TYPE_SM : 0;
        receiveMessageTypeMask |= loseControlMessages ? 1 << HeaderFlyweight.HDR_TYPE_NAK : 0;
        receiveMessageTypeMask |= loseControlMessages ? 1 << HeaderFlyweight.HDR_TYPE_RTTM : 0;

        final Object2ObjectHashMap<String, String> lossTransportEnv = new Object2ObjectHashMap<>();

        final String interceptor = "loss";
        final String lossArgs = "rate=" + rate +
            "|seed=" + seed +
            "|recv-msg-mask=0x" + Integer.toHexString(receiveMessageTypeMask);

        lossTransportEnv.put("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", interceptor);
        lossTransportEnv.put("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", lossArgs);

        // This is a bit of an ugly hack to decorate the MediaDriver.Context with additional information.
        C_DRIVER_ADDITIONAL_ENV_VARS.get().put(context, lossTransportEnv);
    }

    public static void enableCsvNameLookupConfiguration(final MediaDriver.Context context, final String csvLookupTable)
    {
        final Object2ObjectHashMap<String, String> csvTableEnv = new Object2ObjectHashMap<>();
        csvTableEnv.put("AERON_NAME_RESOLVER_SUPPLIER", "csv_table");
        csvTableEnv.put("AERON_NAME_RESOLVER_INIT_ARGS", csvLookupTable);
        C_DRIVER_ADDITIONAL_ENV_VARS.get().put(context, csvTableEnv);
    }
}
