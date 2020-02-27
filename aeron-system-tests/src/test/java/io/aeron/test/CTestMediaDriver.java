/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.CommonContext;
import io.aeron.driver.*;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.collections.Object2ObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

public final class CTestMediaDriver implements TestMediaDriver
{
    private static final File NULL_FILE = System.getProperty("os.name").startsWith("Windows") ?
        new File("NUL") : new File("/dev/null");
    private static final Map<Class<?>, String> C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE =
        new IdentityHashMap<>();
    private static final ThreadLocal<Map<MediaDriver.Context, Map<String, String>>> TRANSPORT_BINDINGS_CONFIGURATION =
        ThreadLocal.withInitial(IdentityHashMap::new);

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
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.put(
            TaggedMulticastFlowControlSupplier.class, "aeron_tagged_flow_control_strategy_supplier");
    }

    private final Process aeronMediaDriverProcess;
    private final MediaDriver.Context context;

    private CTestMediaDriver(final Process aeronMediaDriverProcess, final MediaDriver.Context context)
    {
        this.aeronMediaDriverProcess = aeronMediaDriverProcess;
        this.context = context;
    }

    @Override
    public void close()
    {
        terminateDriver();
        try
        {
            if (!aeronMediaDriverProcess.waitFor(10, TimeUnit.SECONDS))
            {
                aeronMediaDriverProcess.destroyForcibly();
                throw new RuntimeException("Failed to shutdown cleaning, forcing close");
            }
        }
        catch (final InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting for shutdown", e);
        }

        final File aeronDirectory = new File(context.aeronDirectoryName());
        IoUtil.delete(aeronDirectory, false);
    }

    private void terminateDriver()
    {
        CommonContext.requestDriverTermination(new File(context.aeronDirectoryName()), null, 0, 0);
    }

    public static CTestMediaDriver launch(
        final MediaDriver.Context context,
        final DriverOutputConsumer driverOutputConsumer)
    {
        final String aeronDirPath = System.getProperty(TestMediaDriver.AERONMD_PATH_PROP_NAME);
        final File f = new File(aeronDirPath);

        if (!f.exists())
        {
            throw new RuntimeException("Unable to find native media driver binary: " + f.getAbsolutePath());
        }

        IoUtil.ensureDirectoryExists(
            new File(context.aeronDirectoryName()).getParentFile(), "Aeron C Media Driver directory");

        final ProcessBuilder pb = new ProcessBuilder(f.getAbsolutePath());

        pb.environment().put("AERON_CLIENT_LIVENESS_TIMEOUT", String.valueOf(context.clientLivenessTimeoutNs()));
        pb.environment().put("AERON_IMAGE_LIVENESS_TIMEOUT", String.valueOf(context.imageLivenessTimeoutNs()));
        pb.environment().put("AERON_DIR", context.aeronDirectoryName());
        pb.environment().put("AERON_DRIVER_TERMINATION_VALIDATOR", "allow");
        pb.environment().put("AERON_TERM_BUFFER_LENGTH", String.valueOf(context.publicationTermBufferLength()));
        pb.environment().put(
            "AERON_PUBLICATION_UNBLOCK_TIMEOUT", String.valueOf(context.publicationUnblockTimeoutNs()));
        pb.environment().put(
            "AERON_PUBLICATION_CONNECTION_TIMEOUT", String.valueOf(context.publicationConnectionTimeoutNs()));
        pb.environment().put("AERON_SPIES_SIMULATE_CONNECTION", String.valueOf(context.spiesSimulateConnection()));
        if (null != context.threadingMode())
        {
            pb.environment().put("AERON_THREADING_MODE", context.threadingMode().name());
        }
        pb.environment().put("AERON_TIMER_INTERVAL", String.valueOf(context.timerIntervalNs()));
        pb.environment().put("AERON_UNTETHERED_RESTING_TIMEOUT", String.valueOf(context.untetheredRestingTimeoutNs()));
        pb.environment().put(
            "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT", String.valueOf(context.untetheredWindowLimitTimeoutNs()));
        if (null != context.receiverGroupTag())
        {
            pb.environment().put("AERON_RECEIVER_GROUP_TAG", context.receiverGroupTag().toString());
        }
        pb.environment().put("AERON_FLOW_CONTROL_GROUP_TAG", String.valueOf(context.flowControlGroupTag()));
        pb.environment().put(
            "AERON_FLOW_CONTROL_GROUP_MIN_SIZE", String.valueOf(context.flowControlGroupMinSize()));
//        pb.environment().put("AERON_PRINT_CONFIGURATION", "true");
        pb.environment().put("AERON_EVENT_LOG", "0xFFFF");

        setFlowControlStrategy(pb.environment(), context);
        TRANSPORT_BINDINGS_CONFIGURATION.get().getOrDefault(context, emptyMap()).forEach(pb.environment()::put);
        setLogging(pb.environment());

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
                final String tmpName = stdoutFile.getName().substring(0, stdoutFile.getName().length() - 4) + ".err";
                stderrFile = new File(stdoutFile.getParent(), tmpName);
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

    private static void setLogging(final Map<String, String> environment)
    {
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

        environment.put("AERON_EVENT_LOG", "0xFFFF");
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

    private static String getFlowControlStrategyName(final FlowControlSupplier flowControlSupplier)
    {
        return null == flowControlSupplier ?
            null : C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_SUPPLIER_TYPE.get(flowControlSupplier.getClass());
    }

    public MediaDriver.Context context()
    {
        return context;
    }

    public String aeronDirectoryName()
    {
        return context.aeronDirectoryName();
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
        TRANSPORT_BINDINGS_CONFIGURATION.get().put(context, lossTransportEnv);
    }
}
