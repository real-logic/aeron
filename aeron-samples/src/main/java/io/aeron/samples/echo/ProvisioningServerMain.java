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
package io.aeron.samples.echo;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

import javax.management.*;
import java.lang.management.ManagementFactory;

import static java.util.Objects.requireNonNull;

/**
 * Main class for starting the provisioning service.
 */
public final class ProvisioningServerMain implements Agent, AutoCloseable
{
    private final MediaDriver driver;
    private final Aeron aeron;
    private final Provisioning provisioning;
    private final AgentRunner runner;
    private volatile ObjectName beanName = null;

    private ProvisioningServerMain(final MediaDriver driver, final Aeron aeron)
    {
        this.driver = driver;
        this.aeron = requireNonNull(aeron);
        this.provisioning = new Provisioning(aeron);
        runner = new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null, this);
    }

    /**
     * Entry point for starting the provisioning service.
     *
     * @param args command line arguments.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        try (ProvisioningServerMain ignore = ProvisioningServerMain.launch(new Aeron.Context()))
        {
            barrier.await();
            System.out.println("Shutdown Provisioning Server...");
        }
    }

    /**
     * Launch the provisioning server.
     *
     * @param context Aeron client context to connect to the local media driver.
     * @return new ProvisionServerMain instance.
     */
    public static ProvisioningServerMain launch(final Aeron.Context context)
    {
        MediaDriver driver = null;
        if (null == System.getProperty(CommonContext.AERON_DIR_PROP_NAME))
        {
            driver = MediaDriver.launchEmbedded();
            context.aeronDirectoryName(driver.aeronDirectoryName());
        }
        final Aeron aeron = Aeron.connect(context);
        final ProvisioningServerMain provisioningServerMain = new ProvisioningServerMain(driver, aeron);

        AgentRunner.startOnThread(provisioningServerMain.runner);

        return provisioningServerMain;
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        try
        {
            this.beanName = new ObjectName(ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING);
            final StandardMBean object = new StandardMBean(provisioning, ProvisioningMBean.class);
            ManagementFactory.getPlatformMBeanServer().registerMBean(object, beanName);
        }
        catch (final InstanceAlreadyExistsException |
            MBeanRegistrationException |
            NotCompliantMBeanException |
            MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        return provisioning.doWork();
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "EchoProvisioningServer";
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (null != beanName)
        {
            try
            {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(beanName);
            }
            catch (final InstanceNotFoundException | MBeanRegistrationException ignore)
            {
            }
        }

        CloseHelper.quietCloseAll(runner, aeron, driver);
    }
}
