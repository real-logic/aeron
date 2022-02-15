/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.BackoffIdleStrategy;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * Main class for starting the provisioning service
 */
public final class ProvisioningServerMain implements Agent, AutoCloseable
{
    private final Aeron aeron;
    private final Provisioning provisioning;
    private volatile ObjectName beanName = null;

    private ProvisioningServerMain(final Aeron aeron)
    {
        this.aeron = aeron;
        this.provisioning = new Provisioning(aeron);
    }

    /**
     * Entry point for starting the provisioning service.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args)
    {
        try (ProvisioningServerMain provisioningServerMain = ProvisioningServerMain.launch(new Aeron.Context()))
        {
            System.out.println("Connected to Aeron");

            final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

            idleStrategy.reset();
            while (!Thread.currentThread().isInterrupted())
            {
                idleStrategy.idle(provisioningServerMain.doWork());
            }
        }
    }

    /**
     * Launch the provisioning server
     *
     * @param context Aeron client context to connect to the local media driver
     * @return new ProvisionServerMain instance
     */
    public static ProvisioningServerMain launch(final Aeron.Context context)
    {
        final Aeron aeron = Aeron.connect(context);
        return new ProvisioningServerMain(aeron);
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
        CloseHelper.quietClose(aeron);
    }
}
