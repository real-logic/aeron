package io.aeron.samples.echo;

import io.aeron.Aeron;
import io.aeron.bindings.agent.api.ProvisioningConstants;
import io.aeron.bindings.agent.api.ProvisioningMBean;
import org.agrona.concurrent.BackoffIdleStrategy;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class ProvisioningServerMain
{
    public static void main(String[] args) throws MalformedObjectNameException, NotCompliantMBeanException,
        InstanceAlreadyExistsException, MBeanRegistrationException
    {
        try (Aeron aeron = Aeron.connect()) {
            System.out.println("Connected to Aeron");
            final Provisioning provisioning = new Provisioning(aeron);
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                new StandardMBean(provisioning, ProvisioningMBean.class),
                new ObjectName(ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING));

            BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();

            idleStrategy.reset();
            while (!Thread.currentThread().isInterrupted())
            {
                idleStrategy.idle(provisioning.doWork());
            }
        }
    }

}
