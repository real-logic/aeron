package io.aeron.samples.echo;

import io.aeron.bindings.agent.api.EchoMonitorMBean;
import io.aeron.bindings.agent.api.ProvisioningConstants;
import io.aeron.bindings.agent.api.ProvisioningMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

public class ProvisioningClientMain
{
    public static void main(String[] args) throws IOException, MalformedObjectNameException
    {
        final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:10000/jmxrmi");
        final JMXConnector connector = JMXConnectorFactory.connect(url);
        final MBeanServerConnection mBeanServerConnection = connector.getMBeanServerConnection();

        final ProvisioningMBean provisioningMBean = JMX.newMBeanProxy(
            mBeanServerConnection,
            new ObjectName(ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING),
            ProvisioningMBean.class);

        final long correlationId = 1L;
        provisioningMBean.createEchoPair(correlationId, "aeron:udp?endpoint=localhost:9001", 1001, "aeron:udp?endpoint=localhost:9002", 1001);

        final EchoMonitorMBean echoMonitorMBean = JMX.newMBeanProxy(
            mBeanServerConnection,
            new ObjectName(ProvisioningConstants.echoPairObjectName(correlationId)),
            EchoMonitorMBean.class);

        System.out.println(echoMonitorMBean.getBackPressureCount());


//        System.out.println(registrationId);
    }
}
