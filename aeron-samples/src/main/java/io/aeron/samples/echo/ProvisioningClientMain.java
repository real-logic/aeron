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

import io.aeron.samples.echo.api.EchoMonitorMBean;
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * Test client for the provisioning service.
 */
public class ProvisioningClientMain
{
    /**
     * Main entry point.
     *
     * @param args command line parameters
     * @throws IOException if an I/O exception occurs.
     * @throws MalformedObjectNameException if any of the JMX names are invalid.
     */
    public static void main(final String[] args) throws IOException, MalformedObjectNameException
    {
        final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:10000/jmxrmi");
        final JMXConnector connector = JMXConnectorFactory.connect(url);
        final MBeanServerConnection mBeanServerConnection = connector.getMBeanServerConnection();

        final ProvisioningMBean provisioningMBean = JMX.newMBeanProxy(
            mBeanServerConnection,
            new ObjectName(ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING),
            ProvisioningMBean.class);

        final long correlationId = 1L;
        provisioningMBean.createEchoPair(
            correlationId, "aeron:udp?endpoint=localhost:9001", 1001, "aeron:udp?endpoint=localhost:9002", 1001);

        final EchoMonitorMBean echoMonitorMBean = JMX.newMBeanProxy(
            mBeanServerConnection,
            new ObjectName(ProvisioningConstants.echoPairObjectName(correlationId)),
            EchoMonitorMBean.class);

        System.out.println(echoMonitorMBean.getBackPressureCount());
    }
}
