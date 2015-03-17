/*
 * Copyright 2014 - 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.CountersManager;

public class SystemCounters implements AutoCloseable
{
    private final AtomicCounter bytesSent;
    private final AtomicCounter bytesReceived;
    private final AtomicCounter receiverProxyFails;
    private final AtomicCounter senderProxyFails;
    private final AtomicCounter conductorProxyFails;
    private final AtomicCounter naksSent;
    private final AtomicCounter naksReceived;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter statusMessagesSent;
    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter heartbeatsReceived;
    private final AtomicCounter flowControlUnderRuns;
    private final AtomicCounter flowControlOverRuns;
    private final AtomicCounter invalidPackets;
    private final AtomicCounter driverExceptions;
    private final AtomicCounter dataFrameShortSends;
    private final AtomicCounter setupFrameShortSends;
    private final AtomicCounter nakFrameShortSends;
    private final AtomicCounter smFrameShortSends;
    private final AtomicCounter clientKeepAlives;
    private final AtomicCounter senderFlowControlLimits;

    public SystemCounters(final CountersManager countersManager)
    {
        bytesSent = countersManager.newCounter("Bytes sent");
        bytesReceived = countersManager.newCounter("Bytes received");
        receiverProxyFails = countersManager.newCounter("Failed offers to ReceiverProxy");
        senderProxyFails = countersManager.newCounter("Failed offers to SenderProxy");
        conductorProxyFails = countersManager.newCounter("Failed offers to DriverConductorProxy");
        naksSent = countersManager.newCounter("NAKs sent");
        naksReceived = countersManager.newCounter("NAKs received");
        statusMessagesSent = countersManager.newCounter("SMs sent");
        statusMessagesReceived = countersManager.newCounter("SMs received");
        heartbeatsSent = countersManager.newCounter("Heartbeats sent");
        heartbeatsReceived = countersManager.newCounter("Heartbeats received");
        retransmitsSent = countersManager.newCounter("Retransmits sent");
        flowControlUnderRuns = countersManager.newCounter("Flow control under runs");
        flowControlOverRuns = countersManager.newCounter("Flow control over runs");
        invalidPackets = countersManager.newCounter("Invalid packets");
        driverExceptions = countersManager.newCounter("Driver Exceptions");
        dataFrameShortSends = countersManager.newCounter("Data Frame short sends");
        setupFrameShortSends = countersManager.newCounter("Setup Frame short sends");
        nakFrameShortSends = countersManager.newCounter("NAK Frame short sends");
        smFrameShortSends = countersManager.newCounter("SM Frame short sends");
        clientKeepAlives = countersManager.newCounter("Client keep-alives");
        senderFlowControlLimits = countersManager.newCounter("Sender flow control limits applied");
    }

    public void close()
    {
        bytesSent.close();
        bytesReceived.close();
        receiverProxyFails.close();
        senderProxyFails.close();
        conductorProxyFails.close();
        naksSent.close();
        naksReceived.close();
        statusMessagesSent.close();
        statusMessagesReceived.close();
        heartbeatsSent.close();
        heartbeatsReceived.close();
        retransmitsSent.close();
        flowControlUnderRuns.close();
        flowControlOverRuns.close();
        invalidPackets.close();
        driverExceptions.close();
        dataFrameShortSends.close();
        setupFrameShortSends.close();
        nakFrameShortSends.close();
        smFrameShortSends.close();
        clientKeepAlives.close();
        senderFlowControlLimits.close();
    }

    public AtomicCounter bytesSent()
    {
        return bytesSent;
    }

    public AtomicCounter bytesReceived()
    {
        return bytesReceived;
    }

    public AtomicCounter receiverProxyFails()
    {
        return receiverProxyFails;
    }

    public AtomicCounter senderProxyFails()
    {
        return senderProxyFails;
    }

    public AtomicCounter conductorProxyFails()
    {
        return conductorProxyFails;
    }

    public AtomicCounter naksSent()
    {
        return naksSent;
    }

    public AtomicCounter naksReceived()
    {
        return naksReceived;
    }

    public AtomicCounter retransmitsSent()
    {
        return retransmitsSent;
    }

    public AtomicCounter statusMessagesSent()
    {
        return statusMessagesSent;
    }

    public AtomicCounter statusMessagesReceived()
    {
        return statusMessagesReceived;
    }

    public AtomicCounter heartbeatsSent()
    {
        return heartbeatsSent;
    }

    public AtomicCounter heartbeatsReceived()
    {
        return heartbeatsReceived;
    }

    public AtomicCounter flowControlUnderRuns()
    {
        return flowControlUnderRuns;
    }

    public AtomicCounter flowControlOverRuns()
    {
        return flowControlOverRuns;
    }

    public AtomicCounter invalidPackets()
    {
        return invalidPackets;
    }

    public AtomicCounter driverExceptions()
    {
        return driverExceptions;
    }

    public AtomicCounter dataFrameShortSends()
    {
        return dataFrameShortSends;
    }

    public AtomicCounter setupFrameShortSends()
    {
        return setupFrameShortSends;
    }

    public AtomicCounter nakFrameShortSends()
    {
        return nakFrameShortSends;
    }

    public AtomicCounter smFrameShortSends()
    {
        return smFrameShortSends;
    }

    public AtomicCounter clientKeepAlives()
    {
        return clientKeepAlives;
    }

    public AtomicCounter senderFlowControlLimits()
    {
        return senderFlowControlLimits;
    }
}
