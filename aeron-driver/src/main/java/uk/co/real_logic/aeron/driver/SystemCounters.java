/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.common.concurrent.CountersManager;

public class SystemCounters implements AutoCloseable
{
    private final AtomicCounter receiverProxyFails;
    private final AtomicCounter senderProxyFails;
    private final AtomicCounter naksSent;
    private final AtomicCounter naksReceived;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter statusMessagesSent;
    private final AtomicCounter statusMessagesReceived;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter flowControlUnderRuns;
    private final AtomicCounter flowControlOverRuns;
    private final AtomicCounter subscriptionCleaningLate;
    private final AtomicCounter invalidPackets;
    private final AtomicCounter driverExceptions;

    public SystemCounters(final CountersManager countersManager)
    {
        receiverProxyFails = countersManager.newCounter("Failed offers to ReceiverProxy");
        senderProxyFails = countersManager.newCounter("Failed offers to SenderProxy");
        naksSent = countersManager.newCounter("NAKs sent");
        naksReceived = countersManager.newCounter("NAKs received");
        statusMessagesSent = countersManager.newCounter("SMs sent");
        statusMessagesReceived = countersManager.newCounter("SMs received");
        heartbeatsSent = countersManager.newCounter("Heartbeats sent");
        retransmitsSent = countersManager.newCounter("Retransmits sent");
        flowControlUnderRuns = countersManager.newCounter("Flow control under runs");
        flowControlOverRuns = countersManager.newCounter("Flow control over runs");
        subscriptionCleaningLate = countersManager.newCounter("Term cleaning late");
        invalidPackets = countersManager.newCounter("Invalid packets");
        driverExceptions = countersManager.newCounter("Driver Exceptions");
    }

    public void close()
    {
        receiverProxyFails.close();
        senderProxyFails.close();
        naksSent.close();
        naksReceived.close();
        statusMessagesSent.close();
        statusMessagesReceived.close();
        heartbeatsSent.close();
        retransmitsSent.close();
        flowControlUnderRuns.close();
        flowControlOverRuns.close();
        subscriptionCleaningLate.close();
        invalidPackets.close();
        driverExceptions.close();
    }

    public AtomicCounter receiverProxyFails()
    {
        return receiverProxyFails;
    }

    public AtomicCounter senderProxyFails()
    {
        return senderProxyFails;
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

    public AtomicCounter flowControlUnderRuns()
    {
        return flowControlUnderRuns;
    }

    public AtomicCounter flowControlOverRuns()
    {
        return flowControlOverRuns;
    }

    public AtomicCounter subscriptionCleaningLate()
    {
        return subscriptionCleaningLate;
    }

    public AtomicCounter invalidPackets()
    {
        return invalidPackets;
    }

    public AtomicCounter driverExceptions()
    {
        return driverExceptions;
    }
}
