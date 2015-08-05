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

import uk.co.real_logic.aeron.driver.cmd.SenderCmd;
import uk.co.real_logic.aeron.driver.media.SendChannelEndpoint;
import uk.co.real_logic.aeron.driver.media.UdpTransportPoller;
import uk.co.real_logic.agrona.collections.ArrayUtil;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

/**
 * Agent that iterates over publications for sending them to registered subscribers.
 */
public class Sender implements Agent, Consumer<SenderCmd>
{
    private static final NetworkPublication[] EMPTY_PUBLICATIONS = new NetworkPublication[0];

    private final UdpTransportPoller transportPoller;
    private final OneToOneConcurrentArrayQueue<SenderCmd> commandQueue;
    private final DriverConductorProxy conductorProxy;
    private final AtomicCounter totalBytesSent;
    private final NanoClock nanoClock;

    private NetworkPublication[] publications = EMPTY_PUBLICATIONS;
    private int roundRobinIndex = 0;

    public Sender(final MediaDriver.Context ctx)
    {
        this.transportPoller = ctx.senderTransportPoller();
        this.commandQueue = ctx.senderCommandQueue();
        this.conductorProxy = ctx.fromSenderDriverConductorProxy();
        this.totalBytesSent = ctx.systemCounters().bytesSent();
        this.nanoClock = ctx.nanoClock();
    }

    public int doWork()
    {
        final long now = nanoClock.nanoTime();
        final int workCount = commandQueue.drain(this);
        final int bytesSent = doSend(now);
        final int bytesReceived = transportPoller.pollTransports();

        return workCount + bytesSent + bytesReceived;
    }

    public String roleName()
    {
        return "sender";
    }

    public void onRegisterSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        channelEndpoint.openChannel();
        channelEndpoint.registerForRead(transportPoller);
        transportPoller.selectNowWithoutProcessing();
    }

    public void onCloseSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        channelEndpoint.close();
        transportPoller.selectNowWithoutProcessing();
    }

    public void onNewPublication(final NetworkPublication publication)
    {
        publications = ArrayUtil.add(publications, publication);
        publication.sendChannelEndpoint().registerForSend(publication);
    }

    public void onRemovePublication(final NetworkPublication publication)
    {
        publications = ArrayUtil.remove(publications, publication);
        publication.sendChannelEndpoint().unregisterForSend(publication);
        conductorProxy.closeResource(publication);
    }

    public void accept(final SenderCmd cmd)
    {
        cmd.execute(this);
    }

    private int doSend(final long now)
    {
        int bytesSent = 0;
        final NetworkPublication[] publications = this.publications;
        final int length = publications.length;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        for (int i = startingIndex; i < length; i++)
        {
            bytesSent += publications[i].send(now);
        }

        for (int i = 0; i < startingIndex; i++)
        {
            bytesSent += publications[i].send(now);
        }

        totalBytesSent.addOrdered(bytesSent);

        return bytesSent;
    }
}
