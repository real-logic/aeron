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
import uk.co.real_logic.aeron.driver.media.TransportPoller;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

/**
 * Agent that iterates over publications for sending them to registered subscribers.
 */
public class Sender implements Agent, Consumer<SenderCmd>
{
    private static final NetworkPublication[] EMPTY_PUBLICATIONS = new NetworkPublication[0];

    private final TransportPoller transportPoller;
    private final OneToOneConcurrentArrayQueue<SenderCmd> commandQueue;
    private final DriverConductorProxy conductorProxy;
    private final AtomicCounter totalBytesSent;

    private NetworkPublication[] publications = EMPTY_PUBLICATIONS;
    private int roundRobinIndex = 0;

    public Sender(final MediaDriver.Context ctx)
    {
        this.transportPoller = ctx.senderNioSelector();
        this.commandQueue = ctx.senderCommandQueue();
        this.conductorProxy = ctx.fromSenderDriverConductorProxy();
        this.totalBytesSent = ctx.systemCounters().bytesSent();
    }

    public int doWork()
    {
        final int workCount = commandQueue.drain(this);
        final int bytesSent = doSend();
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

    public void onNewPublication(
        final NetworkPublication publication,
        final RetransmitHandler retransmitHandler,
        final FlowControl flowControl)
    {
        final NetworkPublication[] oldPublications = publications;
        final int length = oldPublications.length;
        final NetworkPublication[] newPublications = new NetworkPublication[length + 1];

        System.arraycopy(oldPublications, 0, newPublications, 0, length);
        newPublications[length] = publication;

        publications = newPublications;

        publication.sendChannelEndpoint().addToDispatcher(publication, retransmitHandler, flowControl);
    }

    public void onRemovePublication(final NetworkPublication publication)
    {
        final NetworkPublication[] oldPublications = publications;
        final int length = oldPublications.length;
        final NetworkPublication[] newPublications = new NetworkPublication[length - 1];
        for (int i = 0, j = 0; i < length; i++)
        {
            if (oldPublications[i] != publication)
            {
                newPublications[j++] = oldPublications[i];
            }
        }

        publications = newPublications;
        publication.sendChannelEndpoint().removeFromDispatcher(publication);
        conductorProxy.closeResource(publication);
    }

    public void accept(final SenderCmd cmd)
    {
        cmd.execute(this);
    }

    private int doSend()
    {
        int bytesSent = 0;
        final NetworkPublication[] publications = this.publications;
        final int length = publications.length;

        if (length > 0)
        {
            int startingIndex = roundRobinIndex++;
            if (startingIndex >= length)
            {
                roundRobinIndex = startingIndex = 0;
            }

            int i = startingIndex;

            do
            {
                bytesSent += publications[i].send();

                if (++i == length)
                {
                    i = 0;
                }
            }
            while (i != startingIndex);
        }

        totalBytesSent.addOrdered(bytesSent);

        return bytesSent;
    }
}
