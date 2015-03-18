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
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

/**
 * Agent that iterates over publications for sending them to registered subscribers.
 */
public class Sender implements Agent, Consumer<SenderCmd>
{
    private static final DriverPublication[] EMPTY_DRIVER_PUBLICATIONS = new DriverPublication[0];

    private final OneToOneConcurrentArrayQueue<SenderCmd> commandQueue;
    private final AtomicCounter totalBytesSent;

    private DriverPublication[] publications = EMPTY_DRIVER_PUBLICATIONS;
    private int roundRobinIndex = 0;

    public Sender(final MediaDriver.Context ctx)
    {
        this.commandQueue = ctx.senderCommandQueue();
        this.totalBytesSent = ctx.systemCounters().bytesSent();
    }

    public int doWork()
    {
        int workCount = 0;

        workCount += commandQueue.drain(this);
        workCount += doSend();

        return workCount;
    }

    public String roleName()
    {
        return "sender";
    }

    public void onRetransmit(final DriverPublication publication, final int termId, final int termOffset, final int length)
    {
        publication.onRetransmit(termId, termOffset, length);
    }

    public void onNewPublication(final DriverPublication publication)
    {
        final DriverPublication[] oldPublications = publications;
        final int length = oldPublications.length;
        final DriverPublication[] newPublications = new DriverPublication[length + 1];

        System.arraycopy(oldPublications, 0, newPublications, 0, length);
        newPublications[length] = publication;

        publications = newPublications;
    }

    public void onClosePublication(final DriverPublication publication)
    {
        final DriverPublication[] oldPublications = publications;
        final int length = oldPublications.length;
        final DriverPublication[] newPublications = new DriverPublication[length - 1];
        for (int i = 0, j = 0; i < length; i++)
        {
            if (oldPublications[i] != publication)
            {
                newPublications[j++] = oldPublications[i];
            }
        }

        publications = newPublications;
        publication.close();
    }

    public void accept(final SenderCmd cmd)
    {
        cmd.execute(this);
    }

    private int doSend()
    {
        int bytesSent = 0;
        final DriverPublication[] publications = this.publications;
        final int length = publications.length;

        int roundRobinIndex = ++this.roundRobinIndex;
        if (roundRobinIndex >= length)
        {
            this.roundRobinIndex = roundRobinIndex = 0;
        }

        if (length > 0)
        {
            int i = roundRobinIndex;
            do
            {
                bytesSent += publications[i].send();

                if (++i == length)
                {
                    i = 0;
                }
            }
            while (i != roundRobinIndex);
        }

        totalBytesSent.addOrdered(bytesSent);

        return bytesSent;
    }
}
