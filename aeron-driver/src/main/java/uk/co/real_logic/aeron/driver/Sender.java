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

import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.driver.cmd.ClosePublicationCmd;
import uk.co.real_logic.aeron.driver.cmd.NewPublicationCmd;
import uk.co.real_logic.aeron.driver.cmd.RetransmitPublicationCmd;

import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Agent that iterates over publications for sending them to registered subscribers.
 */
public class Sender extends Agent
{
    private final Consumer<Object> processConductorCommandsFunc = this::processConductorCommands;
    private final ArrayList<DriverPublication> publications = new ArrayList<>();
    private final OneToOneConcurrentArrayQueue<Object> commandQueue;

    private int roundRobinIndex = 0;

    public Sender(final MediaDriver.Context ctx)
    {
        super(ctx.senderIdleStrategy(), ctx.exceptionConsumer(), ctx.systemCounters().driverExceptions());

        this.commandQueue = ctx.senderCommandQueue();
    }

    public int doWork()
    {
        int workCount = 0;

        workCount += commandQueue.drain(processConductorCommandsFunc);
        workCount += doSend();

        return workCount;
    }

    private int doSend()
    {
        int workCount = 0;
        final ArrayList<DriverPublication> publications = this.publications;
        final int arrayLength = publications.size();

        roundRobinIndex++;
        if (publications.size() <= roundRobinIndex)
        {
            roundRobinIndex = 0;
        }

        if (arrayLength > 0)
        {
            int i = roundRobinIndex;
            do
            {
                workCount += publications.get(i).send();

                if (++i == arrayLength)
                {
                    i = 0;
                }
            }
            while (i != roundRobinIndex);
        }

        return workCount;
    }

    private void processConductorCommands(final Object obj)
    {
        if (obj instanceof RetransmitPublicationCmd)
        {
            final RetransmitPublicationCmd cmd = (RetransmitPublicationCmd)obj;
            cmd.publication().onRetransmit(cmd.termId(), cmd.termOffset(), cmd.length());
        }
        if (obj instanceof NewPublicationCmd)
        {
            final NewPublicationCmd cmd = (NewPublicationCmd)obj;
            publications.add(cmd.publication());
        }
        else if (obj instanceof ClosePublicationCmd)
        {
            final ClosePublicationCmd cmd = (ClosePublicationCmd)obj;
            publications.remove(cmd.publication());
            cmd.publication().close();
        }
    }
}
