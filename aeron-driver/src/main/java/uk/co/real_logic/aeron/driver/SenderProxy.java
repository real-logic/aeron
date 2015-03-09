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
import uk.co.real_logic.aeron.driver.cmd.ClosePublicationCmd;
import uk.co.real_logic.aeron.driver.cmd.NewPublicationCmd;
import uk.co.real_logic.aeron.driver.cmd.RetransmitPublicationCmd;
import uk.co.real_logic.aeron.driver.cmd.SenderCmd;

import java.util.Queue;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for offering into the Sender Thread's command queue.
 */
public class SenderProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<SenderCmd> commandQueue;
    private final AtomicCounter failCount;
    private Sender sender;

    public SenderProxy(final ThreadingMode threadingMode, final Queue<SenderCmd> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    public void sender(final Sender sender)
    {
        this.sender = sender;
    }

    public void retransmit(final DriverPublication publication, final int termId, final int termOffset, final int length)
    {
        if (isSharedThread())
        {
            sender.onRetransmit(publication, termId, termOffset, length);
        }
        else
        {
            offer(new RetransmitPublicationCmd(publication, termId, termOffset, length));
        }
    }

    public void closePublication(final DriverPublication publication)
    {
        if (isSharedThread())
        {
            sender.onClosePublication(publication);
        }
        else
        {
            offer(new ClosePublicationCmd(publication));
        }
    }

    public void newPublication(final DriverPublication publication)
    {
        if (isSharedThread())
        {
            sender.onNewPublication(publication);
        }
        else
        {
            offer(new NewPublicationCmd(publication));
        }
    }

    private boolean isSharedThread()
    {
        return threadingMode == SHARED;
    }

    private void offer(final SenderCmd cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.orderedIncrement();
            Thread.yield();
        }
    }
}
