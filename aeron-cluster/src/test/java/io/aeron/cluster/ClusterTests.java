/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Counter;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.exceptions.AeronException;
import io.aeron.test.Tests;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

class ClusterTests
{
    static final String HELLO_WORLD_MSG = "Hello World!";
    static final String NO_OP_MSG = "No op!           ";
    static final String REGISTER_TIMER_MSG = "Register a timer!";
    static final String ECHO_IPC_INGRESS_MSG = "Echo as IPC ingress";
    static final String UNEXPECTED_MSG = "Should never get this message because it is not going to be committed!";

    private static final AtomicReference<Throwable> CLUSTER_ERROR = new AtomicReference<>();

    public static final Runnable TERMINATION_HOOK =
        () ->
        {
            throw new AgentTerminationException();
        };

    public static Runnable dynamicTerminationHook(
        final AtomicBoolean terminationExpected, final AtomicBoolean wasTerminated)
    {
        return () ->
        {
            if (null == terminationExpected || !terminationExpected.get())
            {
                throw new AgentTerminationException();
            }

            if (null != wasTerminated)
            {
                wasTerminated.set(true);
            }
        };
    }

    public static ErrorHandler errorHandler(final int nodeId)
    {
        return
            (ex) ->
            {
                if (ex instanceof AeronException)
                {
                    if (((AeronException)ex).category() == AeronException.Category.WARN)
                    {
                        //final String message = ex.getMessage();
                        //final String name = ex.getClass().getName();
                        //System.err.println("Warning in node " + nodeId + " " + name + " : " + message);
                        return;
                    }
                }

                if (ex instanceof AgentTerminationException)
                {
                    return;
                }

                addError(ex);

                System.err.println("\n*** Error in node " + nodeId + " followed by system thread dump ***\n\n");
                ex.printStackTrace();

                System.err.println();
                System.err.println(SystemUtil.threadDump());
            };
    }

    public static void addError(final Throwable ex)
    {
        final Throwable error = CLUSTER_ERROR.get();
        if (null == error)
        {
            CLUSTER_ERROR.set(ex);
        }
        else
        {
            error.addSuppressed(ex);
        }
    }

    public static void failOnClusterError()
    {
        final Throwable error = CLUSTER_ERROR.getAndSet(null);
        if (null != error)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    public static void awaitElectionState(final Counter electionStateCounter, final ElectionState state)
    {
        while (electionStateCounter.get() != state.code())
        {
            Tests.yield();
        }
    }

    public static Thread startMessageThread(final TestCluster cluster, final long backoffIntervalNs)
    {
        final Thread thread = new Thread(
            () ->
            {
                final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
                final AeronCluster client = cluster.client();
                final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
                msgBuffer.putStringWithoutLengthAscii(0, HELLO_WORLD_MSG);

                while (!Thread.interrupted())
                {
                    if (client.offer(msgBuffer, 0, HELLO_WORLD_MSG.length()) < 0)
                    {
                        LockSupport.parkNanos(backoffIntervalNs);
                    }

                    idleStrategy.idle(client.pollEgress());
                }
            });

        thread.setDaemon(true);
        thread.setName("message-thread");
        thread.start();

        return thread;
    }
}
