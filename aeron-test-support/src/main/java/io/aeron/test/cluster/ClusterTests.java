/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.test.cluster;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class ClusterTests
{
    public static final String HELLO_WORLD_MSG = "Hello World!";
    public static final String NO_OP_MSG = "No op!           ";
    public static final String REGISTER_TIMER_MSG = "Register a timer!";
    public static final String ECHO_IPC_INGRESS_MSG = "Echo as IPC ingress";
    public static final String UNEXPECTED_MSG =
        "Should never get this message because it is not going to be committed!";

    private static final AtomicReference<Throwable> ERROR = new AtomicReference<>();
    private static final AtomicReference<Throwable> WARNING = new AtomicReference<>();

    public static final Runnable NOOP_TERMINATION_HOOK = () -> {};

    public static Runnable terminationHook(final AtomicBoolean isTerminationExpected, final AtomicBoolean hasTerminated)
    {
        return
            () ->
            {
                if (null != isTerminationExpected && isTerminationExpected.get())
                {
                    if (null != hasTerminated)
                    {
                        hasTerminated.set(true);
                    }

                    throw new ClusterTerminationException();
                }

                throw new AgentTerminationException();
            };
    }

    public static ErrorHandler errorHandler(final int memberId)
    {
        return
            (ex) ->
            {
                if (ex instanceof AeronException && ((AeronException)ex).category() == AeronException.Category.WARN)
                {
                    //System.err.println("\n *** Warning in member " + memberId + " \n");
                    //ex.printStackTrace();
                    addWarning(ex);
                    return;
                }

                if (ex instanceof ClusterTerminationException)
                {
                    return;
                }

                addError(ex);

                System.err.println("\n*** Error in member " + memberId + " ***\n\n");
                ex.printStackTrace();
                printWarning();

                System.err.println();
                System.err.println(SystemUtil.threadDump());
            };
    }

    public static void addError(final Throwable ex)
    {
        final Throwable error = ERROR.get();
        if (null == error)
        {
            ERROR.set(ex);
        }
        else if (error != ex)
        {
            error.addSuppressed(ex);
        }
    }

    public static void addWarning(final Throwable ex)
    {
        final Throwable warning = WARNING.get();
        if (null == warning)
        {
            WARNING.set(ex);
        }
        else if (warning != ex)
        {
            warning.addSuppressed(ex);
        }
    }

    public static void printWarning()
    {
        final Throwable warning = WARNING.get();
        if (null != warning)
        {
            System.err.println("\n*** Warning captured ***");
            warning.printStackTrace();
        }
    }

    public static void failOnClusterError()
    {
        final Throwable error = ERROR.getAndSet(null);
        final Throwable warning = WARNING.getAndSet(null);

        if (null != error)
        {
            if (null != warning)
            {
                System.err.println("\n*** Warning captured with error ***");
                warning.printStackTrace();
            }

            LangUtil.rethrowUnchecked(error);
        }

        if (Thread.currentThread().isInterrupted() && null != warning)
        {
            System.err.println("\n*** Warning captured with interrupt ***");
            warning.printStackTrace();
        }
    }

    public static Thread startPublisherThread(
        final TestCluster testCluster, final MutableInteger messageCounter, final long backoffIntervalNs)
    {
        final Thread thread = new Thread(
            () ->
            {
                final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
                final AeronCluster client = testCluster.client();
                final ExpandableArrayBuffer msgBuffer = testCluster.msgBuffer();
                msgBuffer.putStringWithoutLengthAscii(0, HELLO_WORLD_MSG);

                while (!Thread.interrupted())
                {
                    final long result = client.offer(msgBuffer, 0, HELLO_WORLD_MSG.length());
                    if (result > 0)
                    {
                        messageCounter.increment();
                    }
                    else
                    {
                        if (Publication.CLOSED == result)
                        {
                            break;
                        }
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
