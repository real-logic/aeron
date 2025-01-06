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
package io.aeron.samples.stress;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

import java.util.concurrent.locks.LockSupport;

import static io.aeron.samples.stress.StressUtil.MDC_STREAM_ID;
import static io.aeron.samples.stress.StressUtil.clientAddress;
import static io.aeron.samples.stress.StressUtil.mdcRspPubChannel;
import static io.aeron.samples.stress.StressUtil.serverAddress;
import static io.aeron.samples.stress.StressUtil.validateMessage;

/**
 * A server that will echo back messages on specific channels to cover a variety of test scenarios designed to stress
 * some of the publication edge cases. E.g. IP fragmentation.
 */
public class StressMdcServer implements Agent
{
    private final String serverAddress;
    private final String clientAddress;
    private final ControlledFragmentAssembler mdcFragmentAssembler1 = new ControlledFragmentAssembler(
        this::mdcReqHandler);
    private final ControlledFragmentAssembler mdcFragmentAssembler2 = new ControlledFragmentAssembler(
        this::mdcReqHandler);
    private final SimpleReservedValueSupplier valueSupplier = new SimpleReservedValueSupplier();
    private final CRC64 crc = new CRC64();

    private Aeron aeron;
    private Subscription mdcSubscription1;
    private Subscription mdcSubscription2;
    private Publication mdcPublication;
    private Counter serverReceiveCount;
    private Counter serverSendCount;

    /**
     * Construct stress server.
     *
     * @param serverAddress local address for the server to listen for requests.
     * @param clientAddress remote address for the server to send responses.
     */
    public StressMdcServer(final String serverAddress, final String clientAddress)
    {
        this.serverAddress = serverAddress;
        this.clientAddress = clientAddress;
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        aeron = Aeron.connect(new Aeron.Context());

        mdcSubscription1 = aeron.addSubscription(
            StressUtil.mdcReqSubChannel1(serverAddress, clientAddress).build(),
            MDC_STREAM_ID,
            StressUtil::imageAvailable,
            StressUtil::imageUnavailable);
        mdcSubscription2 = aeron.addSubscription(
            StressUtil.mdcReqSubChannel2(serverAddress, clientAddress).build(),
            MDC_STREAM_ID,
            StressUtil::imageAvailable,
            StressUtil::imageUnavailable);

        mdcPublication = aeron.addPublication(mdcRspPubChannel(serverAddress).linger(0L).build(), MDC_STREAM_ID);

        serverReceiveCount = aeron.addCounter(StressUtil.SERVER_RECV_COUNT, "Server Receive Count");
        serverSendCount = aeron.addCounter(StressUtil.SERVER_SEND_COUNT, "Server Send Count");
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        if (!mdcSubscription1.isConnected() || !mdcSubscription2.isConnected())
        {
            return 0;
        }

        int count = 0;

        count += pollUnicast();

        return count;
    }

    private int pollUnicast()
    {
        int count = 0;

        count += mdcSubscription1.controlledPoll(mdcFragmentAssembler1, 1);
        count += mdcSubscription2.controlledPoll(mdcFragmentAssembler2, 1);

        return count;
    }

    private ControlledFragmentHandler.Action mdcReqHandler(
        final DirectBuffer msg,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = header.reservedValue();

        validateMessage(crc, msg, offset, length, correlationId);

        final long result = mdcPublication.offer(msg, offset, length, valueSupplier.set(correlationId));

        if (result > 0)
        {
            serverReceiveCount.increment();
            serverSendCount.increment();
        }

        return result < 0 ? ControlledFragmentHandler.Action.ABORT : ControlledFragmentHandler.Action.COMMIT;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "Stress MDC Server";
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        CloseHelper.quietCloseAll(mdcSubscription1, mdcSubscription2, mdcPublication, aeron);
        LockSupport.parkNanos(1_000_000_000L);
    }

    /**
     * Entry point.
     *
     * @param args command line args.
     */
    public static void main(final String[] args)
    {
        final StressMdcServer server = new StressMdcServer(serverAddress(), clientAddress());
        server.onStart();
        try
        {
            while (!Thread.currentThread().isInterrupted())
            {
                server.doWork();
            }
        }
        finally
        {
            server.onClose();
        }
    }
}
