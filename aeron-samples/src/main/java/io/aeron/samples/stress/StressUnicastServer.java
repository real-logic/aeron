/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

import static io.aeron.samples.stress.StressUtil.UNICAST_STREAM_ID;
import static io.aeron.samples.stress.StressUtil.clientAddress;
import static io.aeron.samples.stress.StressUtil.info;
import static io.aeron.samples.stress.StressUtil.serverAddress;
import static io.aeron.samples.stress.StressUtil.unicastReqChannel;
import static io.aeron.samples.stress.StressUtil.unicastRspChannel;
import static io.aeron.samples.stress.StressUtil.validateMessage;

/**
 * A server that will echo back messages on specific channels to cover a variety of test scenarios designed to stress
 * some of the publication edge cases. E.g. IP fragmentation.
 */
public class StressUnicastServer implements Agent
{
    private final String serverAddress;
    private final String clientAddress;
    private final ControlledFragmentAssembler unicastFragmentAssembler = new ControlledFragmentAssembler(
        this::unicastReqHandler);
    private final SimpleReservedValueSupplier valueSupplier = new SimpleReservedValueSupplier();
    private final CRC64 crc = new CRC64();

    private Aeron aeron;
    private Subscription unicastSubscription;
    private Publication unicastPublication;

    /**
     * Construct stress server.
     *
     * @param serverAddress local address for the server to listen for requests.
     * @param clientAddress remote address for the server to send responses.
     */
    public StressUnicastServer(final String serverAddress, final String clientAddress)
    {
        this.serverAddress = serverAddress;
        this.clientAddress = clientAddress;
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        info("server=" + serverAddress + ", client=" + clientAddress);
        aeron = Aeron.connect();
        info("Connected to Aeron dir=" + aeron.context().aeronDirectoryName());

        unicastSubscription = aeron.addSubscription(
            unicastReqChannel(serverAddress).build(),
            UNICAST_STREAM_ID,
            StressUtil::imageAvailable,
            StressUtil::imageUnavailable);
        unicastPublication = aeron.addPublication(unicastRspChannel(clientAddress).build(), UNICAST_STREAM_ID);

        info("publications and subscriptions created");
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int count = 0;

        count += pollUnicast();

        return count;
    }

    private int pollUnicast()
    {
        return unicastSubscription.controlledPoll(unicastFragmentAssembler, 1);
    }

    private ControlledFragmentHandler.Action unicastReqHandler(
        final DirectBuffer msg,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = header.reservedValue();

        validateMessage(crc, msg, offset, length, correlationId);

        final long result = unicastPublication.offer(msg, offset, length, valueSupplier.set(correlationId));
        return result < 0 ? ControlledFragmentHandler.Action.ABORT : ControlledFragmentHandler.Action.COMMIT;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "Stress Server";
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        CloseHelper.quietCloseAll(unicastSubscription, unicastPublication, aeron);
    }

    /**
     * Entry point.
     *
     * @param args command line args.
     */
    public static void main(final String[] args)
    {
        final StressUnicastServer server = new StressUnicastServer(serverAddress(), clientAddress());
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
