/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;

public class ClusteredServiceAgent implements Agent, FragmentHandler, Cluster
{
    /**
     * Length of the session header that will be precede application protocol message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

    private static final int FRAGMENT_LIMIT = 10;
    private static final int INITIAL_BUFFER_LENGTH = 4096;

    private long timestampMs;
    private final boolean useAeronAgentInvoker;
    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final ClusteredService service;
    private final Subscription logSubscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this, INITIAL_BUFFER_LENGTH, true);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();

    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();

    public ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        aeron = ctx.aeron();
        useAeronAgentInvoker = aeron.context().useConductorAgentInvoker();
        aeronAgentInvoker = aeron.conductorAgentInvoker();
        service = ctx.clusteredService();
        logSubscription = aeron.addSubscription(ctx.logChannel(), ctx.logStreamId());
    }

    public void onStart()
    {
        service.onStart(this);
    }

    public int doWork() throws Exception
    {
        int workCount = 0;

        if (useAeronAgentInvoker)
        {
            workCount += aeronAgentInvoker.invoke();
        }

        workCount += logSubscription.poll(fragmentAssembler, FRAGMENT_LIMIT);

        return workCount;
    }

    public String roleName()
    {
        return "service-agent";
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionHeaderDecoder.TEMPLATE_ID:
            {
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                timestampMs = sessionHeaderDecoder.timestamp();
                service.onSessionMessage(
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.correlationId(),
                    timestampMs,
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);

                break;
            }

            case TimerEventDecoder.TEMPLATE_ID:
            {
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                timestampMs = timerEventDecoder.timestamp();
                service.onTimerEvent(timerEventDecoder.correlationId(), timestampMs);
                break;
            }

            case SessionOpenEventDecoder.TEMPLATE_ID:
            {
                openEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = openEventDecoder.clusterSessionId();
                final ClientSession session = new ClientSession(
                    sessionId,
                    aeron.addExclusivePublication(
                        openEventDecoder.responseChannel(),
                        openEventDecoder.responseStreamId()),
                        this);

                sessionByIdMap.put(sessionId, session);
                timestampMs = openEventDecoder.timestamp();
                service.onSessionOpen(session, timestampMs);
                break;
            }

            case SessionCloseEventDecoder.TEMPLATE_ID:
            {
                closeEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final ClientSession session = sessionByIdMap.get(closeEventDecoder.clusterSessionId());
                if (null != session)
                {
                    timestampMs = closeEventDecoder.timestamp();
                    session.responsePublication().close();
                    service.onSessionClose(session, timestampMs, closeEventDecoder.closeReason());
                }
                break;
            }
        }
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public long timeMs()
    {
        return timestampMs;
    }

    public void scheduleTimer(final long correlationId, final long deadlineMs)
    {
    }

    public void cancelTimer(final long correlationId)
    {
    }
}
