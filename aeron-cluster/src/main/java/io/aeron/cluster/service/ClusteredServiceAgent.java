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

public class ClusteredServiceAgent implements Agent, FragmentHandler, Cluster
{
    /**
     * Length of the session header that will be precede application protocol message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

    private static final int FRAGMENT_LIMIT = 10;
    private static final int INITIAL_BUFFER_LENGTH = 4096;

    private final Aeron aeron;
    private final ClusteredService service;
    private final Subscription logSubscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this, INITIAL_BUFFER_LENGTH, true);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionConnectRequestDecoder connectRequestDecoder = new SessionConnectRequestDecoder();
    private final SessionCloseRequestDecoder closeRequestDecoder = new SessionCloseRequestDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();

    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();

    public ClusteredServiceAgent(final Aeron aeron, final ClusteredService service, final Subscription logSubscription)
    {
        this.aeron = aeron;
        this.service = service;
        this.logSubscription = logSubscription;
    }

    public void onStart()
    {
        service.onStart(this);
    }

    public int doWork() throws Exception
    {
        return logSubscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
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
            case SessionConnectRequestDecoder.TEMPLATE_ID:
            {
                connectRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = connectRequestDecoder.clusterSessionId();
                final ClientSession session = new ClientSession(
                    sessionId,
                    aeron.addExclusivePublication(
                        connectRequestDecoder.responseChannel(),
                        connectRequestDecoder.responseStreamId()));

                sessionByIdMap.put(sessionId, session);
                service.onSessionOpen(session);
                break;
            }

            case SessionHeaderDecoder.TEMPLATE_ID:
            {
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                service.onSessionMessage(
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.correlationId(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);

                break;
            }

            case SessionCloseRequestDecoder.TEMPLATE_ID:
                closeRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final ClientSession session = sessionByIdMap.get(closeRequestDecoder.clusterSessionId());
                if (null != session)
                {
                    session.responsePublication().close();
                    service.onSessionClose(session);
                }
                break;

            case TimerEventDecoder.TEMPLATE_ID:
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                service.onTimerEvent(timerEventDecoder.correlationId());
                break;
        }
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public void registerTimer(final long correlationId, final long deadlineMs)
    {

    }

    public void cancelTimer(final long correlationId)
    {

    }
}
