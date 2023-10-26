/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.response;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.DirectBuffer;

/**
 * Sample client to be used with the response server.
 */
public class ResponseClient implements AutoCloseable
{
    private final Aeron aeron;
    private final String requestEndpoint;
    private final ChannelUriStringBuilder requestUriBuilder;
    private final ChannelUriStringBuilder responseUriBuilder;
    private Publication publication;
    private Subscription subscription;

    /**
     * Construct a response client with the associated request endpoint. The response endpoint is not required as
     * the server will manage sending this to the client
     *
     * @param aeron             client to use to connect to the server.
     * @param requestEndpoint   server's subscription endpoint.
     * @param requestChannel    channel fragment to allow for configuration of parameters on the request publication.
     *                          May be null. The 'endpoint' parameter is not required and will be replaced by the
     *                          <code>requestEndpoint</code> if specified.
     * @param responseChannel   channel fragment to allow for configuration parameters on the response subscription.
     *                          May be null. The 'control' parameter is not required and will be removed if specified.
     */
    public ResponseClient(
        final Aeron aeron,
        final String requestEndpoint,
        final String requestChannel,
        final String responseChannel)
    {
        this.aeron = aeron;
        this.requestEndpoint = requestEndpoint;

        requestUriBuilder = null != requestChannel ?
            new ChannelUriStringBuilder(requestChannel) : new ChannelUriStringBuilder();
        requestUriBuilder
            .media("udp")
            .endpoint(requestEndpoint);
        responseUriBuilder = null != responseChannel ?
            new ChannelUriStringBuilder(responseChannel) : new ChannelUriStringBuilder();
        responseUriBuilder
            .media("udp")
            .controlEndpoint((String)null)
            .isResponseChannel(Boolean.TRUE);
    }

    int poll()
    {
        int workCount = 0;

        if (null == subscription)
        {
            subscription = aeron.addSubscription(responseUriBuilder.build(), 10001);
        }

        if (null == publication && null != subscription)
        {
            publication = aeron.addPublication(
                requestUriBuilder.responseSubscriptionId(subscription.registrationId()).build(),
                10001);

            workCount++;
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {

    }

    /**
     * Check that the client is connected.
     *
     * @return true if the client is connected.
     */
    public boolean isConnected()
    {
        return null != subscription && subscription.isConnected() && null != publication && publication.isConnected();
    }

    /**
     * Offer a message on the request channel.
     *
     * @param message to be sent
     * @return result code from the publication.
     */
    public long offer(final DirectBuffer message)
    {
        return publication.offer(message);
    }
}
