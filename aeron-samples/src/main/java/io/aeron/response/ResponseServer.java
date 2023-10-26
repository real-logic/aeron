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
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.Objects;
import java.util.function.Function;

/**
 * A basic sample response server that allows the users to specify a simple function to represent the handling of a
 * request and then return a response. This approach will be effective when request processing is very short. For
 * certain types of response servers, e.g. returning a large volume of data from a database, this pattern will be
 * ineffective. For those types of use cases, something more complex, likely involving thread pooling would be required.
 */
public class ResponseServer implements AutoCloseable
{
    private final Aeron aeron;
    private final Long2ObjectHashMap<ResponseSession> clientToPublicationMap =
        new Long2ObjectHashMap<>();
    private final OneToOneConcurrentArrayQueue<Image> availableImages =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final OneToOneConcurrentArrayQueue<Image> unavailableImages =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final Function<Image, ResponseHandler> handlerFactory;
    private final ChannelUriStringBuilder requestUriBuilder;
    private final ChannelUriStringBuilder responseUriBuilder;

    private Subscription serverSubscription;

    /**
     * Constructs the server.
     *
     * @param aeron                 connected Aeron client.
     * @param handlerFactory        factor to produce handler instances for each incoming session.
     * @param requestEndpoint  channel to listen for requests and new connections.
     * @param responseEndpoint      channel to send responses back on.
     * @param requestChannel   fragment to aid in configuration of the subscription (can be null)
     * @param responseChannel       fragment to aid in configuration of the response publication (can be null)
     */
    public ResponseServer(
        final Aeron aeron,
        final Function<Image, ResponseHandler> handlerFactory,
        final String requestEndpoint,
        final String responseEndpoint,
        final String requestChannel,
        final String responseChannel)
    {
        this.aeron = aeron;
        this.handlerFactory = handlerFactory;

        Objects.requireNonNull(requestEndpoint, "subscriptionEndpoint must not be null");
        Objects.requireNonNull(responseEndpoint, "responseEndpoint must not be null");

        requestUriBuilder = null == requestChannel ?
            new ChannelUriStringBuilder() : new ChannelUriStringBuilder(requestChannel);
        requestUriBuilder
            .media("udp")
            .endpoint(requestEndpoint)
            .responseEndpoint(responseEndpoint);
        responseUriBuilder = null == responseChannel ?
            new ChannelUriStringBuilder() : new ChannelUriStringBuilder(responseChannel);
        responseUriBuilder
            .media("udp")
            .controlMode("response")
            .controlEndpoint(responseEndpoint);
    }

    ResponseSession getOrCreateSession(final Image image)
    {
        ResponseSession session = clientToPublicationMap.get(image.correlationId());

        if (null == session)
        {
            final Publication responsePublication = aeron.addPublication(
                responseUriBuilder.responseCorrelationId(image.correlationId()).build(),
                10002);

            final ResponseHandler handler = handlerFactory.apply(image);
            session = new ResponseSession(responsePublication, handler);

            clientToPublicationMap.put(image.correlationId(), session);
        }

        return session;
    }

    int poll()
    {
        int workCount = 0;

        if (null == serverSubscription)
        {
            serverSubscription = aeron.addSubscription(
                requestUriBuilder.build(),
                10001,
                availableImages::offer,
                unavailableImages::offer);
            workCount++;
        }

        Image image;
        while (null != (image = availableImages.poll()))
        {
            workCount++;
            getOrCreateSession(image);
        }

        while (null != (image = unavailableImages.poll()))
        {
            workCount++;
            clientToPublicationMap.remove(image.correlationId());
        }

        workCount += serverSubscription.poll(
            (buffer, offset, length, header) ->
            {
                final ResponseSession session = getOrCreateSession(
                    serverSubscription.imageBySessionId(header.sessionId()));

                session.process(buffer, offset, length, header);
            },
            1);


        return workCount;
    }

    /**
     * The number of connected clients.
     *
     * @return the number of connected clients.
     */
    public int sessionCount()
    {
        return clientToPublicationMap.size();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.quietClose(serverSubscription);
        clientToPublicationMap.values().forEach(CloseHelper::quietClose);
    }

    /**
     * Interface to manage callback from the response server onto a session.
     */
    public interface ResponseHandler
    {
        /**
         * Called when a message is received via the request subscription.
         *
         * @param buffer                containing the data.
         * @param offset                at which the data begins.
         * @param length                of the data in bytes.
         * @param header                representing the metadata for the data.
         * @param responsePublication   to send responses back to the client.
         */
        void onMessage(DirectBuffer buffer, int offset, int length, Header header, Publication responsePublication);
    }

    private static final class ResponseSession implements AutoCloseable
    {
        private final Publication publication;
        private final ResponseHandler handler;

        ResponseSession(final Publication publication, final ResponseHandler handler)
        {
            this.publication = publication;
            this.handler = handler;
        }

        public void process(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            handler.onMessage(buffer, offset, length, header, publication);
        }

        /**
         * {@inheritDoc}
         */
        public void close()
        {
            CloseHelper.close(publication);
        }
    }
}
