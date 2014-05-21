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
package uk.co.real_logic.aeron.util.command;

/**
 * Facade of client application for use by media driver to send/receive control commands/responses
 *
 * The media driver (aeron-mediadriver) implements this interface to send notifications to a  client application
 * and to handle commands, Aeron control frames, etc. that come from the client application.
 */
public interface ClientFacade
{
    /**
     * Handle an addChannel request from the client
     *
     * The destination and session will be created if necessary
     *
     * @see MediaDriverFacade#sendAddChannel(String, long, long)
     *
     * @param channelMessage the message flyweight
     */
    void onAddChannel(final ChannelMessageFlyweight channelMessage);

    /**
     * Handle a removeChannel request from the client
     *
     * The session and destination will NOT be removed.
     *
     * @see MediaDriverFacade#sendRemoveChannel(String, long, long)
     *
     * @param channelMessage the message flyweight
     */
    void onRemoveChannel(final ChannelMessageFlyweight channelMessage);

    /**
     * Handle a addReceiver request from the client.
     *
     * The destination will be created if not in use by another client.
     *
     * @see MediaDriverFacade#sendAddSubscriber(String, long[])
     * @param subscriberMessage the message
     */
    void onAddSubscriber(final SubscriberMessageFlyweight subscriberMessage);

    /**
     * Handle a removeReceiver request from the client.
     *
     * The destination will be removed if this client is the last one using this destination.
     *
     * @see MediaDriverFacade#sendRemoveSubscriber(String, long[])
     * @param subscriberMessage the message
     */
    void onRemoveSubscriber(final SubscriberMessageFlyweight subscriberMessage);

    /**
     * Request the media driver should setup state for the next Term Buffer
     *
     * @see MediaDriverFacade#sendRequestTerm(long, long, long)
     *
     */
    void onRequestTerm(final long sessionId, final long channelId, final long termId);

    /**
     * Notify the client of an error for a request that it sent previously
     *
     * @see MediaDriverFacade#onErrorResponse(int, byte[])
     * @see uk.co.real_logic.aeron.util.ErrorCode
     *
     * @param code for the error
     * @param message for the error to be included in the notification
     */
    void sendErrorResponse(final int code, final byte[] message);

    /**
     * Notify the client of an error in operation not associated with a request from the application
     *
     * @see MediaDriverFacade#onError(int, byte[])
     * @see uk.co.real_logic.aeron.util.ErrorCode
     *
     * @param code for the error
     * @param message for the error to be included in the notification
     */
    void sendError(final int code, final byte[] message);

}
