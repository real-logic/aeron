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

import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.util.List;

/**
 * Facade of application library for use by media driver to send/receiver control commands/responses
 *
 * The media driver (aeron-mediadriver) implements this interface to send notifications to a library/application and to handle
 * commands, Aeron control frames, etc. that come from the library/application.
 */
public interface LibraryFacade
{
    /* callbacks from library/application */

    /**
     * Handle an addChannel request from the library
     *
     * The destination and session will be created if necessary
     *
     * @see MediaDriverFacade#sendAddChannel(String, long, long)
     *
     * @param destination for the channel
     * @param sessionId for the channel
     * @param channelId for the channel
     */
    void onAddChannel(final String destination, final long sessionId, final long channelId);

    /**
     * Handle a removeChannel request from the library
     *
     * The session and destination will NOT be removed.
     *
     * @see MediaDriverFacade#sendRemoveChannel(String, long, long)
     *
     * @param destination for the channel to be removed from
     * @param sessionId for the channel to be removed from
     * @param channelId for the channel
     */
    void onRemoveChannel(final String destination, final long sessionId, final long channelId);

    /**
     * Handle a removeTerm request from the library
     *
     * Remove and delete a given Term. Terms must be explicitly removed by a library (or admin process/thread)
     *
     * @see MediaDriverFacade#sendRemoveTerm(String, long, long, long)
     *
     * @param destination for the term to be removed from
     * @param sessionId for the term to be removed from
     * @param channelId to remove the term from
     * @param termId for the term
     */
    void onRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId);

    /**
     * Handle a addReceiver request from the library.
     *
     * The destination will be created if not in use by another library/application.
     *
     * @see MediaDriverFacade#sendAddReceiver(String, long[])
     *@param destination to be added
     * @param channelIdList of interested channels for destination
     */
    void onAddReceiver(final String destination, final long[] channelIdList);

    /**
     * Handle a removeReceiver request from the library.
     *
     * The destination will be removed if this library/application is the last one using this destination.
     *
     * @see MediaDriverFacade#sendRemoveReceiver(String)
     *
     * @param destination to be removed
     */
    void onRemoveReceiver(final String destination);

    /**
     * Request the media driver should setup state for the next Term Buffer
     *
     * @see MediaDriverFacade#sendRequestTerm(long, long, long)
     *
     * @param sessionId
     * @param channelId
     * @param termId
     */
    void onRequestTerm(final long sessionId, final long channelId, final long termId);

    /* notifications to library/application */

    /**
     * Notify the library of a received Status Message (SM)
     *
     * This is an Aeron control frame.
     *
     * @see MediaDriverFacade#onStatusMessage(uk.co.real_logic.aeron.util.protocol.HeaderFlyweight)
     *
     * @param header flyweight for the SM
     */
    void sendStatusMessage(final HeaderFlyweight header);

    /**
     * Notify the library of an error for a request that it sent previously
     *
     * @see MediaDriverFacade#onErrorResponse(int, byte[])
     * @see ErrorCode
     *
     * @param code for the error
     * @param message for the error to be included in the notification
     */
    void sendErrorResponse(final int code, final byte[] message);

    /**
     * Notify the library of an error in operation not associated with a request from the application
     *
     * @see MediaDriverFacade#onError(int, byte[])
     * @see ErrorCode
     *
     * @param code for the error
     * @param message for the error to be included in the notification
     */
    void sendError(final int code, final byte[] message);

    /**
     * Notify the library of locations of Term filenames
     *
     * @see MediaDriverFacade#onLocationResponse(java.util.List)
     *
     * @param filenames list of filenames of Terms
     */
    void sendLocationResponse(final List<byte[]> filenames);

    /**
     * Notify the library of new source sessions for interested channels
     *
     * @see MediaDriverFacade#onNewSession(long, java.util.List)
     *
     * @param sessionId for the new session
     * @param filenames for the channels of the new session
     */
    void sendNewSession(final long sessionId, final List<byte[]> filenames);
}
