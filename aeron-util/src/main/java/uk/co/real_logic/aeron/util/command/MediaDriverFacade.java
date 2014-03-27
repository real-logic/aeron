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
 * Facade of MediaDriver for use by Library to send/receiver control commands/responses
 *
 * The library (aeron-core) implements this interface to send commands to a media driver and to handle
 * responses, Aeron control frames, etc. that come from the a media driver.
 */
public interface MediaDriverFacade
{
    /* commands to MediaDriver */

    /**
     * Request media driver to add a source for a given channel onto a given session Id for a given destination.
     *
     * The media driver will create sessions and underlying destination components as necessary
     *
     * @see LibraryFacade#onAddChannel(String, long, long)
     *
     * @param destination to add the channel on
     * @param sessionId to add the channel on
     * @param channelId to add
     */
    void sendAddChannel(final String destination, final long sessionId, final long channelId);

    /**
     * Request the media driver to remove a source for a given channel from a given session.
     *
     * If this is the last channel on this session, it will NOT be removed. It must be removed explicitly later.
     *
     * @see LibraryFacade#onRemoveChannel(String, long, long)
     *
     * @param destination to remove the channel from
     * @param sessionId to remove the channel from
     * @param channelId to remove
     */
    void sendRemoveChannel(final String destination, final long sessionId, final long channelId);

    /**
     * Request the media driver to remove a Term from a channel for a source.
     *
     * Terms are not removed unless this is done. It could be the app, but it could also be a separate admin
     * process/thread to send this command to the media driver.
     *
     * @see LibraryFacade#onRemoveTerm(String, long, long, long)
     *
     * @param destination to remove the term from
     * @param sessionId to remove the term from
     * @param channelId to remove the term from
     * @param termId to remove
     */
    void sendRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId);

    /**
     * Request the media driver to add a receiver for a given list of channels on a destination on behalf
     * of an application.
     *
     * The destination will be created if not already in use by another application.
     *
     * @see LibraryFacade#onAddReceiver(String, long[])
     * @param destination to add the channels to
     * @param channelIdList of interested channels
     */
    void sendAddReceiver(final String destination, final long[] channelIdList);

    /**
     * Request the media driver to remove a receiver destination on behalf of the application.
     *
     * The destination will be removed if this application is the last application using the destination.
     *
     * @see LibraryFacade#onRemoveReceiver(String, long[])
     * @param destination to remove
     * @param channelIdList the list of channels to remove on
     */
    void sendRemoveReceiver(final String destination, final long[] channelIdList);

    /**
     * Request the media driver should setup state for the next Term Buffer
     *
     * @see LibraryFacade#onRequestTerm(long, long, long)
     */
    void sendRequestTerm(final long sessionId, final long channelId, final long termId);

    /* callbacks from MediaDriver */

    /**
     * Handle an error response from the media driver
     *
     * This is an error in response to a command.
     *
     * @see LibraryFacade#sendErrorResponse(int, byte[])
     * @see ErrorCode
     *
     * @param code of the error
     * @param message returned by the media driver for the error
     */
    void onErrorResponse(final int code, final byte[] message);

    /**
     * Handle an error notification from the media driver
     *
     * This is an error in operation (not in response to a command).
     *
     * @see LibraryFacade#sendError(int, byte[])
     * @see ErrorCode
     *
     * @param code of the error
     * @param message returned by the media driver for the error
     */
    void onError(final int code, final byte[] message);

    /**
     * Handle a response from the media driver of returning a list of filenames for buffers
     *
     * @see LibraryFacade#sendNewBufferNotification(long, long, long, boolean)
     *
     */
    void onNewBufferNotification(final long sessionId, final long channelId, final long termId, final boolean isSender);

}
