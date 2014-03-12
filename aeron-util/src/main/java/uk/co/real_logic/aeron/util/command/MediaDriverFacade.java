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

import uk.co.real_logic.aeron.util.HeaderFlyweight;

import java.util.List;

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
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onAddChannel(String, long, long)
     *
     * @param destination to add the channel on
     * @param sessionId to add the channel on
     * @param channelId to add
     */
    void addChannel(final String destination, final long sessionId, final long channelId);

    /**
     * Request the media driver to remove a source for an entire session.
     *
     * If this is the last session on this destination, it will be removed.
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onRemoveSession(String, long)
     *
     * @param destination to remove the session from
     * @param sessionId of the session to remove
     */
    void removeSession(final String destination, final long sessionId);

    /**
     * Request the media driver to remove a source for a given channel from a given session.
     *
     * If this is the last channel on this session, it will NOT be removed. It must be removed explicitly later.
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onRemoveChannel(String, long, long)
     *
     * @param destination to remove the channel from
     * @param sessionId to remove the channel from
     * @param channelId to remove
     */
    void removeChannel(final String destination, final long sessionId, final long channelId);

    /**
     * Request the media driver to remove a Term from a channel for a source.
     *
     * Terms are not removed unless this is done. It could be the app, but it could also be a separate admin process/thread to
     * send this command to the media driver.
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onRemoveTerm(String, long, long, long)
     *
     * @param destination to remove the term from
     * @param sessionId to remove the term from
     * @param channelId to remove the term from
     * @param termId to remove
     */
    void removeTerm(final String destination, final long sessionId, final long channelId, final long termId);

    /**
     * Request the media driver to add a receiver for a given list of channels on a destination on behalf of an application.
     *
     * The destination will be created if not already in use by another application.
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onAddReceiver(String, java.util.List)
     *
     * @param destination to add the channels to
     * @param channelIdList of interested channels
     */
    void addReceiver(final String destination, final List<Long> channelIdList);

    /**
     * Request the media driver to remove a receiver destination on behalf of the application.
     *
     * The destination will be removed if this application is the last application using the destination.
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onRemoveReceiver(String)
     *
     * @param destination to remove
     */
    void removeReceiver(final String destination);

    /* callbacks from MediaDriver */

    /**
     * Handle a Flow Control Response (FCR) from a receiver back to a source
     *
     * May not be necessary to have this Aeron header type pushed back to source applications
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#flowControlResponse(uk.co.real_logic.aeron.util.HeaderFlyweight)
     *
     * @param header flyweight for the packet (TODO: make this its own subclass of HeaderFlyweight)
     */
    void handleFlowControlResponse(final HeaderFlyweight header);

    /**
     * Handle an error response from the media driver
     *
     * This is an error in response to a command.
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onErrorResponse(int, byte[])
     * @see uk.co.real_logic.aeron.util.command.ErrorCode
     *
     * @param code of the error
     * @param message returned by the media driver for the error
     */
    void handleErrorResponse(final int code, final byte[] message);

    /**
     * Handle an error notification from the media driver
     *
     * This is an error in operation (not in response to a command).
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onError(int, byte[])
     * @see uk.co.real_logic.aeron.util.command.ErrorCode
     *
     * @param code of the error
     * @param message returned by the media driver for the error
     */
    void handleErrorNotification(final int code, final byte[] message);

    /**
     * Handle a response from the media driver of returning a list of filenames for buffers
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onLocationResponse(java.util.List)
     *
     * @param filenames list of filenames for buffers
     */
    void handleLocationResponse(final List<byte[]> filenames);

    /**
     * Handle a notification from the media driver of a new session
     *
     * @see uk.co.real_logic.aeron.util.command.LibraryFacade#onNewSession(long, java.util.List)
     *
     * @param sessionId for the new session
     * @param filenames for the buffers associated with the session
     */
    void handleNewSessionNotification(final long sessionId, final List<byte[]> filenames);
}
