/*
 * Copyright 2016 Real Logic Ltd.
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
package io.aeron.driver.status;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

import java.nio.charset.StandardCharsets;

import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Allocates {@link AtomicCounter} indicating channel endpoint status. Indicators include:
 * <ul>
 *     <li>{@link SendChannelStatus}: Indication of send channel status.</li>
 *     <li>{@link ReceiveChannelStatus}: Indication of receive channel status.</li>
 * </ul>
 */
public class ChannelEndpointStatus
{
    /**
     * Channel is being initialized.
     */
    public static final long INITIALIZING = 0;

    /**
     * Channel has errored. Check error log for information.
     */
    public static final long ERRORED = -1;

    /**
     * Channel has finished initialization successfully and is active.
     */
    public static final long ACTIVE = 1;

    /**
     * Channel is being closed.
     */
    public static final long CLOSING = 2;

    /**
     * Offset in the key meta data for the channel of the counter.
     */
    public static final int CHANNEL_OFFSET = 0;

    /**
     * The maximum length in bytes of the encoded channel identity.
     */
    public static final int MAX_CHANNEL_LENGTH = CountersReader.MAX_KEY_LENGTH - (CHANNEL_OFFSET + SIZE_OF_INT);

    /**
     * Allocate an indicator for tracking the status of a channel endpoint.
     *
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which to allocated the underlying storage.
     * @param channel         for the stream of messages.
     * @return a new {@link AtomicCounter} for tracking the status.
     */
    public static AtomicCounter allocate(
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final String channel)
    {
        final String label = name + ": " + channel;

        return countersManager.newCounter(
            label,
            typeId,
            (buffer) ->
            {
                final byte[] channelBytes = channel.getBytes(StandardCharsets.UTF_8);
                final int length = Math.min(channelBytes.length, MAX_CHANNEL_LENGTH);

                buffer.putInt(CHANNEL_OFFSET, length);
                buffer.putBytes(CHANNEL_OFFSET + SIZE_OF_INT, channelBytes, 0, length);
            });
    }
}
