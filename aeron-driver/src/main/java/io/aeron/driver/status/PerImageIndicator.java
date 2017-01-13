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
package io.aeron.driver.status;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.nio.charset.StandardCharsets;

import static io.aeron.driver.status.StreamPositionCounter.*;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Allocates {@link AtomicCounter} indicating a per {@link io.aeron.driver.PublicationImage} indication.
 */
public class PerImageIndicator
{
    /**
     * Type id of a per Image indicator.
     */
    public static final int PER_IMAGE_TYPE_ID = 10;

    /**
     * Allocate a per {@link io.aeron.driver.PublicationImage} indicator.
     *
     * @param name            of the counter for the label.
     * @param countersManager from which to allocated the underlying storage.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @param suffix          for the label.
     * @return a new {@link AtomicCounter} for tracking the indicator.
     */
    public static AtomicCounter allocate(
        final String name,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel,
        final String suffix)
    {
        final String label =
            name + ": " + registrationId + ' ' + sessionId + ' ' + streamId + ' ' + channel + ' ' + suffix;

        return countersManager.newCounter(
            label,
            PER_IMAGE_TYPE_ID,
            (buffer) ->
            {
                buffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
                buffer.putInt(SESSION_ID_OFFSET, sessionId);
                buffer.putInt(STREAM_ID_OFFSET, streamId);

                final byte[] channelBytes = channel.getBytes(StandardCharsets.UTF_8);
                final int length = Math.min(channelBytes.length, MAX_CHANNEL_LENGTH);

                buffer.putInt(CHANNEL_OFFSET, length);
                buffer.putBytes(CHANNEL_OFFSET + SIZE_OF_INT, channelBytes, 0, length);
            });
    }
}
