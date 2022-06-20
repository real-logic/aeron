/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.archive.client;

import io.aeron.Aeron;

/**
 * Contains the optional parameters that can be passed to a Replication Request.
 */
public class ReplicationParams
{
    private long stopPosition;
    private long dstRecordingId;
    private String liveChannel;
    private long channelTagId;
    private long subscriptionTagId;

    /**
     * Initialise all parameters to defaults.
     */
    public ReplicationParams()
    {
        reset();
    }

    /**
     * Reset the state of the parameters to the default for reuse.
     *
     * @return this for a fluent API.
     */
    public ReplicationParams reset()
    {
        stopPosition = AeronArchive.NULL_POSITION;
        dstRecordingId = Aeron.NULL_VALUE;
        liveChannel = null;
        channelTagId = Aeron.NULL_VALUE;
        subscriptionTagId = Aeron.NULL_VALUE;
        return this;
    }

    /**
     * Set the stop position for replication, default is {@link AeronArchive#NULL_POSITION}, which will continuously
     * replicate.
     *
     * @param stopPosition position to stop the replication at.
     * @return this for a fluent API
     */
    public ReplicationParams stopPosition(final long stopPosition)
    {
        this.stopPosition = stopPosition;
        return this;
    }

    /**
     * The stop position for this replication request.
     * @return stop position
     */
    public long stopPosition()
    {
        return stopPosition;
    }

    /**
     * The recording in the local archive to extend. Default is {@link Aeron#NULL_VALUE} which will replicate into a
     * new recording.
     *
     * @param dstRecordingId destination recording to extend.
     * @return this for a fluent API.
     */
    public ReplicationParams dstRecordingId(final long dstRecordingId)
    {
        this.dstRecordingId = dstRecordingId;
        return this;
    }

    /**
     * Destination recording id to extend.
     *
     * @return destination recording id.
     */
    public long dstRecordingId()
    {
        return dstRecordingId;
    }

    /**
     * Destination for the live stream if merge is required. Default is null for no merge.
     *
     * @param liveChannel for the live stream merge
     * @return this for a fluent API.
     */
    public ReplicationParams liveChannel(final String liveChannel)
    {
        this.liveChannel = liveChannel;
        return this;
    }

    /**
     * Gets the destination for the live stream merge.
     *
     * @return destination for live stream merge.
     */
    public String liveChannel()
    {
        return liveChannel;
    }

    /**
     * The channel used by the archive's subscription for replication will have the supplied channel tag applied to it.
     * The default value for channelTagId is {@link Aeron#NULL_VALUE}
     *
     * @param channelTagId tag to apply to the archive's subscription.
     * @return this for a fluent API
     */
    public ReplicationParams channelTagId(final long channelTagId)
    {
        this.channelTagId = channelTagId;
        return this;
    }

    /**
     * Gets channel tag id for the archive subscription.
     *
     * @return channel tag id.
     */
    public long channelTagId()
    {
        return channelTagId;
    }

    /**
     * The channel used by the archive's subscription for replication will have the supplied subscription tag applied to
     * it. The default value for subscriptionTagId is {@link Aeron#NULL_VALUE}
     *
     * @param subscriptionTagId tag to apply to the archive's subscription.
     * @return this for a fluent API
     */
    public ReplicationParams subscriptionTagId(final long subscriptionTagId)
    {
        this.subscriptionTagId = subscriptionTagId;
        return this;
    }

    /**
     * Gets subscription tag id for the archive subscription.
     *
     * @return subscription tag id.
     */
    public long subscriptionTagId()
    {
        return subscriptionTagId;
    }
}
