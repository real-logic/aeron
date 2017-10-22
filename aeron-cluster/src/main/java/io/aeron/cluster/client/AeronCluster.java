/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.cluster.client;

import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.CloseHelper;

public final class AeronCluster implements AutoCloseable
{
    private final long sessionId;
    private final Context ctx;
    private final Subscription subscription;
    private final Publication publication;

    /**
     * Connect to the cluster using default configuration.
     *
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect()
    {
        return connect(new Context());
    }

    /**
     * Connect to the cluster providing {@link Context} for configuration.
     *
     * @param ctx for configuration.
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect(final AeronCluster.Context ctx)
    {
        return null;
    }

    private AeronCluster(
        final Context ctx, final long sessionId, final Subscription subscription, final Publication publication)
    {
        this.ctx = ctx;
        this.sessionId = sessionId;
        this.subscription = subscription;
        this.publication = publication;
    }

    /**
     * Close session and release associated resources.
     */
    public void close()
    {
        CloseHelper.close(subscription);
        CloseHelper.close(publication);
        CloseHelper.close(ctx);
    }

    /**
     * Cluster session id for the session that was opened as the result of a successful connect.
     *
     * @return session id for the session that was opened as the result of a successful connect.
     */
    public long sessionId()
    {
        return sessionId;
    }

    /**
     * Get the raw {@link Publication} for sending to the cluster.
     * <p>
     * This can be wrapped with a {@link ClusterPublication} for prepending the cluster session header to messages.
     *
     * @return the raw {@link Publication} for connecting to the cluster.
     */
    public Publication publication()
    {
        return publication;
    }

    /**
     * Get the raw {@link Subscription} for receiving from the cluster.
     *
     * @return the raw {@link Subscription} for receiving from the cluster.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    public static class Configuration
    {

    }

    public static class Context implements AutoCloseable
    {
        public void conclude()
        {

        }

        public void close()
        {

        }
    }
}