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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.collections.Int2ObjectHashMap;

import java.util.Collection;

/**
 * Subscriptions maintained per stream for receiver processing from dispatcher
 */
public class DispatcherSubscription
{
    private final int streamId;
    private final DriverConductorProxy conductorProxy;
    private final Int2ObjectHashMap<DriverConnection> connectionBySessionIdMap = new Int2ObjectHashMap<>();

    public DispatcherSubscription(final int streamId, final DriverConductorProxy conductorProxy)
    {
        this.streamId = streamId;
        this.conductorProxy = conductorProxy;
    }

    public DriverConnection getConnection(final int sessionId)
    {
        return connectionBySessionIdMap.get(sessionId);
    }

    public DriverConnection putConnection(final DriverConnection connection)
    {
        return connectionBySessionIdMap.put(connection.sessionId(), connection);
    }

    public DriverConnection removeConnection(final int sessionId)
    {
        return connectionBySessionIdMap.remove(sessionId);
    }

    public int streamId()
    {
        return streamId;
    }

    public void close()
    {
        conductorProxy.removeSubscription(this);
    }

    public Collection<DriverConnection> connections()
    {
        return connectionBySessionIdMap.values();
    }
}
