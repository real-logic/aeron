/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public class StubClusteredService implements ClusteredService
{
    protected Cluster cluster;

    public void onStart(final Cluster cluster)
    {
        this.cluster = cluster;
    }

    public void onSessionOpen(final ClientSession session, final long timestampMs)
    {
    }

    public void onSessionClose(final ClientSession session, final long timestampMs, final CloseReason closeReason)
    {
    }

    public void onSessionMessage(
        final ClientSession session,
        final long correlationId,
        final long timestampMs,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
    }

    public void onTimerEvent(final long correlationId, final long timestampMs)
    {
    }

    public void onTakeSnapshot(final Publication snapshotPublication)
    {
    }

    public void onLoadSnapshot(final Image snapshotImage)
    {
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }
}
