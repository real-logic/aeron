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
package io.aeron.driver.cmd;

import io.aeron.driver.Receiver;
import io.aeron.driver.media.ReceiveChannelEndpoint;

public class AddSubscriptionAndSessionCmd implements ReceiverCmd
{
    private final ReceiveChannelEndpoint channelEndpoint;
    private final int streamId;
    private final int sessionId;

    public AddSubscriptionAndSessionCmd(
        final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        this.channelEndpoint = channelEndpoint;
        this.streamId = streamId;
        this.sessionId = sessionId;
    }

    public void execute(final Receiver receiver)
    {
        receiver.onAddSubscription(channelEndpoint, streamId, sessionId);
    }
}
