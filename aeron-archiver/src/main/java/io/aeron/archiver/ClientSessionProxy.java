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
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.*;
import org.agrona.*;
import org.agrona.concurrent.*;

class ClientSessionProxy
{
    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private final IdleStrategy idleStrategy;
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();

    ClientSessionProxy(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
    }

    void sendResponse(final ExclusivePublication reply, final String err, final long correlationId)
    {
        responseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId);

        if (!Strings.isEmpty(err))
        {
            responseEncoder.err(err);
        }

        final int length = HEADER_LENGTH + responseEncoder.encodedLength();
        while (true)
        {
            final long result = reply.offer(buffer, 0, length);
            if (result > 0)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + reply);
            }

            idleStrategy.idle();
        }
    }
}
