
/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.cluster.service;

import io.aeron.BufferBuilder;
import io.aeron.cluster.client.ClusterException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.FrameDescriptor.END_FRAG_FLAG;

class LogFragmentAssembler implements ControlledFragmentHandler
{
    private final ControlledFragmentHandler delegate;
    private final BufferBuilder builder;

    LogFragmentAssembler(final ControlledFragmentHandler delegate)
    {
        this.delegate = delegate;
        this.builder = new BufferBuilder();
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final byte flags = header.flags();
        Action action = Action.CONTINUE;

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            action = delegate.onFragment(buffer, offset, length, header);
        }
        else
        {
            if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
            {
                builder.reset().append(buffer, offset, length);
            }
            else
            {
                final int limit = builder.limit();
                if (0 == limit)
                {
                    throw new ClusterException("unexpected beginning of message at position " + header.position());
                }
                else
                {
                    builder.append(buffer, offset, length);

                    if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                    {
                        action = delegate.onFragment(builder.buffer(), 0, builder.limit(), header);

                        if (Action.ABORT == action)
                        {
                            builder.limit(limit);
                        }
                        else
                        {
                            builder.reset();
                        }
                    }
                }
            }
        }

        return action;
    }
}
