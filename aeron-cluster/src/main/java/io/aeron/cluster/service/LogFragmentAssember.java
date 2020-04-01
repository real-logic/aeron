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
