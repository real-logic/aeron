package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

public interface ManagedBuffer extends AutoCloseable
{
    AtomicBuffer buffer();

    void close();
}
