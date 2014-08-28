package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

interface ManagedBuffer extends AutoCloseable
{
    AtomicBuffer buffer();

    void close();
}
