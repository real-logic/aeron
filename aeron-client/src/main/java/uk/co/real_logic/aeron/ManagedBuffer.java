package uk.co.real_logic.aeron;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

interface ManagedBuffer extends AutoCloseable
{
    UnsafeBuffer buffer();

    void close();
}
