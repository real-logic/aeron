package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.concurrent.UnsafeBuffer;

interface ManagedBuffer extends AutoCloseable
{
    UnsafeBuffer buffer();

    void close();
}
