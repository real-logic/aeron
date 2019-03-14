package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

@FunctionalInterface
interface DissectFunction<T>
{
    void dissect(T event, MutableDirectBuffer buffer, int offset, StringBuilder builder);
}
