package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

@FunctionalInterface
interface DissectFunction
{
    void dissect(EventCode event, MutableDirectBuffer buffer, int offset, StringBuilder builder);
}
