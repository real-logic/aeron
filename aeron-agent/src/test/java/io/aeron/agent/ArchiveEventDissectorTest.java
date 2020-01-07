package io.aeron.agent;

import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArchiveEventDissectorTest
{
    @Test
    void controlResponse()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(1024, 32));
        final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
        responseEncoder.wrap(buffer, 16)
            .controlSessionId(13)
            .correlationId(42)
            .relevantId(8)
            .code(ControlResponseCode.NULL_VAL)
            .version(111)
            .errorMessage("the %ERR% msg");
        final StringBuilder builder = new StringBuilder();

        ArchiveEventDissector.controlResponse(buffer, 16, builder);

        assertEquals("ARCHIVE: CONTROL_RESPONSE" +
            ", controlSessionId=13" +
            ", correlationId=42" +
            ", relevantId=8" +
            ", code=" + ControlResponseCode.NULL_VAL +
            ", version=111" +
            ", errorMessage=the %ERR% msg",
            builder.toString());
    }
}