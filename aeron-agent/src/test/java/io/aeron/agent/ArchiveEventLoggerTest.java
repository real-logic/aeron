package io.aeron.agent;

import io.aeron.archive.codecs.ControlResponseDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static io.aeron.agent.ArchiveEventCode.CMD_OUT_RESPONSE;
import static io.aeron.agent.ArchiveEventCode.EVENT_CODE_TYPE;
import static io.aeron.agent.ArchiveEventLogger.CONTROL_REQUEST_EVENTS;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;
import static io.aeron.archive.codecs.ControlResponseDecoder.BLOCK_LENGTH;
import static io.aeron.archive.codecs.ControlResponseDecoder.SCHEMA_VERSION;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.typeOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;

class ArchiveEventLoggerTest
{

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void toEventCodeId(final ArchiveEventCode code)
    {
        assertEquals(0xFFFF + EVENT_CODE_TYPE + code.id(), ArchiveEventLogger.toEventCodeId(code));
    }

    @Test
    void logControlResponse()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirect(2048 + TRAILER_LENGTH));
        final ArchiveEventLogger logger = new ArchiveEventLogger(new ManyToOneRingBuffer(buffer));

        logger.logControlResponse(
            Long.MIN_VALUE,
            -100,
            Long.MAX_VALUE,
            RECORDING_UNKNOWN,
            "the ERROR message");

        assertEquals(ArchiveEventLogger.toEventCodeId(CMD_OUT_RESPONSE), buffer.getInt(typeOffset(0), LITTLE_ENDIAN));
        final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
        controlResponseDecoder.wrap(buffer, encodedMsgOffset(0), BLOCK_LENGTH, SCHEMA_VERSION);
        assertEquals(Long.MIN_VALUE, controlResponseDecoder.controlSessionId());
        assertEquals(-100, controlResponseDecoder.correlationId());
        assertEquals(Long.MAX_VALUE, controlResponseDecoder.relevantId());
        assertEquals(RECORDING_UNKNOWN, controlResponseDecoder.code());
        assertEquals("the ERROR message", controlResponseDecoder.errorMessage());
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = EXCLUDE, names = { "CMD_OUT_RESPONSE" })
    void controlRequestEvents(final ArchiveEventCode code)
    {
        assertTrue(CONTROL_REQUEST_EVENTS.contains(code));
    }

    @ParameterizedTest
    @EnumSource(value = ArchiveEventCode.class, mode = INCLUDE, names = { "CMD_OUT_RESPONSE" })
    void nonControlRequestEvents(final ArchiveEventCode code)
    {
        assertFalse(CONTROL_REQUEST_EVENTS.contains(code));
    }
}