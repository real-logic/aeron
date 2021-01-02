/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.agent;

import io.aeron.ErrorCode;
import io.aeron.command.*;
import io.aeron.protocol.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetSocketAddress;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventDissector.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.*;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverEventDissectorTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);
    private final StringBuilder builder = new StringBuilder();

    @Test
    void dissectRemovePublicationCleanup()
    {
        final int offset = 12;
        internalEncodeLogHeader(buffer, offset, 22, 88, () -> 2_500_000_000L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 42, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, 11, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, "channel uri");

        DriverEventDissector.dissectRemovePublicationCleanup(buffer, offset, builder);

        assertEquals("[2.5] " + CONTEXT + ": " + REMOVE_PUBLICATION_CLEANUP.name() +
            " [22/88]: sessionId=42, streamId=11, uri=channel uri",
            builder.toString());
    }

    @Test
    void dissectRemoveSubscriptionCleanup()
    {
        final int offset = 16;
        internalEncodeLogHeader(buffer, offset, 100, 100, () -> 100_000_000L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 33, LITTLE_ENDIAN);
        buffer.putLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, 111_111_111_111L, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, "test");

        DriverEventDissector.dissectRemoveSubscriptionCleanup(buffer, offset, builder);

        assertEquals("[0.1] " + CONTEXT + ": " + REMOVE_SUBSCRIPTION_CLEANUP.name() +
            " [100/100]: streamId=33, id=111111111111, uri=test",
            builder.toString());
    }

    @Test
    void dissectRemoveImageCleanup()
    {
        final int offset = 32;
        internalEncodeLogHeader(buffer, offset, 66, 99, () -> 12345678900L);
        buffer.putInt(offset + LOG_HEADER_LENGTH, 77, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, 55, LITTLE_ENDIAN);
        buffer.putLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, 1_000_000L, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, "URI");

        DriverEventDissector.dissectRemoveImageCleanup(buffer, offset, builder);

        assertEquals("[12.3456789] " + CONTEXT + ": " + REMOVE_IMAGE_CLEANUP.name() +
            " [66/99]: sessionId=77, streamId=55, id=1000000, uri=URI",
            builder.toString());
    }

    @Test
    void dissectFrameTypePad()
    {
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8080));
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_PAD);
        flyweight.flags((short)13);
        flyweight.frameLength(100);
        flyweight.sessionId(42);
        flyweight.streamId(5);
        flyweight.termId(16);
        flyweight.termOffset(1045);

        dissectFrame(CMD_IN_ADD_COUNTER, buffer, 0, builder);

        assertEquals("[1.0] " + CONTEXT + ": " + CMD_IN_ADD_COUNTER.name() + " [5/5]: 127.0.0.1:8080 " +
            "PAD 00001101 len 100 42:5:16 @1045",
            builder.toString());
    }

    @Test
    void dissectFrameTypeData()
    {
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_DATA);
        flyweight.flags((short)23);
        flyweight.frameLength(77);
        flyweight.sessionId(12);
        flyweight.streamId(51);
        flyweight.termId(6);
        flyweight.termOffset(444);

        dissectFrame(CMD_IN_ADD_PUBLICATION, buffer, 0, builder);

        assertEquals("[1.0] " + CONTEXT + ": " + CMD_IN_ADD_PUBLICATION.name() + " [5/5]: 127.0.0.1:8888 " +
            "DATA 00010111 len 77 12:51:6 @444",
            builder.toString());
    }

    @Test
    void dissectFrameTypeStatusMessage()
    {
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final StatusMessageFlyweight flyweight = new StatusMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_SM);
        flyweight.flags((short)7);
        flyweight.frameLength(121);
        flyweight.sessionId(5);
        flyweight.streamId(8);
        flyweight.consumptionTermId(4);
        flyweight.consumptionTermOffset(18);
        flyweight.receiverWindowLength(2048);
        flyweight.receiverId(11);

        dissectFrame(CMD_OUT_ERROR, buffer, 0, builder);

        assertEquals("[1.0] " + CONTEXT + ": " + CMD_OUT_ERROR.name() + " [5/5]: 127.0.0.1:8888 " +
            "SM 00000111 len 121 5:8:4 @18 2048 11",
            builder.toString());
    }

    @Test
    void dissectFrameTypeNak()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final NakFlyweight flyweight = new NakFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_NAK);
        flyweight.flags((short)2);
        flyweight.frameLength(54);
        flyweight.sessionId(5);
        flyweight.streamId(8);
        flyweight.termId(20);
        flyweight.termOffset(0);
        flyweight.length(999999);

        dissectFrame(CMD_OUT_ERROR, buffer, 0, builder);

        assertEquals("[3.0] " + CONTEXT + ": " + CMD_OUT_ERROR.name() + " [3/3]: 127.0.0.1:8888 " +
            "NAK 00000010 len 54 5:8:20 @0 999999",
            builder.toString());
    }

    @Test
    void dissectFrameTypeSetup()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final SetupFlyweight flyweight = new SetupFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_SETUP);
        flyweight.flags((short)200);
        flyweight.frameLength(1);
        flyweight.sessionId(15);
        flyweight.streamId(18);
        flyweight.activeTermId(81);
        flyweight.initialTermId(69);
        flyweight.termOffset(10);
        flyweight.termLength(444);
        flyweight.mtuLength(8096);
        flyweight.ttl(20_000);

        dissectFrame(CMD_OUT_EXCLUSIVE_PUBLICATION_READY, buffer, 0, builder);

        assertEquals("[3.0] " + CONTEXT + ": " + CMD_OUT_EXCLUSIVE_PUBLICATION_READY.name() +
            " [3/3]: 127.0.0.1:8888 " +
            "SETUP 11001000 len 1 15:18:81 69 @10 444 MTU 8096 TTL 20000",
            builder.toString());
    }

    @Test
    void dissectFrameTypeRtt()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final RttMeasurementFlyweight flyweight = new RttMeasurementFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(HDR_TYPE_RTTM);
        flyweight.flags((short)20);
        flyweight.frameLength(100);
        flyweight.sessionId(0);
        flyweight.streamId(1);
        flyweight.echoTimestampNs(123456789);
        flyweight.receptionDelta(354);
        flyweight.receiverId(22);

        dissectFrame(CMD_IN_ADD_RCV_DESTINATION, buffer, 0, builder);

        assertEquals("[3.0] " + CONTEXT + ": " + CMD_IN_ADD_RCV_DESTINATION.name() + " [3/3]: 127.0.0.1:8888 " +
            "RTT 00010100 len 100 0:1 123456789 354 22",
            builder.toString());
    }

    @Test
    void dissectFrameTypeUnknown()
    {
        internalEncodeLogHeader(buffer, 0, 3, 3, () -> 3_000_000_000L);
        final int socketAddressOffset = encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH, new InetSocketAddress("localhost", 8888));
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH + socketAddressOffset, 300);
        flyweight.headerType(Integer.MAX_VALUE);

        dissectFrame(CMD_OUT_ON_OPERATION_SUCCESS, buffer, 0, builder);

        assertEquals("[3.0] " + CONTEXT + ": " + CMD_OUT_ON_OPERATION_SUCCESS.name() + " [3/3]: 127.0.0.1:8888 " +
            "FRAME_UNKNOWN: " + HDR_TYPE_EXT,
            builder.toString());
    }

    @Test
    void dissectString()
    {
        internalEncodeLogHeader(buffer, 0, 1, 1, () -> 1_100_000_000L);
        buffer.putStringAscii(LOG_HEADER_LENGTH, "Hello, World!");

        DriverEventDissector.dissectString(CMD_IN_CLIENT_CLOSE, buffer, 0, builder);

        assertEquals("[1.1] " + CONTEXT + ": " + CMD_IN_CLIENT_CLOSE.name() + " [1/1]: Hello, World!",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_PUBLICATION", "CMD_IN_ADD_EXCLUSIVE_PUBLICATION" })
    void dissectCommandPublication(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, 10, 10, () -> 1_780_000_000L);
        final PublicationMessageFlyweight flyweight = new PublicationMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.channel("pub channel");
        flyweight.streamId(3);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(15);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.78] " + CONTEXT + ": " + eventCode.name() + " [10/10]: " +
            "3 [" + eventCode.id() + ":15] pub channel",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_SUBSCRIPTION" })
    void dissectCommandSubscription(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 10, () -> 1_780_000_000L);
        final SubscriptionMessageFlyweight flyweight = new SubscriptionMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.channel("sub channel");
        flyweight.streamId(31);
        flyweight.registrationCorrelationId(90);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(6);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.78] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/10]: " +
            "31 [90][" + eventCode.id() + ":6] sub channel",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class,
        names = { "CMD_IN_REMOVE_PUBLICATION", "CMD_IN_REMOVE_SUBSCRIPTION", "CMD_IN_REMOVE_COUNTER" })
    void dissectCommandRemoveEvent(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 87, () -> 21_032_000_000L);
        final RemoveMessageFlyweight flyweight = new RemoveMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.registrationId(11);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(16);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/87]: " +
            "11 [" + eventCode.id() + ":16]",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class,
        names = { "CMD_OUT_PUBLICATION_READY", "CMD_OUT_EXCLUSIVE_PUBLICATION_READY" })
    void dissectCommandPublicationReady(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 21_032_000_000L);
        final PublicationBuffersReadyFlyweight flyweight = new PublicationBuffersReadyFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.sessionId(eventCode.ordinal());
        flyweight.streamId(42);
        flyweight.publicationLimitCounterId(1);
        flyweight.channelStatusCounterId(5);
        flyweight.correlationId(8);
        flyweight.registrationId(eventCode.id());
        flyweight.logFileName("log.txt");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            eventCode.ordinal() + ":42 1 5 [8 " + eventCode.id() + "] log.txt",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_AVAILABLE_IMAGE" })
    void dissectCommandImageReady(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 21_032_000_000L);
        final ImageBuffersReadyFlyweight flyweight = new ImageBuffersReadyFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.sessionId(eventCode.ordinal());
        flyweight.streamId(22);
        flyweight.subscriberPositionId(0);
        flyweight.subscriptionRegistrationId(245);
        flyweight.correlationId(767);
        flyweight.logFileName("log2.txt");
        flyweight.sourceIdentity("source identity");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            eventCode.ordinal() + ":22 [0:245] [767] log2.txt source identity",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_OPERATION_SUCCESS" })
    void dissectCommandOperationSuccess(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 111, () -> 21_032_000_000L);
        final OperationSucceededFlyweight flyweight = new OperationSucceededFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.correlationId(eventCode.id());

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/111]: " +
            eventCode.id(),
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_KEEPALIVE_CLIENT", "CMD_IN_CLIENT_CLOSE" })
    void dissectCommandCorrelationEvent(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 21_032_000_000L);
        final CorrelatedMessageFlyweight flyweight = new CorrelatedMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(2);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "[" + eventCode.id() + ":2]",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_UNAVAILABLE_IMAGE" })
    void dissectCommandImage(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 99, () -> 21_032_000_000L);
        final ImageMessageFlyweight flyweight = new ImageMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.streamId(300);
        flyweight.correlationId(eventCode.id());
        flyweight.subscriptionRegistrationId(-19);
        flyweight.channel("the channel");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/99]: " +
            "300 [" + eventCode.id() + " -19] the channel",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = {
        "CMD_IN_ADD_DESTINATION",
        "CMD_IN_REMOVE_DESTINATION",
        "CMD_IN_ADD_RCV_DESTINATION",
        "CMD_IN_REMOVE_RCV_DESTINATION" })
    void dissectCommandDestination(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 77, () -> 21_032_000_000L);
        final DestinationMessageFlyweight flyweight = new DestinationMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.channel("dst");
        flyweight.registrationCorrelationId(eventCode.id());
        flyweight.clientId(1010101);
        flyweight.correlationId(404);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[21.032] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/77]: " +
            eventCode.id() + " [1010101:404] dst",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ERROR" })
    void dissectCommandError(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final ErrorResponseFlyweight flyweight = new ErrorResponseFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.offendingCommandCorrelationId(eventCode.id());
        flyweight.errorCode(ErrorCode.MALFORMED_COMMAND);
        flyweight.errorMessage("Huge stacktrace!");

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.9] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            eventCode.id() + " " + ErrorCode.MALFORMED_COMMAND + " Huge stacktrace!",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_ADD_COUNTER" })
    void dissectCommandCounter(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final CounterMessageFlyweight flyweight = new CounterMessageFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.typeId(3);
        flyweight.keyBuffer(newBuffer(new byte[20]), 0, 10);
        flyweight.labelBuffer(newBuffer(new byte[100]), 26, 13);
        flyweight.clientId(eventCode.id());
        flyweight.correlationId(42);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.9] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "3 [" + flyweight.keyBufferOffset() + " 10][" + flyweight.labelBufferOffset() + " 13][" +
            eventCode.id() + ":42]",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_SUBSCRIPTION_READY" })
    void dissectCommandSubscriptionReady(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final SubscriptionReadyFlyweight flyweight = new SubscriptionReadyFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.correlationId(42);
        flyweight.channelStatusCounterId(eventCode.id());

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.9] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            "42 " + eventCode.id(),
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_COUNTER_READY", "CMD_OUT_ON_UNAVAILABLE_COUNTER" })
    void dissectCommandCounterUpdate(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final CounterUpdateFlyweight flyweight = new CounterUpdateFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.correlationId(eventCode.id());
        flyweight.counterId(18);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.9] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            eventCode.id() + " 18",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_OUT_ON_CLIENT_TIMEOUT" })
    void dissectCommandClientTimeout(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final ClientTimeoutFlyweight flyweight = new ClientTimeoutFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.clientId(eventCode.id());

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.9] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            eventCode.id(),
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = DriverEventCode.class, names = { "CMD_IN_TERMINATE_DRIVER" })
    void dissectCommandTerminateDriver(final DriverEventCode eventCode)
    {
        internalEncodeLogHeader(buffer, 0, eventCode.ordinal(), 100, () -> 1_900_000_000L);
        final TerminateDriverFlyweight flyweight = new TerminateDriverFlyweight();
        flyweight.wrap(buffer, LOG_HEADER_LENGTH);
        flyweight.clientId(eventCode.id());
        flyweight.tokenBuffer(newBuffer(new byte[15]), 4, 11);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.9] " + CONTEXT + ": " + eventCode.name() + " [" + eventCode.ordinal() + "/100]: " +
            eventCode.id() + " 11",
            builder.toString());
    }

    @Test
    void dissectCommandUnknown()
    {
        final DriverEventCode eventCode = SEND_CHANNEL_CREATION;
        internalEncodeLogHeader(buffer, 0, 5, 5, () -> 1_000_000_000L);

        dissectCommand(eventCode, buffer, 0, builder);

        assertEquals("[1.0] " + CONTEXT + ": " + eventCode.name() + " [5/5]: COMMAND_UNKNOWN: " + eventCode,
            builder.toString());
    }

    @Test
    void dissectUntetheredSubscriptionStateChange()
    {
        final int offset = 12;
        internalEncodeLogHeader(buffer, offset, 22, 88, () -> 1_500_000_000L);
        buffer.putLong(offset + LOG_HEADER_LENGTH, 88, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG, 123, LITTLE_ENDIAN);
        buffer.putInt(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT, 777, LITTLE_ENDIAN);
        buffer.putStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG + 2 * SIZE_OF_INT, "state changed");

        DriverEventDissector.dissectUntetheredSubscriptionStateChange(buffer, offset, builder);

        assertEquals("[1.5] " + CONTEXT + ": " + UNTETHERED_SUBSCRIPTION_STATE_CHANGE.name() +
            " [22/88]: subscriptionId=88, streamId=123, sessionId=777, state changed",
            builder.toString());
    }

    @Test
    void dissectAddress()
    {
        final int offset = 24;
        internalEncodeLogHeader(buffer, offset, 17, 27, () -> 2_500_000_000L);
        encodeSocketAddress(
            buffer, LOG_HEADER_LENGTH + offset, new InetSocketAddress("localhost", 4848));

        DriverEventDissector.dissectAddress(NAME_RESOLUTION_NEIGHBOR_ADDED, buffer, offset, builder);

        assertEquals("[2.5] " + CONTEXT + ": " + NAME_RESOLUTION_NEIGHBOR_ADDED.name() +
            " [17/27]: 127.0.0.1:4848", builder.toString());
    }

    private DirectBuffer newBuffer(final byte[] bytes)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(allocate(bytes.length));
        buffer.putBytes(0, bytes);
        return buffer;
    }
}
