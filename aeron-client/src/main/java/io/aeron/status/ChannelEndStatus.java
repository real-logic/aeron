package io.aeron.status;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ChannelEndStatus
{
    private static final int CHANNEL_STATUS_ID_OFFSET = 0;
    private static final int BIND_ADDRESS_AND_PORT_OFFSET = CHANNEL_STATUS_ID_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int BIND_ADDRESS_AND_PORT_STRING_OFFSET = BIND_ADDRESS_AND_PORT_OFFSET + BitUtil.SIZE_OF_INT;

    private static final int MAX_IPV6_LENGTH = "[ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255]:65536".length();

    /**
     * Maximum possible length for a key, reserve this much space in the key buffer on creation to allow
     * for updating later.
     */
    public static final int KEY_RESERVED_LENGTH = BitUtil.SIZE_OF_INT * 2 + MAX_IPV6_LENGTH;

    public static final int RECEIVE_END_STATUS_TYPE_ID = 14;

    public static final int SEND_END_STATUS_TYPE_ID = 15;

    private static final byte[] RESERVED_KEY_BYTES = new byte[KEY_RESERVED_LENGTH];

    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final int channelStatusId,
        final String name,
        final int typeId)
    {
        tempBuffer.putInt(0, channelStatusId);
        tempBuffer.putInt(BIND_ADDRESS_AND_PORT_OFFSET, 0); // Zero-length address initially.
        tempBuffer.putBytes(BIND_ADDRESS_AND_PORT_STRING_OFFSET, RESERVED_KEY_BYTES); // Maybe don't need this.

        final int keyLength = KEY_RESERVED_LENGTH;

        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, ": ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, String.valueOf(channelStatusId));
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");

        return countersManager.newCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelLength);
    }

    public static void updateWithBindAddress(final AtomicCounter counter, final String bindAddressAndPort)
    {
        if (bindAddressAndPort.length() > MAX_IPV6_LENGTH)
        {
            throw new IllegalArgumentException(
                "bindAddressAndPort value too long: " + bindAddressAndPort.length() + " max: " + MAX_IPV6_LENGTH);
        }

        counter.updateKey((keyBuffer) ->
        {
            final int bindingLength = keyBuffer.putStringWithoutLengthAscii(
                BIND_ADDRESS_AND_PORT_STRING_OFFSET, bindAddressAndPort);
            keyBuffer.putInt(BIND_ADDRESS_AND_PORT_OFFSET, bindingLength);
        });

        counter.appendToLabel(bindAddressAndPort);
    }

    public static int channelStatusId(final DirectBuffer keyBuffer, final int offset)
    {
        return keyBuffer.getInt(offset + CHANNEL_STATUS_ID_OFFSET);
    }

    public static String bindAddressAndPort(final DirectBuffer keyBuffer, final int offset)
    {
        final int bindingLength = keyBuffer.getInt(offset + BIND_ADDRESS_AND_PORT_OFFSET);
        return 0 != bindingLength ?
            keyBuffer.getStringWithoutLengthAscii(BIND_ADDRESS_AND_PORT_STRING_OFFSET, bindingLength) : null;
    }

    public static List<String> findChannelEnds(
        final CountersReader countersReader,
        final long channelStatus,
        final int candidateTypeId,
        final int channelStatusId)
    {
        if (channelStatus != ChannelEndpointStatus.ACTIVE)
        {
            return Collections.emptyList();
        }

        final List<String> bindings = new ArrayList<>(2);

        countersReader.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (candidateTypeId == typeId &&
                channelStatusId == channelStatusId(keyBuffer, 0) &&
                ChannelEndpointStatus.ACTIVE == countersReader.getCounterValue(counterId))
            {
                final String bindAddressAndPort = bindAddressAndPort(keyBuffer, 0);
                if (null != bindAddressAndPort)
                {
                    bindings.add(bindAddressAndPort);
                }
            }
        });

        return bindings;
    }
}
