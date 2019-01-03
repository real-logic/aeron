/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.status;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Allocate a counter for tracking the last heartbeat of an entity.
 */
public class HeartbeatStatus
{
    /**
     * Offset in the key meta data for the registration id of the counter.
     */
    public static final int REGISTRATION_ID_OFFSET = 0;

    /**
     * Allocate a counter for tracking the last heartbeat of an entity.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which to allocated the underlying storage.
     * @param registrationId  to be associated with the counter.
     * @return a new {@link AtomicCounter} for tracking the last heartbeat.
     */
    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId)
    {
        return new AtomicCounter(
            countersManager.valuesBuffer(),
            allocateCounterId(tempBuffer, name, typeId, countersManager, registrationId),
            countersManager);
    }

    public static int allocateCounterId(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId)
    {
        tempBuffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
        final int keyLength = REGISTRATION_ID_OFFSET + SIZE_OF_LONG;

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, registrationId);

        return countersManager.allocate(
            typeId,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            labelOffset,
            labelLength);
    }
}
