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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

/**
 * Events that can be enabled for logging in the archive module.
 */
public enum ArchiveEventCode implements EventCode
{
    CMD_IN_CONNECT(1, ArchiveEventDissector::controlRequest),
    CMD_IN_CLOSE_SESSION(2, ArchiveEventDissector::controlRequest),
    CMD_IN_START_RECORDING(3, ArchiveEventDissector::controlRequest),
    CMD_IN_STOP_RECORDING(4, ArchiveEventDissector::controlRequest),
    CMD_IN_REPLAY(5, ArchiveEventDissector::controlRequest),
    CMD_IN_STOP_REPLAY(6, ArchiveEventDissector::controlRequest),
    CMD_IN_LIST_RECORDINGS(7, ArchiveEventDissector::controlRequest),
    CMD_IN_LIST_RECORDINGS_FOR_URI(8, ArchiveEventDissector::controlRequest),
    CMD_IN_LIST_RECORDING(9, ArchiveEventDissector::controlRequest),
    CMD_IN_EXTEND_RECORDING(10, ArchiveEventDissector::controlRequest),
    CMD_IN_RECORDING_POSITION(11, ArchiveEventDissector::controlRequest),
    CMD_IN_TRUNCATE_RECORDING(12, ArchiveEventDissector::controlRequest),
    CMD_IN_STOP_RECORDING_SUBSCRIPTION(13, ArchiveEventDissector::controlRequest),
    CMD_IN_STOP_POSITION(14, ArchiveEventDissector::controlRequest),
    CMD_IN_FIND_LAST_MATCHING_RECORD(15, ArchiveEventDissector::controlRequest),
    CMD_IN_LIST_RECORDING_SUBSCRIPTIONS(16, ArchiveEventDissector::controlRequest);

    static final int EVENT_CODE_TYPE = EventCodeType.ARCHIVE.getTypeCode();
    private static final int MAX_ID = 63;
    private static final ArchiveEventCode[] EVENT_CODE_BY_ID = new ArchiveEventCode[MAX_ID];

    private final long tagBit;
    private final int id;
    private final DissectFunction<ArchiveEventCode> dissector;

    static
    {
        for (final ArchiveEventCode code : ArchiveEventCode.values())
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    ArchiveEventCode(final int id, final DissectFunction<ArchiveEventCode> dissector)
    {
        this.id = id;
        this.tagBit = 1L << id;
        this.dissector = dissector;
    }

    static ArchiveEventCode get(final int eventCodeId)
    {
        return EVENT_CODE_BY_ID[eventCodeId];
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    public long tagBit()
    {
        return tagBit;
    }

    /**
     * {@inheritDoc}
     */
    public EventCodeType eventCodeType()
    {
        return EventCodeType.ARCHIVE;
    }

    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }

    public static boolean isEnabled(final ArchiveEventCode code, final long mask)
    {
        return (mask & code.tagBit()) == code.tagBit();
    }
}
