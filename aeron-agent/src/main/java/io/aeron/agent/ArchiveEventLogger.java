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

import io.aeron.archive.codecs.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import static io.aeron.agent.ArchiveEventCode.*;

public final class ArchiveEventLogger
{
    static final long ENABLED_EVENT_CODES = EventConfiguration.getEnabledArchiveEventCodes();
    public static final ArchiveEventLogger LOGGER = new ArchiveEventLogger(EventConfiguration.EVENT_RING_BUFFER);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ManyToOneRingBuffer ringBuffer;

    private ArchiveEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    public void logControlRequest(final DirectBuffer buffer, final int offset, final int length)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        switch (templateId)
        {
            case ConnectRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_CONNECT);
                break;

            case CloseSessionRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_CLOSE_SESSION);
                break;

            case StartRecordingRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_START_RECORDING);
                break;

            case StopRecordingRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_STOP_RECORDING);
                break;

            case ReplayRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_REPLAY);
                break;

            case StopReplayRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_STOP_REPLAY);
                break;

            case ListRecordingsRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_LIST_RECORDINGS);
                break;

            case ListRecordingsForUriRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_LIST_RECORDINGS_FOR_URI);
                break;

            case ListRecordingRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_LIST_RECORDING);
                break;

            case ExtendRecordingRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_EXTEND_RECORDING);
                break;

            case RecordingPositionRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_RECORDING_POSITION);
                break;

            case TruncateRecordingRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_TRUNCATE_RECORDING);
                break;

            case StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_STOP_RECORDING_SUBSCRIPTION);
                break;

            case StopPositionRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_STOP_POSITION);
                break;

            case FindLastMatchingRecordingRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_FIND_LAST_MATCHING_RECORD);
                break;

            case ListRecordingSubscriptionsRequestDecoder.TEMPLATE_ID:
                dispatchIfEnabled(buffer, offset, length, CMD_IN_LIST_RECORDING_SUBSCRIPTIONS);
                break;

            default:
                throw new IllegalArgumentException("Unknown template id: " + templateId);
        }
    }

    private void dispatchIfEnabled(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ArchiveEventCode eventCode)
    {
        if (ArchiveEventCode.isEnabled(eventCode, ENABLED_EVENT_CODES))
        {
            ringBuffer.write(toEventCodeId(eventCode), buffer, offset, length);
        }
    }

    private static int toEventCodeId(final ArchiveEventCode code)
    {
        return ArchiveEventCode.EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}
