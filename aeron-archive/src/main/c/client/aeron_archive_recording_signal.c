/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include "aeron_archive_recording_signal.h"
#include "aeron_archive_context.h"

#include "c/aeron_archive_client/recordingSignalEvent.h"
#include "util/aeron_error.h"

int aeron_archive_recording_signal_dispatch_buffer(aeron_archive_context_t *ctx, const uint8_t* buffer, size_t length)
{
    if (NULL != ctx->on_recording_signal)
    {
        struct aeron_archive_client_recordingSignalEvent recording_signal_event;

        aeron_archive_client_recordingSignalEvent_wrap_for_decode(
            &recording_signal_event,
            (char *)buffer,
            aeron_archive_client_messageHeader_encoded_length(),
            aeron_archive_client_recordingSignalEvent_sbe_block_length(),
            aeron_archive_client_recordingSignalEvent_sbe_schema_version(),
            length);

        aeron_archive_recording_signal_t signal;

        signal.control_session_id = aeron_archive_client_recordingSignalEvent_controlSessionId(&recording_signal_event);
        signal.recording_id = aeron_archive_client_recordingSignalEvent_recordingId(&recording_signal_event);
        signal.subscription_id = aeron_archive_client_recordingSignalEvent_subscriptionId(&recording_signal_event);
        signal.position = aeron_archive_client_recordingSignalEvent_position(&recording_signal_event);

        if (!aeron_archive_client_recordingSignalEvent_signal(
            &recording_signal_event,
            (enum aeron_archive_client_recordingSignal *)&signal.recording_signal_code))
        {
            AERON_SET_ERR(-1, "%s", "unable to read recording signal code");
            return -1;
        }

        ctx->on_recording_signal(&signal, ctx->on_recording_signal_clientd);
    }

    return 0;
}

void aeron_archive_recording_signal_dispatch_signal(aeron_archive_context_t *ctx, aeron_archive_recording_signal_t *signal)
{
    if (NULL != ctx->on_recording_signal)
    {
        ctx->on_recording_signal(signal, ctx->on_recording_signal_clientd);
    }
}

