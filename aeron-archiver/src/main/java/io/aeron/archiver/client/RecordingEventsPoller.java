/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.archiver.client;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archiver.codecs.MessageHeaderDecoder;
import io.aeron.archiver.codecs.RecordingProgressDecoder;
import io.aeron.archiver.codecs.RecordingStartedDecoder;
import io.aeron.archiver.codecs.RecordingStoppedDecoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling, decoding, and dispatching of recording events.
 */
public class RecordingEventsPoller
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();
    private final RecordingProgressDecoder recordingProgressDecoder = new RecordingProgressDecoder();
    private final RecordingStoppedDecoder recordingStoppedDecoder = new RecordingStoppedDecoder();

    private final int fragmentLimit;
    private final RecordingEventsListener listener;
    private final Subscription subscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);

    /**
     * Create a poller for a given subscription to an archive for recording events.
     *
     * @param listener      to which events are dispatched.
     * @param subscription  to poll for new events.
     * @param fragmentLimit to apply for each polling operation.
     */
    public RecordingEventsPoller(
        final RecordingEventsListener listener,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this.fragmentLimit = fragmentLimit;
        this.listener = listener;
        this.subscription = subscription;
    }

    /**
     * Poll for recording events and dispatch them to the {@link RecordingEventsListener} for this instance.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        return subscription.poll(fragmentAssembler, fragmentLimit);
    }

    private void onFragment(
        final DirectBuffer buffer,
        final int offset,
        @SuppressWarnings("unused") final int length,
        @SuppressWarnings("unused") final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case RecordingStartedDecoder.TEMPLATE_ID:
                recordingStartedDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onStart(
                    recordingStartedDecoder.recordingId(),
                    recordingStartedDecoder.joinPosition(),
                    recordingStartedDecoder.sessionId(),
                    recordingStartedDecoder.streamId(),
                    recordingStartedDecoder.channel(),
                    recordingStartedDecoder.sourceIdentity());
                break;

            case RecordingProgressDecoder.TEMPLATE_ID:
                recordingProgressDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onProgress(
                    recordingProgressDecoder.recordingId(),
                    recordingProgressDecoder.joinPosition(),
                    recordingProgressDecoder.position());
                break;

            case RecordingStoppedDecoder.TEMPLATE_ID:
                recordingStoppedDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onStop(
                    recordingStoppedDecoder.recordingId(),
                    recordingStoppedDecoder.joinPosition(),
                    recordingStoppedDecoder.endPosition());
                break;

            default:
                throw new IllegalStateException("Unknown templateId: " + templateId);
        }
    }
}
