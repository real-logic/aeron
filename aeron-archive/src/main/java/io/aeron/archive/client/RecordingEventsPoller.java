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
package io.aeron.archive.client;

import io.aeron.Subscription;
import io.aeron.archive.codecs.MessageHeaderDecoder;
import io.aeron.archive.codecs.RecordingProgressDecoder;
import io.aeron.archive.codecs.RecordingStartedDecoder;
import io.aeron.archive.codecs.RecordingStoppedDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling and decoding of recording events.
 */
public class RecordingEventsPoller implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();
    private final RecordingProgressDecoder recordingProgressDecoder = new RecordingProgressDecoder();
    private final RecordingStoppedDecoder recordingStoppedDecoder = new RecordingStoppedDecoder();

    private final Subscription subscription;
    private int templateId;
    private boolean pollComplete;

    /**
     * Create a poller for a given subscription to an archive for recording events.
     *
     * @param subscription  to poll for new events.
     */
    public RecordingEventsPoller(final Subscription subscription)
    {
        this.subscription = subscription;
    }

    /**
     * Poll for recording events and dispatch them to the {@link RecordingEventsListener} for this instance.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        templateId = -1;
        pollComplete = false;

        return subscription.poll(this, 1);
    }

    /**
     * Has the last polling action received a complete message?
     *
     * @return true of the last polling action received a complete message?
     */
    public boolean isPollComplete()
    {
        return pollComplete;
    }

    /**
     * Get the template id of the last received message.
     *
     * @return the template id of the last received message.
     */
    public int templateId()
    {
        return templateId;
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case RecordingStartedDecoder.TEMPLATE_ID:
                recordingStartedDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());
                break;

            case RecordingProgressDecoder.TEMPLATE_ID:
                recordingProgressDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());
                break;

            case RecordingStoppedDecoder.TEMPLATE_ID:
                recordingStoppedDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());
                break;

            default:
                throw new IllegalStateException("Unknown templateId: " + templateId);
        }

        pollComplete = true;
    }

    public MessageHeaderDecoder messageHeaderDecoder()
    {
        return messageHeaderDecoder;
    }

    public RecordingStartedDecoder recordingStartedDecoder()
    {
        return recordingStartedDecoder;
    }

    public RecordingProgressDecoder recordingProgressDecoder()
    {
        return recordingProgressDecoder;
    }

    public RecordingStoppedDecoder recordingStoppedDecoder()
    {
        return recordingStoppedDecoder;
    }
}
