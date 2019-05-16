package io.aeron.archive;

import java.io.File;


import io.aeron.Aeron;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;

public class CatalogView
{

    /**
     * List all recording descriptors.
     * <p>
     *
     * @param archiveDir the directory containing the {@link Catalog}.
     * @param consumer    to which the descriptor are dispatched.
     */
    public static void listRecordings(final File archiveDir, final RecordingDescriptorConsumer consumer)
    {
        final RecordingDescriptorConsumerAdaptor adaptor = new RecordingDescriptorConsumerAdaptor(consumer);

        try (Catalog catalog = new Catalog(archiveDir, System::currentTimeMillis))
        {
            catalog.forEach(adaptor);
        }
    }

    /**
     * List a recording descriptor for a single recording id.
     * <p>
     * If the recording id is not found then nothing is returned.
     *
     * @param archiveDir the directory containing the {@link Catalog}.
     * @param recordingId to view
     * @param consumer    to which the descriptor are dispatched.
     * @return the true of a descriptor is found and consumed.
     */
    public static boolean listRecording(final File archiveDir,
        final long recordingId,
        final RecordingDescriptorConsumer consumer)
    {
        final RecordingDescriptorConsumerAdaptor adaptor = new RecordingDescriptorConsumerAdaptor(consumer);

        try (Catalog catalog = new Catalog(archiveDir, System::currentTimeMillis))
        {
            return catalog.forEntry(adaptor, recordingId);
        }
    }

    private static class RecordingDescriptorConsumerAdaptor implements Catalog.CatalogEntryProcessor
    {
        private static final long NULL_SESSION_ID = Aeron.NULL_VALUE;
        private static final long NULL_CORRELATION_ID = Aeron.NULL_VALUE;

        private final RecordingDescriptorConsumer consumer;

        RecordingDescriptorConsumerAdaptor(final RecordingDescriptorConsumer consumer)
        {
            this.consumer = consumer;
        }

        @Override
        public void accept(final RecordingDescriptorHeaderEncoder headerEncoder,
            final RecordingDescriptorHeaderDecoder headerDecoder,
            final RecordingDescriptorEncoder descriptorEncoder,
            final RecordingDescriptorDecoder descriptorDecoder)
        {
            consumer.onRecordingDescriptor(NULL_SESSION_ID,
                NULL_CORRELATION_ID,
                descriptorDecoder.recordingId(),
                descriptorDecoder.startTimestamp(),
                descriptorDecoder.stopTimestamp(),
                descriptorDecoder.startPosition(),
                descriptorDecoder.stopPosition(),
                descriptorDecoder.initialTermId(),
                descriptorDecoder.segmentFileLength(),
                descriptorDecoder.termBufferLength(),
                descriptorDecoder.mtuLength(),
                descriptorDecoder.sessionId(),
                descriptorDecoder.streamId(),
                descriptorDecoder.strippedChannel(),
                descriptorDecoder.originalChannel(),
                descriptorDecoder.sourceIdentity());
        }
    }

}
