/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.Strings;

import java.util.Scanner;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;
import static org.agrona.SystemUtil.getProperty;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * {@code RecordingReplicator} allows replicating a recording from the source Archive to the destination Archive either
 * as a new recording or by replacing an existing recording.
 * <p>
 * <em>Note: If {@link #DESTINATION_RECORDING_ID_PROP_NAME} is set then the existing destination recording will
 * be completely replaced, i.e. truncated and overwritten with the data from the source recording.</em>
 * <p>
 * Configuration properties:
 * <ul>
 *     <li>{@link #SOURCE_RECORDING_ID_PROP_NAME} - required, specifies id of source recording to be replicated.</li>
 *     <li>{@link #DESTINATION_RECORDING_ID_PROP_NAME} - optional, specifies id of destination recording to be
 *     replaced. If omitted or set to {@link Aeron#NULL_VALUE} then the new recording will be created in the destination
 *     Archive.</li>
 *     <li>{@link #SOURCE_ARCHIVE_CONTROL_REQUEST_CHANNEL} - required, specifies the control request channel of the
 *     source Archive. Must be reachable from the destination Archive.</li>
 *     <li>{@link #SOURCE_ARCHIVE_CONTROL_REQUEST_STREAM_ID} - optional, specifies the control request stream id for
 *     connecting to the source Archive from the destination Archive. Defaults to
 *     {@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_DEFAULT}</li>
 *     <li>{@link #REPLICATION_CHANNEL} - optional, specifies the replication channel to be used for the replication of
 *     the source recording. When non-empty overrides the {@link io.aeron.archive.Archive.Context#replicationChannel()}
 *     of the destination Archive, otherwise the latter is used.</li>
 *     <li>{@link io.aeron.CommonContext#AERON_DIR_PROP_NAME} - optional, specifies the aeron directory for connecting
 *     to the MediaDriver. If not specified the the default Aeron directory is assumed
 *     ({@link io.aeron.CommonContext#AERON_DIR_PROP_DEFAULT}).</li>
 *     <li>{@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME} - required, specifies
 *     control request channel for connecting to the destination Archive.</li>
 *     <li>{@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME} - optional, specifies
 *     control request stream id for connecting to the destination Archive. Defaults to
 *     {@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_DEFAULT}.
 *     </li>
 *     <li>{@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_RESPONSE_CHANNEL_PROP_NAME} - required,
 *     specifies control response channel for receiving responses from the destination Archive.</li>
 *     <li>{@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_RESPONSE_STREAM_ID_PROP_NAME} - optional,
 *     specifies control response stream id for receiving responses from the destination Archive. Defaults to
 *     {@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_RESPONSE_STREAM_ID_DEFAULT}.</li>
 * </ul>
 * <p>
 * The easiest way is to pass the configuration via file, e.g.:
 * <pre>
 * {@code java -cp ... io.aeron.samples.Archive.RecordingReplicator <prop file name>}
 * </pre>
 * <p>
 * An alternative is to pass properties using the {@code -D} JVM flag, e.g.:
 * <pre>
 * {@code java -cp ... -Daeron.dir=some-dir ... io.aeron.samples.Archive.RecordingReplicator}
 * </pre>
 */
public final class RecordingReplicator
{
    /**
     * Name of the required system property for specifying the id of the source recording that must be replicated to
     * the destination Archive.
     */
    public static final String SOURCE_RECORDING_ID_PROP_NAME = "aeron.sample.archive.replicate.source.recording.id";

    /**
     * Name of the optional system property for specifying the destination recording id. Defaults to
     * {@link io.aeron.Aeron#NULL_VALUE} in which case the source recording will be replicated as a new recording in
     * the destination Archive.
     */
    public static final String DESTINATION_RECORDING_ID_PROP_NAME =
        "aeron.sample.archive.replicate.destination.recording.id";

    /**
     * Name of the required system property for specifying the control request channel to connect to the source Archive
     * from the destination Archive.
     */
    public static final String SOURCE_ARCHIVE_CONTROL_REQUEST_CHANNEL =
        "aeron.sample.archive.replicate.source.control.request.channel";

    /**
     * Name of the optional system property for specifying the control request stream id to connect to the source
     * Archive from the destination Archive. Defaults to
     * {@link io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_DEFAULT}.
     */
    public static final String SOURCE_ARCHIVE_CONTROL_REQUEST_STREAM_ID =
        "aeron.sample.archive.replicate.source.control.request.stream.id";

    /**
     * Name of the optional system property for specifying an explicit replication channel for recording replication
     * between source and destination archives. If not specified then the
     * {@link io.aeron.archive.Archive.Context#replicationChannel()} of the destination Archive will be used.
     */
    public static final String REPLICATION_CHANNEL = "aeron.sample.archive.replicate.replication.channel";

    private final AeronArchive aeronArchive;
    private final RecordingSignalCapture signalCapture;
    private final long srcRecordingId;
    private final String srcArchiveRequestChannel;
    private final int srcArchiveRequestStreamId;
    private final long dstRecordingId;
    private final String replicationChannel;

    /**
     * @param aeronArchive              client for the destination Archive.
     * @param srcRecordingId            id of the recording from the source Archive that must be replicated.
     * @param dstRecordingId            id of existing recording in the destination Archive that must be
     *                                  replaced. If set as {@link Aeron#NULL_VALUE} in which case a new recording
     *                                  will be created.
     * @param srcArchiveRequestChannel  request channel for sending commands to the source Archive.
     * @param srcArchiveRequestStreamId request stream id for sending commands to the source Archive.
     * @param replicationChannel        override the channel via which the recording data is replicated between
     *                                  the source and destination Archives. If {@code null} or empty then the
     *                                  {@link io.aeron.archive.Archive.Context#replicationChannel()} of the
     *                                  destination Archive will be used.
     * @throws NullPointerException     if any of the required properties are not set.
     * @throws IllegalArgumentException if {@link io.aeron.archive.client.AeronArchive.Context#recordingSignalConsumer()}
     *                                  is not set.
     * @throws ClassCastException       if {@link io.aeron.archive.client.AeronArchive.Context#recordingSignalConsumer()}
     *                                  is not an instance of the {@link RecordingSignalCapture} class.
     */
    public RecordingReplicator(
        final AeronArchive aeronArchive,
        final long srcRecordingId,
        final long dstRecordingId,
        final String srcArchiveRequestChannel,
        final int srcArchiveRequestStreamId,
        final String replicationChannel)
    {
        this.aeronArchive = aeronArchive;
        signalCapture = (RecordingSignalCapture)aeronArchive.context().recordingSignalConsumer();

        if (null == signalCapture)
        {
            throw new IllegalArgumentException("RecordingSignalConsumer not configured!");
        }

        if (NULL_VALUE == srcRecordingId)
        {
            throw new IllegalArgumentException(SOURCE_RECORDING_ID_PROP_NAME + " must be specified");
        }

        this.srcRecordingId = srcRecordingId;
        this.srcArchiveRequestChannel =
            requireNonNull(trimToNull(srcArchiveRequestChannel), SOURCE_ARCHIVE_CONTROL_REQUEST_CHANNEL);
        this.srcArchiveRequestStreamId = srcArchiveRequestStreamId;
        this.dstRecordingId = dstRecordingId;
        this.replicationChannel = trimToNull(replicationChannel);
    }

    /**
     * Replicate source recording to the destination Archive.
     *
     * @return id of the destination recording (created or replaced).
     */
    public long replicate()
    {
        if (NULL_VALUE != dstRecordingId)
        {
            final RecordingDescriptorCollector recordingDescriptorCollector = new RecordingDescriptorCollector(1);
            if (1 != aeronArchive.listRecording(dstRecordingId, recordingDescriptorCollector.reset()))
            {
                throw new IllegalArgumentException("unknown destination recording id: " + dstRecordingId);
            }
            final RecordingDescriptor recordingDescriptor = recordingDescriptorCollector.descriptors().get(0).retain();

            signalCapture.reset();
            aeronArchive.truncateRecording(dstRecordingId, recordingDescriptor.startPosition());
            signalCapture.awaitSignalForRecordingId(aeronArchive, dstRecordingId, RecordingSignal.DELETE);
        }

        final ReplicationParams replicationParams = new ReplicationParams()
            .replicationChannel(replicationChannel)
            .dstRecordingId(dstRecordingId);
        final long replicationId = aeronArchive.replicate(
            srcRecordingId, srcArchiveRequestStreamId, srcArchiveRequestChannel, replicationParams);

        signalCapture.reset();
        signalCapture.awaitSignalForCorrelationId(aeronArchive, replicationId, RecordingSignal.SYNC);
        final long recordingId = signalCapture.recordingId();

        signalCapture.reset();
        signalCapture.awaitSignalForCorrelationId(aeronArchive, replicationId, RecordingSignal.REPLICATE_END);

        return recordingId;
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        final long srcRecordingId = parseLong(getProperty(SOURCE_RECORDING_ID_PROP_NAME));
        final long dstRecordingId = parseLong(getProperty(DESTINATION_RECORDING_ID_PROP_NAME));
        final String srcArchiveRequestChannel = getProperty(SOURCE_ARCHIVE_CONTROL_REQUEST_CHANNEL);
        final int srcArchiveRequestStreamId = parseInt(
            getProperty(SOURCE_ARCHIVE_CONTROL_REQUEST_STREAM_ID, String.valueOf(CONTROL_STREAM_ID_DEFAULT)));
        final String replicationChannel = getProperty(REPLICATION_CHANNEL);

        if (NULL_VALUE != dstRecordingId)
        {
            System.out.println("Destination recording=" + dstRecordingId + " will be replaced with source recording=" +
                srcRecordingId + ". Continue? (y/n)");
            final String answer = new Scanner(System.in).nextLine();
            if (!"y".equalsIgnoreCase(answer) && !"yes".equalsIgnoreCase(answer))
            {
                System.out.println("Action aborted!");
                System.exit(-1);
                return;
            }
        }

        final RecordingSignalCapture signalCapture = new RecordingSignalCapture();
        try (AeronArchive aeronArchive = AeronArchive.connect(
            new AeronArchive.Context().recordingSignalConsumer(signalCapture)))
        {
            final RecordingReplicator replicator = new RecordingReplicator(
                aeronArchive,
                srcRecordingId,
                dstRecordingId,
                srcArchiveRequestChannel,
                srcArchiveRequestStreamId,
                replicationChannel);

            final long newRecordingId = replicator.replicate();
            System.out.println("Source recordingId=" + srcRecordingId + " replicated to the destination recordingId=" +
                newRecordingId + ".");
        }
    }

    private static String trimToNull(final String value)
    {
        if (Strings.isEmpty(value))
        {
            return null;
        }

        final String result = value.trim();

        return result.isEmpty() ? null : result;
    }
}
