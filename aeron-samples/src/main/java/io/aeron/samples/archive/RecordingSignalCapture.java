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

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.exceptions.AeronException;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * A {@link RecordingSignalConsumer} implementation that captures recording signal data.
 *
 * <p>Usage example:
 * <pre>
 * {@code
 *     final RecordingSignalCapture recordingSignalCapture = new RecordingSignalCapture();
 *     try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
 *          AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
 *              .recordingSignalConsumer(recordingSignalCapture)
 *              .aeron(aeron)))
 *     {
 *         ...
 *         recordingSignalCapture.reset();
 *         archive.replicate(srcRecordingId, dstRecordingId, ...);
 *
 *         recordingSignalCapture.awaitSignalForRecordingId(archive, dstRecordingId, RecordingSignal.STOP);
 *         final long stopPosition = recordingSignalCapture.position();
 *
 *         recordingSignalCapture.reset();
 *         recordingSignalCapture.awaitSignalForRecordingId(archive, dstRecordingId, RecordingSignal.REPLICATE_END);
 *         ...
 *     }
 * }
 * </pre>
 */
public final class RecordingSignalCapture implements RecordingSignalConsumer
{
    private long controlSessionId;
    private long correlationId;
    private long subscriptionId;
    private long recordingId;
    private long position;
    private RecordingSignal signal;

    /**
     * {@inheritDoc}
     */
    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.subscriptionId = subscriptionId;
        if (NULL_VALUE != recordingId)
        {
            this.recordingId = recordingId;
        }
        if (NULL_POSITION != position)
        {
            this.position = position;
        }
        this.signal = signal;
    }

    /**
     * Uses {@link AeronArchive#pollForRecordingSignals()} until the specified signal is received.
     *
     * @param archive               client to poll for signals on.
     * @param expectedCorrelationId to match the signal.
     * @param expectedSignal        to await.
     */
    public void awaitSignalForCorrelationId(
        final AeronArchive archive, final long expectedCorrelationId, final RecordingSignal expectedSignal)
    {
        while (expectedCorrelationId != correlationId || expectedSignal != signal)
        {
            if (0 == archive.pollForRecordingSignals())
            {
                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    throw new AeronException("unexpected interrupt");
                }
            }
            else
            {
                if (expectedCorrelationId == correlationId)
                {
                    if (expectedSignal == signal)
                    {
                        return;
                    }
                    else if (RecordingSignal.REPLICATE_END == signal)
                    {
                        throw new AeronException("unexpected end of replication: correlationId=" + correlationId);
                    }
                }
            }
        }
    }

    /**
     * Uses {@link AeronArchive#pollForRecordingSignals()} until the specified signal for the specified recording is
     * received.
     *
     * @param archive             client to poll for signals on.
     * @param expectedRecordingId that should be delivered with the signal.
     * @param expectedSignal      to await.
     */
    public void awaitSignalForRecordingId(
        final AeronArchive archive, final long expectedRecordingId, final RecordingSignal expectedSignal)
    {
        while (expectedRecordingId != recordingId || expectedSignal != signal)
        {
            if (0 == archive.pollForRecordingSignals())
            {
                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    throw new AeronException("unexpected interrupt");
                }
            }
            else
            {
                if (expectedRecordingId == recordingId)
                {
                    if (expectedSignal == signal)
                    {
                        return;
                    }
                    else if (RecordingSignal.REPLICATE_END == signal)
                    {
                        throw new AeronException("unexpected end of replication: correlationId=" + correlationId);
                    }
                }
            }
        }
    }

    /**
     * Reset internal state before awaiting next signal.
     */
    public void reset()
    {
        correlationId = NULL_VALUE;
        controlSessionId = NULL_VALUE;
        subscriptionId = NULL_VALUE;
        recordingId = NULL_VALUE;
        position = NULL_POSITION;
        signal = null;
    }

    /**
     * Control session id captured when the {@link #onSignal(long, long, long, long, long, RecordingSignal)} was last
     * invoked.
     *
     * @return last captured control session id.
     * @see #onSignal(long, long, long, long, long, RecordingSignal)
     */
    public long controlSessionId()
    {
        return controlSessionId;
    }

    /**
     * Correlation id captured when the {@link #onSignal(long, long, long, long, long, RecordingSignal)} was last
     * invoked.
     *
     * @return last captured correlation id.
     * @see #onSignal(long, long, long, long, long, RecordingSignal)
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Subscription id captured when the {@link #onSignal(long, long, long, long, long, RecordingSignal)} was last
     * invoked.
     *
     * @return last captured subscription id.
     * @see #onSignal(long, long, long, long, long, RecordingSignal)
     */
    public long subscriptionId()
    {
        return subscriptionId;
    }

    /**
     * Recording id captured when the {@link #onSignal(long, long, long, long, long, RecordingSignal)} was last invoked.
     *
     * @return last captured recording id.
     * @see #onSignal(long, long, long, long, long, RecordingSignal)
     */
    public long recordingId()
    {
        return recordingId;
    }

    /**
     * Recording position captured when the {@link #onSignal(long, long, long, long, long, RecordingSignal)} was last
     * invoked.
     *
     * @return last captured recording position.
     * @see #onSignal(long, long, long, long, long, RecordingSignal)
     */
    public long position()
    {
        return position;
    }

    /**
     * Recording signal captured when the {@link #onSignal(long, long, long, long, long, RecordingSignal)} was last
     * invoked.
     *
     * @return last captured recording signal.
     * @see #onSignal(long, long, long, long, long, RecordingSignal)
     */
    public RecordingSignal signal()
    {
        return signal;
    }
}
