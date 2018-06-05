/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.RecoveryPlanDecoder;
import io.aeron.cluster.codecs.RecoveryPlanEncoder;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.*;

/**
 * A log of recordings which make up the history of a Raft log across leadership terms. Entries are in order.
 * <p>
 * The log is made up of entries of leadership terms or snapshots to roll up state as of a log position within a
 * leadership term.
 * <p>
 * The latest state is made up of a the latest snapshot followed by any leadership term logs which follow. It is
 * possible that a snapshot is taken mid term and therefore the latest state is the snapshot plus the log of messages
 * which got appended to the log after the snapshot was taken.
 * <p>
 * Record layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Recording ID                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Log Position at beginning of term                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Log Position when entry was created              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |               Timestamp when entry was created                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |          Applicable ID (Member ID vote / Service ID)          |
 *  +---------------------------------------------------------------+
 *  |                 Entry Type (Log or Snapshot)                  |
 *  +---------------------------------------------------------------+
 *  |                                                               |
 *  |                                                              ...
 * ...                Repeats to the end of the log                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class RecordingLog
{
    /**
     * The ID assigned to the consensus module as its service id for taking snapshots.
     */
    public static final int CONSENSUS_MODULE_ID = 0;

    /**
     * Representation of the entry in the {@link RecordingLog}.
     */
    public static final class Entry
    {
        public final long recordingId;
        public final long leadershipTermId;
        public final long termBaseLogPosition;
        public final long logPosition;
        public final long timestamp;
        public final int applicableId;
        public final int type;
        public final int entryIndex;

        /**
         * A new entry in the recording log.
         *
         * @param recordingId         of the entry in an archive.
         * @param leadershipTermId    of this entry.
         * @param termBaseLogPosition position of the log over leadership terms at the beginning of this term.
         * @param logPosition         position reached when the entry was created
         * @param timestamp           of this entry.
         * @param applicableId        member id for vote or service id for snapshot.
         * @param type                of the entry as a log of a term or a snapshot.
         * @param entryIndex          of the entry on disk.
         */
        public Entry(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long timestamp,
            final int applicableId,
            final int type,
            final int entryIndex)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.timestamp = timestamp;
            this.applicableId = applicableId;
            this.type = type;
            this.entryIndex = entryIndex;
        }

        public String toString()
        {
            return "Entry{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", logPosition=" + logPosition +
                ", timestamp=" + timestamp +
                ", applicableId=" + applicableId +
                ", type=" + type +
                ", entryIndex=" + entryIndex +
                '}';
        }
    }

    /**
     * Representation of a snapshot entry in the {@link RecordingLog}.
     */
    public static final class Snapshot
    {
        public final long recordingId;
        public final long leadershipTermId;
        public final long termBaseLogPosition;
        public final long logPosition;
        public final long timestamp;
        public final int serviceId;

        /**
         * A snapshot entry in the {@link RecordingLog}.
         *
         * @param recordingId         of the entry in an archive.
         * @param leadershipTermId    in which the snapshot was taken.
         * @param termBaseLogPosition position of the log over leadership terms at the beginning of this term.
         * @param logPosition         position reached when the entry was snapshot was taken.
         * @param timestamp           as which the snapshot was taken.
         * @param serviceId           which the snapshot belongs to.
         */
        public Snapshot(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long timestamp,
            final int serviceId)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.timestamp = timestamp;
            this.serviceId = serviceId;
        }

        public Snapshot(final RecoveryPlanDecoder.SnapshotsDecoder decoder)
        {
            this.recordingId = decoder.recordingId();
            this.leadershipTermId = decoder.leadershipTermId();
            this.termBaseLogPosition = decoder.termBaseLogPosition();
            this.logPosition = decoder.logPosition();
            this.timestamp = decoder.timestamp();
            this.serviceId = decoder.serviceId();
        }

        public void encode(final RecoveryPlanEncoder.SnapshotsEncoder encoder)
        {
            encoder
                .recordingId(recordingId)
                .leadershipTermId(leadershipTermId)
                .termBaseLogPosition(termBaseLogPosition)
                .logPosition(logPosition)
                .timestamp(timestamp)
                .serviceId(serviceId);
        }

        public String toString()
        {
            return "Snapshot{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", logPosition=" + logPosition +
                ", timestamp=" + timestamp +
                ", serviceId=" + serviceId +
                '}';
        }
    }

    /**
     * Representation of a log entry in the {@link RecordingLog}.
     */
    public static final class Log
    {
        public final long recordingId;
        public final long leadershipTermId;
        public final long termBaseLogPosition;
        public final long logPosition;
        public final long startPosition;
        public final long stopPosition;
        public final int initialTermId;
        public final int termBufferLength;
        public final int mtuLength;
        public final int sessionId;

        public Log(
            final long recordingId,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long logPosition,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.logPosition = logPosition;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
            this.initialTermId = initialTermId;
            this.termBufferLength = termBufferLength;
            this.mtuLength = mtuLength;
            this.sessionId = sessionId;
        }

        public Log(final RecoveryPlanDecoder.LogsDecoder decoder)
        {
            this.recordingId = decoder.recordingId();
            this.leadershipTermId = decoder.leadershipTermId();
            this.termBaseLogPosition = decoder.termBaseLogPosition();
            this.logPosition = decoder.logPosition();
            this.startPosition = decoder.startPosition();
            this.stopPosition = decoder.stopPosition();
            this.initialTermId = decoder.initialTermId();
            this.termBufferLength = decoder.termBufferLength();
            this.mtuLength = decoder.mtuLength();
            this.sessionId = decoder.sessionId();
        }

        public void encode(final RecoveryPlanEncoder.LogsEncoder encoder)
        {
            encoder
                .recordingId(recordingId)
                .leadershipTermId(leadershipTermId)
                .termBaseLogPosition(termBaseLogPosition)
                .logPosition(logPosition)
                .startPosition(startPosition)
                .stopPosition(stopPosition)
                .initialTermId(initialTermId)
                .termBufferLength(termBufferLength)
                .mtuLength(mtuLength)
                .sessionId(sessionId);
        }

        public String toString()
        {
            return "Log{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", termBaseLogPosition=" + termBaseLogPosition +
                ", logPosition=" + logPosition +
                ", startPosition=" + startPosition +
                ", stopPosition=" + stopPosition +
                ", initialTermId=" + initialTermId +
                ", termBufferLength=" + termBufferLength +
                ", mtuLength=" + mtuLength +
                ", sessionId=" + sessionId +
                '}';
        }
    }

    /**
     * The snapshots and steps to recover the state of a cluster.
     */
    public static class RecoveryPlan
    {
        public final long lastLeadershipTermId;
        public final long lastTermBaseLogPosition;
        public final long lastAppendedLogPosition;
        public final ArrayList<Snapshot> snapshots;
        public final ArrayList<Log> logs;
        public final RecoveryPlanEncoder encoder = new RecoveryPlanEncoder();
        public final RecoveryPlanDecoder decoder = new RecoveryPlanDecoder();

        public RecoveryPlan(
            final long lastLeadershipTermId,
            final long lastTermBaseLogPosition,
            final long lastAppendedLogPosition,
            final ArrayList<Snapshot> snapshots,
            final ArrayList<Log> logs)
        {
            this.lastLeadershipTermId = lastLeadershipTermId;
            this.lastTermBaseLogPosition = lastTermBaseLogPosition;
            this.lastAppendedLogPosition = lastAppendedLogPosition;
            this.snapshots = snapshots;
            this.logs = logs;
        }

        public RecoveryPlan(final DirectBuffer buffer, final int offset)
        {
            decoder.wrap(buffer, offset, RecoveryPlanDecoder.BLOCK_LENGTH, RecoveryPlanDecoder.SCHEMA_VERSION);

            this.lastLeadershipTermId = decoder.lastLeadershipTermId();
            this.lastTermBaseLogPosition = decoder.lastTermBaseLogPosition();
            this.lastAppendedLogPosition = decoder.lastAppendedLogPosition();

            snapshots = new ArrayList<>();
            logs = new ArrayList<>();

            for (final RecoveryPlanDecoder.SnapshotsDecoder snapshotsDecoder : decoder.snapshots())
            {
                snapshots.add(new Snapshot(snapshotsDecoder));
            }

            for (final RecoveryPlanDecoder.LogsDecoder logsDecoder : decoder.logs())
            {
                logs.add(new Log(logsDecoder));
            }
        }

        public int encodedLength()
        {
            return RecoveryPlanEncoder.BLOCK_LENGTH +
                RecoveryPlanEncoder.SnapshotsEncoder.sbeHeaderSize() +
                RecoveryPlanEncoder.LogsEncoder.sbeHeaderSize() +
                (snapshots.size() * RecoveryPlanEncoder.SnapshotsEncoder.sbeBlockLength()) +
                (logs.size() * RecoveryPlanEncoder.LogsEncoder.sbeBlockLength());
        }

        public int encode(final MutableDirectBuffer buffer, final int offset)
        {
            encoder.wrap(buffer, offset)
                .lastLeadershipTermId(lastLeadershipTermId)
                .lastTermBaseLogPosition(lastTermBaseLogPosition)
                .lastAppendedLogPosition(lastAppendedLogPosition);

            final RecoveryPlanEncoder.SnapshotsEncoder snapshotsEncoder = encoder.snapshotsCount(snapshots.size());
            for (int i = 0, size = snapshots.size(); i < size; i++)
            {
                snapshotsEncoder.next();
                snapshots.get(i).encode(snapshotsEncoder);
            }

            final RecoveryPlanEncoder.LogsEncoder logsEncoder = encoder.logsCount(logs.size());
            for (int i = 0, size = logs.size(); i < size; i++)
            {
                logsEncoder.next();
                logs.get(i).encode(logsEncoder);
            }

            return encoder.encodedLength();
        }

        /**
         * Has the log to be replayed as part of the recovery plan?
         *
         * @return true if log replay is require otherwise false.
         */
        public boolean hasReplay()
        {
            boolean hasReplay = false;
            if (logs.size() > 0)
            {
                final RecordingLog.Log log = logs.get(0);
                hasReplay = log.stopPosition > log.startPosition;
            }

            return hasReplay;
        }

        public String toString()
        {
            return "RecoveryPlan{" +
                "lastLeadershipTermId=" + lastLeadershipTermId +
                ", lastTermBaseLogPosition=" + lastTermBaseLogPosition +
                ", lastAppendedLogPosition=" + lastAppendedLogPosition +
                ", snapshots=" + snapshots +
                ", logs=" + logs +
                '}';
        }
    }

    /**
     * Filename for the history of leadership log terms and snapshot recordings.
     */
    public static final String RECORDING_LOG_FILE_NAME = "recording.log";

    /**
     * The log entry is for a recording of messages within a leadership term to the log.
     */
    public static final int ENTRY_TYPE_TERM = 0;

    /**
     * The log entry is for a recording of a snapshot of state taken as of a position in the log.
     */
    public static final int ENTRY_TYPE_SNAPSHOT = 1;

    /**
     * The offset at which the recording id for the entry is stored.
     */
    public static final int RECORDING_ID_OFFSET = 0;

    /**
     * The offset at which the leadership term id for the entry is stored.
     */
    public static final int LEADERSHIP_TERM_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the log position as of the beginning of the term for the entry is stored.
     */
    public static final int TERM_BASE_LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the log position is stored.
     */
    public static final int LOG_POSITION_OFFSET = TERM_BASE_LOG_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the timestamp for the entry is stored.
     */
    public static final int TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the voted for member id or service id is recorded.
     */
    public static final int APPLICABLE_ID_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the type of the entry is stored.
     */
    public static final int ENTRY_TYPE_OFFSET = APPLICABLE_ID_OFFSET + SIZE_OF_INT;

    /**
     * The length of each entry in the recording log (not the recordings in the archive).
     */
    private static final int ENTRY_LENGTH = BitUtil.align(ENTRY_TYPE_OFFSET + SIZE_OF_INT, CACHE_LINE_LENGTH);

    private int nextEntryIndex;
    private final File parentDir;
    private final File logFile;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096).order(LITTLE_ENDIAN);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
    private final ArrayList<Entry> entries = new ArrayList<>();

    /**
     * Create a log that appends to an existing log or creates a new one.
     *
     * @param parentDir in which the log will be created.
     */
    public RecordingLog(final File parentDir)
    {
        this.parentDir = parentDir;
        this.logFile = new File(parentDir, RECORDING_LOG_FILE_NAME);

        reload();
    }

    /**
     * List of currently loaded entries.
     *
     * @return the list of currently loaded entries.
     */
    public List<Entry> entries()
    {
        return entries;
    }

    /**
     * Get the next index to be used when appending an entry to the log.
     *
     * @return the next index to be used when appending an entry to the log.
     */
    public int nextEntryIndex()
    {
        return nextEntryIndex;
    }

    /**
     * Reload the log from disk.
     */
    public void reload()
    {
        entries.clear();
        final boolean newFile = !logFile.exists();

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), CREATE, READ, WRITE))
        {
            if (newFile)
            {
                syncDirectory(parentDir);
                return;
            }

            nextEntryIndex = 0;
            byteBuffer.clear();
            while (true)
            {
                final int bytes = fileChannel.read(byteBuffer);
                if (byteBuffer.remaining() == 0)
                {
                    byteBuffer.flip();
                    captureEntriesFromBuffer(byteBuffer, buffer, entries);
                    byteBuffer.clear();
                }

                if (-1 == bytes)
                {
                    if (byteBuffer.position() > 0)
                    {
                        byteBuffer.flip();
                        captureEntriesFromBuffer(byteBuffer, buffer, entries);
                        byteBuffer.clear();
                    }

                    break;
                }
            }

        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Get the latest snapshot {@link Entry} in the log.
     *
     * @param serviceId for the snapshot.
     * @return the latest snapshot {@link Entry} in the log or null if no snapshot exists.
     */
    public Entry getLatestSnapshot(final int serviceId)
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (ENTRY_TYPE_SNAPSHOT == entry.type && serviceId == entry.applicableId)
            {
                return entry;
            }
        }

        return null;
    }

    /**
     * Create a recovery plan for the cluster that when the steps are replayed will bring the cluster back to the
     * latest stable state.
     *
     * @param archive      to lookup recording descriptors.
     * @param serviceCount of services that may have snapshots.
     * @return a new {@link RecoveryPlan} for the cluster.
     */
    public RecoveryPlan createRecoveryPlan(final AeronArchive archive, final int serviceCount)
    {
        final ArrayList<Snapshot> snapshots = new ArrayList<>();
        final ArrayList<Log> logs = new ArrayList<>();
        planRecovery(snapshots, logs, entries, archive, serviceCount);

        long lastLeadershipTermId = NULL_VALUE;
        long lastTermBaseLogPosition = 0;
        long lastAppendedLogPosition = 0;

        final int snapshotStepsSize = snapshots.size();
        if (snapshotStepsSize > 0)
        {
            final Snapshot snapshot = snapshots.get(0);

            lastLeadershipTermId = snapshot.leadershipTermId;
            lastTermBaseLogPosition = snapshot.termBaseLogPosition;
            lastAppendedLogPosition = snapshot.logPosition;
        }

        if (!logs.isEmpty())
        {
            final Log log = logs.get(0);

            lastLeadershipTermId = log.leadershipTermId;
            lastTermBaseLogPosition = log.termBaseLogPosition;
            lastAppendedLogPosition = log.stopPosition;
        }

        return new RecoveryPlan(
            lastLeadershipTermId,
            lastTermBaseLogPosition,
            lastAppendedLogPosition,
            snapshots,
            logs);
    }

    /**
     * Append a log entry for a leadership term.
     *
     * @param leadershipTermId    for the current term.
     * @param termBaseLogPosition reached at the beginning of the term.
     * @param timestamp           at the beginning of the term.
     * @param votedForMemberId    in the leader election.
     */
    public void appendTerm(
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long timestamp,
        final int votedForMemberId)
    {
        final int size = entries.size();
        if (size > 0)
        {
            final Entry entry = entries.get(size - 1);

            if (entry.type != NULL_VALUE && entry.leadershipTermId > leadershipTermId)
            {
                throw new IllegalStateException("leadershipTermId out of sequence: previous " +
                    entry.leadershipTermId + " this " + leadershipTermId);
            }
        }

        append(
            ENTRY_TYPE_TERM,
            NULL_VALUE,
            leadershipTermId,
            termBaseLogPosition,
            NULL_POSITION,
            timestamp,
            votedForMemberId);
    }

    /**
     * Append a log entry for a snapshot.
     *
     * @param recordingId         in the archive for the snapshot.
     * @param leadershipTermId    for the current term
     * @param termBaseLogPosition at the beginning of the leadership term.
     * @param logPosition         for the position in the current term or length so far for that term.
     * @param timestamp           at which the snapshot was taken.
     * @param serviceId           for which the snapshot is recorded.
     */
    public void appendSnapshot(
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final int serviceId)
    {
        final int size = entries.size();
        if (size > 0)
        {
            final Entry entry = entries.get(size - 1);

            if (entry.type == ENTRY_TYPE_TERM && entry.leadershipTermId != leadershipTermId)
            {
                throw new IllegalStateException("leadershipTermId out of sequence: previous " +
                    entry.leadershipTermId + " this " + leadershipTermId);
            }
        }

        append(
            ENTRY_TYPE_SNAPSHOT,
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            serviceId);
    }

    /**
     * Commit the recording id of the log for a leadership term.
     *
     * @param leadershipTermId for committing the recording id for the log.
     * @param recordingId      for the log of the leadership term.
     */
    public void commitLeadershipRecordingId(final long leadershipTermId, final long recordingId)
    {
        final int index = getLeadershipTermEntryIndex(leadershipTermId);
        commitEntryValue(index, recordingId, RECORDING_ID_OFFSET);

        final Entry entry = entries.get(index);
        entries.set(index, new Entry(
            recordingId,
            entry.leadershipTermId,
            entry.termBaseLogPosition,
            entry.logPosition,
            entry.timestamp,
            entry.applicableId,
            entry.type,
            entry.entryIndex));
    }

    /**
     * Commit the position reached in a leadership term before a clean shutdown.
     *
     * @param leadershipTermId for committing the term position reached.
     * @param logPosition      reached in the leadership term.
     */
    public void commitLogPosition(final long leadershipTermId, final long logPosition)
    {
        final int index = getLeadershipTermEntryIndex(leadershipTermId);
        commitEntryValue(index, logPosition, LOG_POSITION_OFFSET);

        final Entry entry = entries.get(index);
        entries.set(index, new Entry(
            entry.recordingId,
            entry.leadershipTermId,
            entry.termBaseLogPosition,
            logPosition,
            entry.timestamp,
            entry.applicableId,
            entry.type,
            entry.entryIndex));
    }

    /**
     * Commit the position for the base of a leadership term.
     *
     * @param leadershipTermId for committing the base position.
     * @param logPosition      for the base of a leadership term.
     */
    public void commitTermBaseLogPosition(final long leadershipTermId, final long logPosition)
    {
        final int index = getLeadershipTermEntryIndex(leadershipTermId);
        commitEntryValue(index, logPosition, TERM_BASE_LOG_POSITION_OFFSET);

        final Entry entry = entries.get(index);
        entries.set(index, new Entry(
            entry.recordingId,
            entry.leadershipTermId,
            logPosition,
            entry.logPosition,
            entry.timestamp,
            entry.applicableId,
            entry.type,
            entry.entryIndex));
    }

    /**
     * Tombstone an entry in the log so it is no longer valid.
     *
     * @param leadershipTermId to match for validation.
     * @param entryIndex       reached in the leadership term.
     */
    public void tombstoneEntry(final long leadershipTermId, final int entryIndex)
    {
        int index = -1;
        for (int i = 0, size = entries.size(); i < size; i++)
        {
            final Entry entry = entries.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.entryIndex == entryIndex)
            {
                index = entry.entryIndex;
                break;
            }
        }

        if (-1 == index)
        {
            throw new IllegalArgumentException("unknown entry index: " + entryIndex);
        }

        buffer.putInt(0, NULL_VALUE, LITTLE_ENDIAN);
        byteBuffer.limit(SIZE_OF_INT).position(0);
        final long filePosition = (index * ENTRY_LENGTH) + ENTRY_TYPE_OFFSET;

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), WRITE, SYNC))
        {
            if (SIZE_OF_INT != fileChannel.write(byteBuffer, filePosition))
            {
                throw new IllegalStateException("failed to write field atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public String toString()
    {
        return "RecordingLog{" +
            "file=" + logFile.getAbsolutePath() +
            ", entries=" + entries +
            '}';
    }

    private void append(
        final int entryType,
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final int applicableId)
    {
        buffer.putLong(RECORDING_ID_OFFSET, recordingId, LITTLE_ENDIAN);
        buffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId, LITTLE_ENDIAN);
        buffer.putLong(TERM_BASE_LOG_POSITION_OFFSET, termBaseLogPosition, LITTLE_ENDIAN);
        buffer.putLong(LOG_POSITION_OFFSET, logPosition, LITTLE_ENDIAN);
        buffer.putLong(TIMESTAMP_OFFSET, timestamp, LITTLE_ENDIAN);
        buffer.putInt(APPLICABLE_ID_OFFSET, applicableId, LITTLE_ENDIAN);
        buffer.putInt(ENTRY_TYPE_OFFSET, entryType, LITTLE_ENDIAN);

        byteBuffer.limit(ENTRY_LENGTH).position(0);

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), WRITE, APPEND, SYNC))
        {
            if (ENTRY_LENGTH != fileChannel.write(byteBuffer))
            {
                throw new IllegalStateException("failed to write entry atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        entries.add(new Entry(
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            applicableId,
            entryType,
            nextEntryIndex++));
    }

    private void captureEntriesFromBuffer(
        final ByteBuffer byteBuffer, final UnsafeBuffer buffer, final ArrayList<Entry> entries)
    {
        for (int i = 0, length = byteBuffer.limit(); i < length; i += ENTRY_LENGTH)
        {
            final int entryType = buffer.getInt(i + ENTRY_TYPE_OFFSET);

            if (NULL_VALUE != entryType)
            {
                entries.add(new Entry(
                    buffer.getLong(i + RECORDING_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + LEADERSHIP_TERM_ID_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TERM_BASE_LOG_POSITION_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + LOG_POSITION_OFFSET, LITTLE_ENDIAN),
                    buffer.getLong(i + TIMESTAMP_OFFSET, LITTLE_ENDIAN),
                    buffer.getInt(i + APPLICABLE_ID_OFFSET, LITTLE_ENDIAN),
                    entryType,
                    nextEntryIndex));
            }

            ++nextEntryIndex;
        }
    }

    private static void syncDirectory(final File dir)
    {
        try (FileChannel fileChannel = FileChannel.open(dir.toPath()))
        {
            fileChannel.force(true);
        }
        catch (final IOException ignore)
        {
        }
    }

    private static void getRecordingExtent(
        final AeronArchive archive, final RecordingExtent recordingExtent, final Entry entry)
    {
        if (archive.listRecording(entry.recordingId, recordingExtent) == 0)
        {
            throw new IllegalStateException("unknown recording id: " + entry.recordingId);
        }
    }

    private int getLeadershipTermEntryIndex(final long leadershipTermId)
    {
        for (int i = 0, size = entries.size(); i < size; i++)
        {
            final Entry entry = entries.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.type == ENTRY_TYPE_TERM)
            {
                return entry.entryIndex;
            }
        }

        throw new IllegalArgumentException("unknown leadershipTermId: " + leadershipTermId);
    }

    private void commitEntryValue(final int entryIndex, final long value, final int fieldOffset)
    {
        buffer.putLong(0, value, LITTLE_ENDIAN);
        byteBuffer.limit(SIZE_OF_LONG).position(0);
        final long filePosition = (entryIndex * ENTRY_LENGTH) + fieldOffset;

        try (FileChannel fileChannel = FileChannel.open(logFile.toPath(), WRITE, SYNC))
        {
            if (SIZE_OF_LONG != fileChannel.write(byteBuffer, filePosition))
            {
                throw new IllegalStateException("failed to write field atomically");
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static void planRecovery(
        final ArrayList<Snapshot> snapshots,
        final ArrayList<Log> logs,
        final ArrayList<Entry> entries,
        final AeronArchive archive,
        final int serviceCount)
    {
        if (entries.isEmpty())
        {
            return;
        }

        int logIndex = -1;
        int snapshotIndex = -1;
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (-1 == snapshotIndex && ENTRY_TYPE_SNAPSHOT == entry.type)
            {
                snapshotIndex = i;
            }
            else if (-1 == logIndex && ENTRY_TYPE_TERM == entry.type)
            {
                logIndex = i;
            }
            else if (-1 != snapshotIndex && -1 != logIndex)
            {
                break;
            }
        }

        final RecordingExtent recordingExtent = new RecordingExtent();

        if (-1 != snapshotIndex)
        {
            final Entry snapshot = entries.get(snapshotIndex);
            snapshots.add(new Snapshot(
                snapshot.recordingId,
                snapshot.leadershipTermId,
                snapshot.termBaseLogPosition,
                snapshot.logPosition,
                snapshot.timestamp,
                snapshot.applicableId));

            for (int i = 1; i <= serviceCount; i++)
            {
                if ((snapshotIndex - i) < 0)
                {
                    throw new IllegalStateException("Snapshot missing for service at index " + i + " in " + entries);
                }

                final Entry entry = entries.get(snapshotIndex - 1);

                if (ENTRY_TYPE_SNAPSHOT == entry.type &&
                    entry.leadershipTermId == snapshot.leadershipTermId &&
                    entry.logPosition == snapshot.logPosition)
                {
                    snapshots.add(entry.applicableId + 1, new Snapshot(
                        entry.recordingId,
                        entry.leadershipTermId,
                        entry.termBaseLogPosition,
                        entry.logPosition,
                        entry.timestamp,
                        entry.applicableId));
                }
            }
        }

        if (-1 != logIndex)
        {
            final Entry entry = entries.get(logIndex);
            getRecordingExtent(archive, recordingExtent, entry);

            final long startPosition = -1 == snapshotIndex ?
                recordingExtent.startPosition : snapshots.get(0).logPosition;

            logs.add(new Log(
                entry.recordingId,
                entry.leadershipTermId,
                entry.termBaseLogPosition,
                entry.logPosition,
                startPosition,
                recordingExtent.stopPosition,
                recordingExtent.initialTermId,
                recordingExtent.termBufferLength,
                recordingExtent.mtuLength,
                recordingExtent.sessionId));
        }
    }
}
