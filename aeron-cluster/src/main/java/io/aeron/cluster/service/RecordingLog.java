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
package io.aeron.cluster.service;

import io.aeron.archive.client.AeronArchive;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.*;

/**
 * A log of recordings that make up the history of a Raft log. Entries are in order.
 * <p>
 * The log is made up of entries of log terms or snapshots to roll up state as of a log position and leadership term.
 * <p>
 * The latest state is made up of a the latest snapshot followed by any term logs which follow. It is possible that
 * the a snapshot is taken mid term and therefore the latest state is the snapshot plus the log of messages which
 * begin before the snapshot but continues after it.
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
 *  |             Log Position at beginning of term                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Term Position/Length                       |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |   Timestamp at beginning of term or when snapshot was taken   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Member ID vote                          |
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
     * A copy of the entry in the log.
     */
    public static final class Entry
    {
        public final long recordingId;
        public final long leadershipTermId;
        public final long logPosition;
        public final long termPosition;
        public final long timestamp;
        public final int memberIdVote;
        public final int type;
        public final int entryIndex;

        /**
         * A new entry in the recording log.
         *
         * @param recordingId      of the entry in an archive.
         * @param leadershipTermId of this entry.
         * @param logPosition      accumulated position of the log over leadership terms for the beginning of the term.
         * @param termPosition     position reached within the current leadership term, same at leadership term length.
         * @param timestamp        of this entry.
         * @param memberIdVote     which member this node voted for in the election.
         * @param type             of the entry as a log of a term or a snapshot.
         * @param entryIndex       of the entry on disk.
         */
        public Entry(
            final long recordingId,
            final long leadershipTermId,
            final long logPosition,
            final long termPosition,
            final long timestamp,
            final int memberIdVote,
            final int type,
            final int entryIndex)
        {
            this.recordingId = recordingId;
            this.leadershipTermId = leadershipTermId;
            this.logPosition = logPosition;
            this.termPosition = termPosition;
            this.timestamp = timestamp;
            this.memberIdVote = memberIdVote;
            this.type = type;
            this.entryIndex = entryIndex;
        }

        public String toString()
        {
            return "Entry{" +
                "recordingId=" + recordingId +
                ", leadershipTermId=" + leadershipTermId +
                ", logPosition=" + logPosition +
                ", termPosition=" + termPosition +
                ", timestamp=" + timestamp +
                ", memberIdVote=" + memberIdVote +
                ", type=" + type +
                ", entryIndex=" + entryIndex +
                '}';
        }
    }

    /**
     * Steps in a recovery plan.
     */
    public static class ReplayStep
    {
        public final long recordingStartPosition;
        public final long recordingStopPosition;
        public final Entry entry;

        public ReplayStep(final long recordingStartPosition, final long recordingStopPosition, final Entry entry)
        {
            this.recordingStartPosition = recordingStartPosition;
            this.recordingStopPosition = recordingStopPosition;
            this.entry = entry;
        }

        public String toString()
        {
            return "ReplayStep{" +
                "recordingStartPosition=" + recordingStartPosition +
                ", recordingStopPosition=" + recordingStopPosition +
                ", entry=" + entry +
                '}';
        }
    }

    /**
     * The snapshot and steps to recover the state of a cluster.
     */
    public static class RecoveryPlan
    {
        public final long lastLeadershipTermId;
        public final long lastLogPosition;
        public final long lastTermPositionCommitted;
        public final long lastTermPositionAppended;
        public final ReplayStep snapshotStep;
        public final ArrayList<ReplayStep> termSteps;

        public RecoveryPlan(
            final long lastLeadershipTermId,
            final long lastLogPosition,
            final long lastTermPositionCommitted,
            final long lastTermPositionAppended,
            final ReplayStep snapshotStep,
            final ArrayList<ReplayStep> termSteps)
        {
            this.lastLeadershipTermId = lastLeadershipTermId;
            this.lastLogPosition = lastLogPosition;
            this.lastTermPositionCommitted = lastTermPositionCommitted;
            this.lastTermPositionAppended = lastTermPositionAppended;
            this.snapshotStep = snapshotStep;
            this.termSteps = termSteps;
        }

        public String toString()
        {
            return "RecoveryPlan{" +
                "lastLeadershipTermId=" + lastLeadershipTermId +
                ", lastLogPosition=" + lastLogPosition +
                ", lastTermPositionCommitted=" + lastTermPositionCommitted +
                ", lastTermPositionAppended=" + lastTermPositionAppended +
                ", snapshotStep=" + snapshotStep +
                ", termSteps=" + termSteps +
                '}';
        }
    }

    /**
     * Filename for the recording index for the history of log terms and snapshots.
     */
    public static final String RECORDING_INDEX_FILE_NAME = "recording-index.log";

    /**
     * Represents a value that is not set or invalid.
     */
    public static final int NULL_VALUE = -1;

    /**
     * The index entry is for a recording of messages within a term to the consensus log.
     */
    public static final int ENTRY_TYPE_TERM = 0;

    /**
     * The index entry is for a recording of a snapshot of state taken as of a position in the log.
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
     * The offset at which the absolute log position for the entry is stored.
     */
    public static final int LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the term position is stored.
     */
    public static final int TERM_POSITION_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the timestamp for the entry is stored.
     */
    public static final int TIMESTAMP_OFFSET = TERM_POSITION_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the voted for member id is recorded.
     */
    public static final int MEMBER_ID_VOTE_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;

    /**
     * The offset at which the type of the entry is stored.
     */
    public static final int ENTRY_TYPE_OFFSET = MEMBER_ID_VOTE_OFFSET + SIZE_OF_INT;

    /**
     * The length of each entry.
     */
    private static final int ENTRY_LENGTH = BitUtil.align(ENTRY_TYPE_OFFSET + SIZE_OF_INT, CACHE_LINE_LENGTH);

    private int nextEntryIndex;
    private final File parentDir;
    private final File indexFile;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096).order(ByteOrder.LITTLE_ENDIAN);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
    private final ArrayList<Entry> entries = new ArrayList<>();

    /**
     * Create an index that appends to an existing index or creates a new one.
     *
     * @param parentDir in which the index will be created.
     */
    public RecordingLog(final File parentDir)
    {
        this.parentDir = parentDir;
        this.indexFile = new File(parentDir, RECORDING_INDEX_FILE_NAME);

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
     * Reload the index from disk.
     */
    public void reload()
    {
        entries.clear();

        FileChannel fileChannel = null;
        try
        {
            final boolean newFile = !indexFile.exists();
            fileChannel = FileChannel.open(indexFile.toPath(), CREATE, READ, WRITE, SYNC);

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
        finally
        {
            CloseHelper.close(fileChannel);
        }
    }

    /**
     * Get the latest snapshot {@link Entry} in the index.
     *
     * @return the latest snapshot {@link Entry} in the index or null if no snapshot exists.
     */
    public Entry getLatestSnapshot()
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (ENTRY_TYPE_SNAPSHOT == entry.type)
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
     * @param archive to lookup recording descriptors.
     * @return a new {@link RecoveryPlan} for the cluster.
     */
    public RecoveryPlan createRecoveryPlan(final AeronArchive archive)
    {
        final ArrayList<ReplayStep> steps = new ArrayList<>();
        final ReplayStep snapshotStep = planRecovery(steps, entries, archive);
        long lastLeadershipTermId = -1;
        long lastLogPosition = 0;
        long lastTermPositionCommitted = -1;
        long lastTermPositionAppended = 0;

        if (null != snapshotStep)
        {
            lastLeadershipTermId = snapshotStep.entry.leadershipTermId;
            lastLogPosition = snapshotStep.entry.logPosition;
            lastTermPositionCommitted = snapshotStep.entry.termPosition;
            lastTermPositionAppended = lastTermPositionCommitted;
        }

        final int size = steps.size();
        if (size > 0)
        {
            final ReplayStep replayStep = steps.get(size - 1);
            final Entry entry = replayStep.entry;

            lastLeadershipTermId = entry.leadershipTermId;
            lastLogPosition = entry.logPosition;
            lastTermPositionCommitted = entry.termPosition;
            lastTermPositionAppended = replayStep.recordingStopPosition;
        }

        return new RecoveryPlan(
            lastLeadershipTermId,
            lastLogPosition,
            lastTermPositionCommitted,
            lastTermPositionAppended,
            snapshotStep,
            steps);
    }

    static ReplayStep planRecovery(
        final ArrayList<ReplayStep> steps, final ArrayList<Entry> entries, final AeronArchive archive)
    {
        if (entries.isEmpty())
        {
            return null;
        }

        int snapshotIndex = -1;
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (ENTRY_TYPE_SNAPSHOT == entry.type)
            {
                snapshotIndex = i;
            }
        }

        final ReplayStep snapshotStep;
        final RecordingExtent recordingExtent = new RecordingExtent();

        if (-1 != snapshotIndex)
        {
            final Entry snapshot = entries.get(snapshotIndex);
            getRecordingExtent(archive, recordingExtent, snapshot);

            snapshotStep = new ReplayStep(recordingExtent.startPosition, recordingExtent.stopPosition, snapshot);

            if (snapshotIndex - 1 >= 0)
            {
                for (int i = snapshotIndex - 1; i >= 0; i--)
                {
                    final Entry entry = entries.get(i);
                    if (ENTRY_TYPE_TERM == entry.type)
                    {
                        getRecordingExtent(archive, recordingExtent, snapshot);
                        final long snapshotPosition = snapshot.logPosition + snapshot.termPosition;

                        if (recordingExtent.stopPosition == NULL_POSITION ||
                            (entry.logPosition + recordingExtent.stopPosition) > snapshotPosition)
                        {
                            steps.add(new ReplayStep(snapshot.termPosition, recordingExtent.stopPosition, entry));
                        }
                        break;
                    }
                }
            }
        }
        else
        {
            snapshotStep = null;
        }

        for (int i = snapshotIndex + 1, length = entries.size(); i < length; i++)
        {
            final Entry entry = entries.get(i);
            getRecordingExtent(archive, recordingExtent, entry);

            steps.add(new ReplayStep(recordingExtent.startPosition, recordingExtent.stopPosition, entry));
        }

        return snapshotStep;
    }

    /**
     * Get the latest snapshot for a given position within a leadership term.
     *
     * @param leadershipTermId in which the snapshot was taken.
     * @param termPosition     within the leadership term.
     * @return the latest snapshot for a given position or null if no match found.
     */
    public Entry getSnapshot(final long leadershipTermId, final long termPosition)
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            final Entry entry = entries.get(i);
            if (entry.type == ENTRY_TYPE_SNAPSHOT &&
                leadershipTermId == entry.leadershipTermId &&
                termPosition == entry.termPosition)
            {
                return entry;
            }
        }

        return null;
    }

    /**
     * Append an index entry for a Raft term.
     *
     * @param recordingId      in the archive for the term.
     * @param leadershipTermId for the current term.
     * @param logPosition      reached at the beginning of the term.
     * @param timestamp        at the beginning of the term.
     * @param memberIdVote     in the leader election.
     */
    public void appendTerm(
        final long recordingId,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int memberIdVote)
    {
        final int size = entries.size();
        if (size > 0)
        {
            final long expectedTermId = leadershipTermId - 1;
            final Entry entry = entries.get(size - 1);

            if (entry.type != NULL_VALUE && entry.leadershipTermId != expectedTermId)
            {
                throw new IllegalStateException("leadershipTermId out of sequence: previous " +
                    entry.leadershipTermId + " this " + leadershipTermId);
            }
        }

        append(ENTRY_TYPE_TERM, recordingId, leadershipTermId, logPosition, NULL_POSITION, timestamp, memberIdVote);
    }

    /**
     * Append an index entry for a snapshot.
     *
     * @param recordingId      in the archive for the snapshot.
     * @param leadershipTermId for the current term
     * @param logPosition      at the beginning of the leadership term.
     * @param termPosition     for the position in the current term or length so far for that term.
     * @param timestamp        at which the snapshot was taken.
     */
    public void appendSnapshot(
        final long recordingId,
        final long leadershipTermId,
        final long logPosition,
        final long termPosition,
        final long timestamp)
    {
        final int size = entries.size();
        if (size > 0)
        {
            final Entry entry = entries.get(size - 1);

            if (entry.leadershipTermId != leadershipTermId)
            {
                throw new IllegalStateException("leadershipTermId out of sequence: previous " +
                    entry.leadershipTermId + " this " + leadershipTermId);
            }
        }

        append(ENTRY_TYPE_SNAPSHOT, recordingId, leadershipTermId, logPosition, termPosition, timestamp, NULL_VALUE);
    }

    /**
     * Commit the position reached in a leadership term before a clean shutdown.
     *
     * @param leadershipTermId for committing the term position reached.
     * @param termPosition     reached in the leadership term.
     */
    public void commitLeadershipTermPosition(final long leadershipTermId, final long termPosition)
    {
        int index = -1;
        for (int i = 0, size = entries.size(); i < size; i++)
        {
            final Entry entry = entries.get(i);
            if (entry.leadershipTermId == leadershipTermId && entry.type == ENTRY_TYPE_TERM)
            {
                index = entry.entryIndex;
                break;
            }
        }

        if (-1 == index)
        {
            throw new IllegalArgumentException("Unknown leadershipTermId: " + leadershipTermId);
        }

        buffer.putLong(0, termPosition);
        byteBuffer.limit(SIZE_OF_LONG).position(0);
        final long filePosition = (index * ENTRY_LENGTH) + TERM_POSITION_OFFSET;

        try (FileChannel fileChannel = FileChannel.open(indexFile.toPath(), WRITE, SYNC))
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
            throw new IllegalArgumentException("Unknown entry index: " + entryIndex);
        }

        buffer.putInt(0, NULL_VALUE);
        byteBuffer.limit(SIZE_OF_INT).position(0);
        final long filePosition = (index * ENTRY_LENGTH) + ENTRY_TYPE_OFFSET;

        try (FileChannel fileChannel = FileChannel.open(indexFile.toPath(), WRITE, SYNC))
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

    private void append(
        final int entryType,
        final long recordingId,
        final long leadershipTermId,
        final long logPosition,
        final long termPosition,
        final long timestamp,
        final int memberIdVote)
    {
        buffer.putLong(RECORDING_ID_OFFSET, recordingId);
        buffer.putLong(LOG_POSITION_OFFSET, logPosition);
        buffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
        buffer.putLong(TIMESTAMP_OFFSET, timestamp);
        buffer.putLong(TERM_POSITION_OFFSET, termPosition);
        buffer.putInt(MEMBER_ID_VOTE_OFFSET, memberIdVote);
        buffer.putInt(ENTRY_TYPE_OFFSET, entryType);

        byteBuffer.limit(ENTRY_LENGTH).position(0);

        try (FileChannel fileChannel = FileChannel.open(indexFile.toPath(), WRITE, APPEND, SYNC))
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
            logPosition,
            NULL_POSITION,
            timestamp,
            memberIdVote,
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
                    buffer.getLong(i + RECORDING_ID_OFFSET),
                    buffer.getLong(i + LEADERSHIP_TERM_ID_OFFSET),
                    buffer.getLong(i + LOG_POSITION_OFFSET),
                    buffer.getLong(i + TERM_POSITION_OFFSET),
                    buffer.getLong(i + TIMESTAMP_OFFSET),
                    buffer.getInt(i + MEMBER_ID_VOTE_OFFSET),
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
            throw new IllegalStateException("Unknown recording id: " + entry.recordingId);
        }
    }
}
