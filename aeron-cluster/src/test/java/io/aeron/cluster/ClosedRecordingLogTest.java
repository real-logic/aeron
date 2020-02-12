package io.aeron.cluster;

import io.aeron.cluster.client.ClusterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClosedRecordingLogTest
{

    private RecordingLog recordingLog;

    @BeforeEach
    void before(final @TempDir Path dir)
    {
        recordingLog = new RecordingLog(dir.toFile());
        recordingLog.close();
    }

    @Test
    void forceThrowsClusterException()
    {
        assertClosedExceptionThrown(() -> recordingLog.force(1));
    }

    @Test
    void reloadThrowsClusterException()
    {
        assertClosedExceptionThrown(recordingLog::reload);
    }

    @Test
    void invalidateLatestSnapshotThrowsClusterException()
    {
        assertClosedExceptionThrown(recordingLog::invalidateLatestSnapshot);
    }

    @Test
    void appendTermThrowsClusterException()
    {
        assertClosedExceptionThrown(() -> recordingLog.appendTerm(42, 1, 128, 0));
    }

    @Test
    void appendSnapshotThrowsClusterException()
    {
        assertClosedExceptionThrown(() -> recordingLog.appendSnapshot(33, 21, 0, 10, 0, -1));
    }

    @Test
    void commitLogPositionThrowsClusterException()
    {
        assertClosedExceptionThrown(() -> recordingLog.commitLogPosition(42, 128));
    }

    @Test
    void invalidateEntryThrowsClusterException()
    {
        assertClosedExceptionThrown(() -> recordingLog.invalidateEntry(42, 0));
    }

    @Test
    void removeEntryThrowsClusterException()
    {
        assertClosedExceptionThrown(() -> recordingLog.removeEntry(42, 0));
    }

    private void assertClosedExceptionThrown(final Executable action)
    {
        final ClusterException ex = assertThrows(ClusterException.class, action);
        assertEquals("RecordingLog is closed!", ex.getMessage());
    }
}