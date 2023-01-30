package io.aeron.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class NodeStateFileTest
{
    @Test
    void shouldFailIfCreateNewFalseAndFileDoesNotExist(@TempDir final File archiveDir)
    {
        assertThrows(IOException.class, () -> new NodeStateFile(archiveDir, false));
    }

    @Test
    void shouldCreateIfCreateNewTrueAndFileDoesNotExist(@TempDir final File archiveDir) throws IOException
    {
        assertEquals(0, Objects.requireNonNull(archiveDir.list()).length);
        final NodeStateFile nodeStateFile = new NodeStateFile(archiveDir, true);
        assertTrue(new File(archiveDir, NodeStateFile.FILENAME).exists());
    }
}