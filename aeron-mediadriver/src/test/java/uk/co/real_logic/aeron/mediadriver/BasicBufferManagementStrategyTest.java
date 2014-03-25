package uk.co.real_logic.aeron.mediadriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.util.IoUtil;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BasicBufferManagementStrategyTest
{

    private static final File dataDir = new File(System.getProperty("java.io.tmpdir"), "dataDir");

    @After
    public void cleanupFiles() throws IOException
    {
        IoUtil.delete(dataDir, true);
    }

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(dataDir, "data");
    }

    @Test
    public void mappedFilesAreCorrectSizeAndZeroed() throws Exception
    {
        BasicBufferManagementStrategy strategy = new BasicBufferManagementStrategy(dataDir.getAbsolutePath());
        MappedByteBuffer term = strategy.mapTerm(dataDir, 1, 1, 1, 256);

        assertThat(term.capacity(), is(256));
        assertThat(term.get(0), is((byte) 0));
        assertThat(term.get(255), is((byte) 0));
    }

}
