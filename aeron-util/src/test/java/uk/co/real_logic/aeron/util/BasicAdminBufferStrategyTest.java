package uk.co.real_logic.aeron.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BasicAdminBufferStrategyTest
{

    private static final File ADMIN_DIR = new File(System.getProperty("java.io.tmpdir"), "ADMIN_DIR");

    @After
    public void cleanupFiles() throws IOException
    {
        IoUtil.delete(ADMIN_DIR, true);
    }

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(ADMIN_DIR, "data");
    }

    private final BasicAdminBufferStrategy mediaDriverStrategy = new BasicAdminBufferStrategy(ADMIN_DIR, 5, true);
    private final BasicAdminBufferStrategy apiStrategy = new BasicAdminBufferStrategy(ADMIN_DIR, 10, false);

    @Test(expected = IllegalStateException.class)
    public void mediaDriverDoesntMapApiBuffer() throws IOException
    {
        mediaDriverStrategy.toApi();
    }

    @Test(expected = IllegalStateException.class)
    public void apiDoesntMapMediaDriverBuffer() throws IOException
    {
        apiStrategy.toMediaDriver();
    }

    @Test
    public void mediaDriverBuffersAreTheSame() throws IOException
    {
        assertSameBuffer(mediaDriverStrategy.toMediaDriver(), apiStrategy.toMediaDriver());
    }

    @Test
    public void apiBuffersAreTheSame() throws IOException
    {
        assertSameBuffer(apiStrategy.toApi(), mediaDriverStrategy.toApi());
    }

    private void assertSameBuffer(final ByteBuffer left, final ByteBuffer right)
    {
        assertThat(left.capacity(), is(right.capacity()));
        left.put(0, (byte) 1);
        assertThat(right.get(), is((byte) 1));
    }

}
