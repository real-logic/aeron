package uk.co.real_logic.aeron.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

public class CreatingAdminBufferStrategy extends AdminBufferStrategy
{

    private final int bufferSize;

    public CreatingAdminBufferStrategy(final String adminDir, final int bufferSize)
    {
        super(adminDir);
        this.bufferSize = bufferSize;
    }

    public ByteBuffer toMediaDriver() throws IOException
    {
        return mapNewFile(toMediaDriver, MEDIA_DRIVER_FILE, bufferSize);
    }

    public ByteBuffer toApi() throws IOException
    {
        return mapNewFile(toApi, API_FILE, bufferSize);
    }

}
