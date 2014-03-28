package uk.co.real_logic.aeron.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.IoUtil.mapExistingFile;
import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

public class BasicAdminBufferStrategy implements AdminBufferStrategy
{

    private static final String MEDIA_DRIVER_FILE = "media-driver";
    private static final String API_FILE = "api";

    private final File toMediaDriver;
    private final File toApi;
    private final boolean isMediaDriver;
    private final int bufferSize;

    public BasicAdminBufferStrategy(final String adminDir, final int bufferSize, final boolean isMediaDriver)
    {
        this.bufferSize = bufferSize;
        toMediaDriver = new File(adminDir, MEDIA_DRIVER_FILE);
        toApi = new File(adminDir, API_FILE);
        this.isMediaDriver = isMediaDriver;
    }

    @Override
    public ByteBuffer toMediaDriver() throws IOException
    {
        return isMediaDriver ? mapNewFile(toMediaDriver, MEDIA_DRIVER_FILE, bufferSize)
                             : mapExistingFile(toMediaDriver, MEDIA_DRIVER_FILE);
    }

    @Override
    public ByteBuffer toApi() throws IOException
    {
        return isMediaDriver ? mapExistingFile(toApi, API_FILE)
                             : mapNewFile(toApi, API_FILE, bufferSize);
    }

}
