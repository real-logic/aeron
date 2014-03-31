package uk.co.real_logic.aeron.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.IoUtil.mapExistingFile;

public class MappingAdminBufferStrategy extends AdminBufferStrategy
{

    public MappingAdminBufferStrategy(final String adminDir)
    {
        super(adminDir);
    }

    @Override
    public ByteBuffer toMediaDriver() throws IOException
    {
        return mapExistingFile(toMediaDriver, MEDIA_DRIVER_FILE);
    }

    @Override
    public ByteBuffer toApi() throws IOException
    {
        return mapExistingFile(toApi, API_FILE);
    }

}
