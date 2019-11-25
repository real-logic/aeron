package io.aeron.support;

import io.aeron.driver.MediaDriver;

public final class JavaTestMediaDriver implements TestMediaDriver
{
    private MediaDriver mediaDriver;

    private JavaTestMediaDriver(final MediaDriver mediaDriver)
    {
        this.mediaDriver = mediaDriver;
    }

    @Override
    public void close()
    {
        mediaDriver.close();
    }

    public static JavaTestMediaDriver launch(final MediaDriver.Context context)
    {
        final MediaDriver mediaDriver = MediaDriver.launch(context);
        return new JavaTestMediaDriver(mediaDriver);
    }

    @Override
    public MediaDriver.Context context()
    {
        return mediaDriver.context();
    }

    @Override
    public String aeronDirectoryName()
    {
        return mediaDriver.aeronDirectoryName();
    }
}
