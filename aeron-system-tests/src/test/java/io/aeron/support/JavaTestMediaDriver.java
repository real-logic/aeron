package io.aeron.support;

import io.aeron.driver.MediaDriver;

public class JavaTestMediaDriver implements TestMediaDriver
{
    private MediaDriver mediaDriver;

    private JavaTestMediaDriver(MediaDriver mediaDriver)
    {
        this.mediaDriver = mediaDriver;
    }

    @Override
    public void close()
    {
        mediaDriver.close();
    }

    public static JavaTestMediaDriver launch(MediaDriver.Context context)
    {
        MediaDriver mediaDriver = MediaDriver.launch(context);
        return new JavaTestMediaDriver(mediaDriver);
    }

    @Override
    public MediaDriver.Context context()
    {
        return mediaDriver.context();
    }
}
