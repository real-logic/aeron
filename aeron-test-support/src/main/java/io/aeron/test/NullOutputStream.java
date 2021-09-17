package io.aeron.test;

import java.io.IOException;
import java.io.OutputStream;

public class NullOutputStream extends OutputStream
{
    public void write(final int ignore) throws IOException
    {
    }
}
