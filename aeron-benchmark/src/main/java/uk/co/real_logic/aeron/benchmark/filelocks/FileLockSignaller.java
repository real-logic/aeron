package uk.co.real_logic.aeron.benchmark.filelocks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class FileLockSignaller implements Signaller
{
    private final FileChannel channel;

    private FileLock fileLock;

    public FileLockSignaller(final String path)
    {
        try
        {
            final RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
            channel = randomAccessFile.getChannel();
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void await()
    {
        try
        {
            fileLock = channel.lock();
            fileLock.release();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start()
    {
        try
        {
            fileLock = channel.lock();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void signal()
    {
        try
        {
            fileLock.release();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

}
