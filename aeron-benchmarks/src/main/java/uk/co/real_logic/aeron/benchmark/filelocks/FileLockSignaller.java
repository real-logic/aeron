/*
 * Copyright 2014 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
