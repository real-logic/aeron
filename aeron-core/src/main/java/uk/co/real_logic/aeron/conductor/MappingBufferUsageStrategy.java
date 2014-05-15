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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.IoUtil.mapExistingFile;

/**
 * Buffer usage where each Term is a memory mapped file.
 */
public class MappingBufferUsageStrategy implements BufferUsageStrategy
{
    private final FileMappingConvention fileConventions;
    private final List<IdentifiedBuffer> publisherBuffers;
    private final List<IdentifiedBuffer> subscriberBuffers;

    public MappingBufferUsageStrategy(final String dataDir)
    {
        fileConventions = new FileMappingConvention(dataDir);
        publisherBuffers = new ArrayList<>();
        subscriberBuffers = new ArrayList<>();
    }

    public void close()
    {
        publisherBuffers.forEach(IdentifiedBuffer::close);
        subscriberBuffers.forEach(IdentifiedBuffer::close);
    }

    public AtomicBuffer newPublisherLogBuffer(final String destination,
                                              final long sessionId,
                                              final long channelId,
                                              final int index) throws IOException
    {
        return map(destination, sessionId, channelId, index, fileConventions.senderDir(), LOG, publisherBuffers);
    }

    public AtomicBuffer newPublisherStateBuffer(final String destination,
                                                final long sessionId,
                                                final long channelId,
                                                final int index) throws IOException
    {
        return map(destination, sessionId, channelId, index, fileConventions.senderDir(), STATE, publisherBuffers);
    }

    public AtomicBuffer newSubscriberLogBuffer(final String destination,
                                               final long channelId,
                                               final long sessionId,
                                               final int index) throws IOException
    {
        return map(destination, sessionId, channelId, index, fileConventions.receiverDir(), LOG, subscriberBuffers);
    }

    public AtomicBuffer newSubscriberStateBuffer(final String destination,
                                                 final long channelId,
                                                 final long sessionId,
                                                 final int index) throws IOException
    {
        return map(destination, sessionId, channelId, index, fileConventions.receiverDir(), STATE, subscriberBuffers);
    }

    public int releaseSubscriberBuffers(final String destination, final long sessionId, final long channelId)
    {
        return release(destination, sessionId, channelId, subscriberBuffers);
    }

    public int releasePublisherBuffers(final String destination, final long sessionId, final long channelId)
    {
        return release(destination, sessionId, channelId, publisherBuffers);
    }

    private AtomicBuffer map(final String destination,
                             final long sessionId,
                             final long channelId,
                             final int index,
                             final File rootDir,
                             final FileMappingConvention.Type type,
                             final List<IdentifiedBuffer> buffers) throws IOException
    {
        final File termIdFile = termLocation(rootDir, sessionId, channelId, index, false, destination, type);
        MappedByteBuffer buffer = mapExistingFile(termIdFile, "Term Buffer");
        buffers.add(new IdentifiedBuffer(destination, sessionId, channelId, buffer));
        return new AtomicBuffer(buffer);
    }

    private int release(final String destination,
                        final long sessionId,
                        final long channelId,
                        final List<IdentifiedBuffer> buffers)
    {
        int count = 0;
        final Iterator<IdentifiedBuffer> it = buffers.iterator();
        while (it.hasNext())
        {
            final IdentifiedBuffer buffer = it.next();
            if (buffer.matches(destination, sessionId, channelId))
            {
                buffer.close();
                it.remove();
                count++;
            }
        }

        return count;
    }

    private class IdentifiedBuffer
    {
        private final String destination;
        private final long sessionId;
        private final long channelId;
        private final MappedByteBuffer buffer;

        private IdentifiedBuffer(final String destination,
                                 final long sessionId,
                                 final long channelId,
                                 final MappedByteBuffer buffer)
        {
            this.destination = destination;
            this.sessionId = sessionId;
            this.channelId = channelId;
            this.buffer = buffer;
        }

        public boolean matches(final String destination, final long sessionId, final long channelId)
        {
            return this.sessionId == sessionId && this.channelId == channelId && this.destination.equals(destination);
        }

        public void close()
        {
            IoUtil.unmap(buffer);
        }
    }
}
