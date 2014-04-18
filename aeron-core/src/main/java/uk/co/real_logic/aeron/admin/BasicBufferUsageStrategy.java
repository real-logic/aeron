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
package uk.co.real_logic.aeron.admin;

import uk.co.real_logic.aeron.util.FileMappingConvention;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.io.File;
import java.io.IOException;

import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;

/**
 * Basic buffer usage where each Term is a file.
 */
public class BasicBufferUsageStrategy implements BufferUsageStrategy
{
    private final FileMappingConvention fileConventions;

    public BasicBufferUsageStrategy(final String dataDir)
    {
        fileConventions = new FileMappingConvention(dataDir);
    }

    public AtomicBuffer newSenderLogBuffer(final String destination,
                                           final long sessionId,
                                           final long channelId,
                                           final int index) throws IOException
    {
        return mapTerm(destination, sessionId, channelId, index, fileConventions.senderDir(), LOG);
    }

    public AtomicBuffer newSenderStateBuffer(final String destination,
                                             final long sessionId,
                                             final long channelId,
                                             final int index) throws IOException
    {
        return mapTerm(destination, sessionId, channelId, index, fileConventions.senderDir(), STATE);
    }

    public AtomicBuffer newReceiverLogBuffer(final String destination,
                                             final long channelId,
                                             final int index) throws IOException
    {
        // TODO: parameterise session
        return mapTerm(destination, 3L, channelId, index, fileConventions.receiverDir(), LOG);
    }

    public AtomicBuffer newReceiverStateBuffer(final String destination,
                                               final long channelId,
                                               final int index) throws IOException
    {
        // TODO: parameterise session
        return mapTerm(destination, 3L, channelId, index, fileConventions.receiverDir(), STATE);
    }

    private AtomicBuffer mapTerm(final String destination,
                                 final long sessionId,
                                 final long channelId,
                                 final long termId,
                                 final File rootDir,
                                 final FileMappingConvention.Type type) throws IOException
    {
        final File termIdFile = termLocation(rootDir, sessionId, channelId, termId, false, destination, type);
        return new AtomicBuffer(IoUtil.mapExistingFile(termIdFile, "Term Buffer"));
    }
}
