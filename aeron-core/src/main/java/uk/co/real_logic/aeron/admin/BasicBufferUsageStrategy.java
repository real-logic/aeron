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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Basic buffer usage where each Term is a file.
 */
public class BasicBufferUsageStrategy implements BufferUsageStrategy
{
    private final FileMappingConvention fileConventions;
    private final long sessionId;

    public BasicBufferUsageStrategy(final long sessionId, final String dataDir)
    {
        this.sessionId = sessionId;
        fileConventions = new FileMappingConvention(dataDir);
    }

    @Override
    public ByteBuffer onTermAdded(final long channelId, final long termId, boolean isSender) throws IOException
    {
        final File rootDir = isSender ? fileConventions.senderDir() : fileConventions.receiverDir();
        return mapTerm(channelId, termId, rootDir);
    }

    private MappedByteBuffer mapTerm(final long channelId, final long termId, final File rootDir) throws IOException
    {
        final File termIdFile = FileMappingConvention.termIdFile(rootDir, sessionId, channelId, termId, false);
        return IoUtil.mapExistingFile(termIdFile, "Term Buffer");
    }

}
