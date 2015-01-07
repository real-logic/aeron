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
package uk.co.real_logic.aeron.driver.buffer;

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.MappedByteBuffer;

/**
 * Memory mapped raw logs to make up a term log buffer.
 */
class MappedRawLogFragment implements RawLogFragment
{
    private final MappedByteBuffer mappedTermBuffer;
    private final MappedByteBuffer mappedMetaDataBuffer;

    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer metaDataBuffer;

    MappedRawLogFragment(final MappedByteBuffer termBuffer, final MappedByteBuffer metaDataBuffer)
    {
        this.mappedTermBuffer = termBuffer;
        this.mappedMetaDataBuffer = metaDataBuffer;

        this.metaDataBuffer = new UnsafeBuffer(metaDataBuffer);
        this.termBuffer = new UnsafeBuffer(termBuffer);
    }

    public UnsafeBuffer termBuffer()
    {
        return termBuffer;
    }

    public UnsafeBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }

    public void close()
    {
        IoUtil.unmap(mappedTermBuffer);
        IoUtil.unmap(mappedMetaDataBuffer);
    }
}
