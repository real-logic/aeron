/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archiver;

class ArchiveUtil
{
    static final String RECORDING_SEGMENT_POSTFIX = ".rec";

    static String recordingDataFileName(final long recordingId, final int segmentIndex)
    {
        return recordingId + "." + segmentIndex + RECORDING_SEGMENT_POSTFIX;
    }

    static int segmentFileIndex(final long joinPosition, final long position, final int segmentFileLength)
    {
        return (int)((position - joinPosition) / segmentFileLength);
    }

    static int recordingOffset(
        final int termOffset,
        final int termId,
        final int initialTermId,
        final int termsMask,
        final int termBufferLength)
    {
        return ((termId - initialTermId) & termsMask) * termBufferLength + termOffset;
    }
}
