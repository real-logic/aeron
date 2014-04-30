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
package uk.co.real_logic.aeron.util;

/**
 * .Common Information around buffer rotation, used by both the client and core API.
 */
public class BufferRotationDescriptor
{
    public static final int BUFFER_COUNT = 3;

    public static final int CLEAN_WINDOW = 1;

    public static final long UNKNOWN_TERM_ID = -1;

    public static int rotateId(int previous)
    {
        return BitUtil.next(previous, BUFFER_COUNT);
    }
}
