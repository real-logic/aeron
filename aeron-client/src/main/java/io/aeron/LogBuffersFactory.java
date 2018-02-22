/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;


/**
 * Interface for encapsulating the strategy of mapping {@link LogBuffers} at a giving file location.
 */
interface LogBuffersFactory
{
    /**
     * Map a log file into memory and wrap each section with a {@link org.agrona.concurrent.UnsafeBuffer}.
     *
     * @param logFileName to be mapped into memory.
     * @return a representation of the mapped log buffer.
     */
    LogBuffers map(String logFileName);
}
