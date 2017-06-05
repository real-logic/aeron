/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

/**
 * Threading mode to be employed by the {@link org.agrona.concurrent.Agent}s in the {@link Archiver}.
 */
public enum ArchiverThreadingMode
{
    /**
     * No threads are started in the {@link Archiver}.
     * <p>
     * The {@link Archiver} will be made runnable via an {@link Archiver#invoker()}
     */
    INVOKER,

    /**
     * One thread shared by all {@link Archiver} {@link org.agrona.concurrent.Agent}s.
     */
    SHARED,

    /**
     * 3 Threads, one dedicated to each of the {@link org.agrona.concurrent.Agent}s.
     */
    DEDICATED
}
