/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.archive;

/**
 * Sessions are created by the conductor but perform their work on a different {@link SessionWorker} potentially. After
 * construction sessions are safely published to the {@link SessionWorker} and thereafter interacted with only from
 * that thread until they are done. Once done they are closed from the conductor.
 */
interface Session
{
    void abort();

    boolean isDone();

    int doWork();

    long sessionId();

    void close();
}
