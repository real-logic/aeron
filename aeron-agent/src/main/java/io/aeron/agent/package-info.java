/*
 * Copyright 2014-2019 Real Logic Ltd.
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
/**
 * A Java agent which when attached to a JVM will weave byte code to intercept and record events as defined by
 * {@link io.aeron.agent.DriverEventCode}. These events are recorded to an in-memory
 * {@link org.agrona.concurrent.ringbuffer.RingBuffer} which is consumed
 * and appended asynchronous to a log as defined by the class
 * {@link io.aeron.agent.EventLogAgent#READER_CLASSNAME_PROP_NAME} which defaults to
 * {@link io.aeron.agent.EventLogReaderAgent}.
 */
package io.aeron.agent;