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
package io.aeron.samples;

/**
 * Sends a large file in chunks to a {@link FileReceiver}.
 * <p>
 * Protocol is to send a {@code file-create} followed by 1 or more {@code file-chunk} messages that are all
 * linked via the correlation id. Messages are encoded in {@link java.nio.ByteOrder#LITTLE_ENDIAN}.
 * <p>
 * The chunk size if best determined by {@link io.aeron.Publication#maxPayloadLength()} minus header for the chunk.
 *
 * <b>file-create</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Version                              |
 *  +---------------------------------------------------------------+
 *  |                      Message Type = 1                         |
 *  +---------------------------------------------------------------+
 *  |                       Correlation ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        File Length                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Name Length                            |
 *  +---------------------------------------------------------------+
 *  |                          Name                                 |
 * ...                                                              |
 *  |                                                              ...
 *  +---------------------------------------------------------------+
 * </pre>
 * <b>file-chunk</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          Version                              |
 *  +---------------------------------------------------------------+
 *  |                      Message Type = 2                         |
 *  +---------------------------------------------------------------+
 *  |                       Correlation ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                           Offset                              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                           Length                              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        File Length                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class FileSender
{
}
