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
 * The aeron-archive is an module which enables Aeron data stream recording and replay from persistent storage.
 * <p>
 * Features:
 * <ul>
 *     <li><b>Record:</b> service can record a particular subscription, described by {@code <channel, streamId>}. Each
 *     resulting image for the subscription will be recorded under a new {@code recordingId}. Local network publications
 *     are recorded using the spy feature for efficiency.</li>
 *     <li><b>Extend:</b> an existing recording by appending.</li>
 *     <li><b>Replay:</b> service can replay a recorded {@code recordingId} from a particular position and length.</li>
 *     <li><b>Query:</b> the catalog for existing recordings and the recorded position of an active recording.</li>
 *     <li><b>Truncate:</b> allows a stopped recording to have its length truncated, and if truncated to the start
 *     position then it is effectively deleted.</li>
 *     <li><b>Replay Merge:</b> allows a late joining subscriber of a recorded stream to replay a recording and then
 *     merge with the live stream for cut over if the consumer is fast enough to keep up.</li>
 * </ul>
 */
package io.aeron.archive;