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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

/**
 * Interface for delivery of data to a {@link uk.co.real_logic.aeron.Subscription}
 *
 * Each fragment delivered will be a whole message if it is under MTU size. If larger than MTU side then it will come
 * as a series of fragments ordered withing a session.
 */
public interface DataHandler
{
    /**
     * Method called by Aeron to deliver data to a {@link uk.co.real_logic.aeron.Subscription}
     *
     * @param buffer    to be delivered
     * @param offset    within buffer that data starts
     * @param length    of the data in the buffer
     * @param sessionId for the data source
     * @param flags     for the status of the frame
     */
    void onData(AtomicBuffer buffer, int offset, int length, int sessionId, byte flags);
}
