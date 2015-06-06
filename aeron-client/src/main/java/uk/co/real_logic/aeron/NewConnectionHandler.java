/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

/**
 * Interface for delivery of new connection events to a {@link Aeron} instance.
 */
@FunctionalInterface
public interface NewConnectionHandler
{
    /**
     * Method called by Aeron to deliver notification of a new connected session.
     *
     * @param channel         The channel for the new session.
     * @param streamId        The scope within the channel for the new session.
     * @param sessionId       The publisher instance identifier for the new session.
     * @param joiningPosition At which the stream is being joined by the subscriber.
     * @param sourceIdentity  A transport specific string with additional details about the publisher.
     */
    void onNewConnection(String channel, int streamId, int sessionId, long joiningPosition, String sourceIdentity);
}
