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
package io.aeron.driver;

import org.agrona.concurrent.status.ReadablePosition;

/**
 * Stream source that can be observed by subscribers which identify themselves the position they have read up to.
 */
public interface Subscribable
{
    /**
     * Add a subscriber identified by its position.
     *
     * @param subscriberPosition for tracking and identifying the subscriber.
     */
    void addSubscriber(ReadablePosition subscriberPosition);

    /**
     * Remove a subscriber identified by its position.
     * <p>
     * <b>Note:</b> The {@link Subscribable} is responsible for calling {@link ReadablePosition#close()} on
     * removed positions.
     *
     * @param subscriberPosition to be identified by.
     */
    void removeSubscriber(ReadablePosition subscriberPosition);
}
