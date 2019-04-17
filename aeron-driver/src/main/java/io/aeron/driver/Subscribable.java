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
package io.aeron.driver;

import org.agrona.concurrent.status.ReadablePosition;

/**
 * Stream source that can be observed by subscribers which identify themselves the position they have read up to.
 */
public interface Subscribable
{
    /**
     * Add a subscriber and its position used for tracking consumption.
     *
     * @param subscriptionLink   for identifying the subscriber.
     * @param subscriberPosition for tracking the subscriber.
     */
    void addSubscriber(SubscriptionLink subscriptionLink, ReadablePosition subscriberPosition);

    /**
     * Remove a subscriber and its position used for tracking consumption.
     * <p>
     * <b>Note:</b> The {@link Subscribable} is responsible for calling {@link ReadablePosition#close()} on
     * removed positions.
     *
     * @param subscriptionLink   for identifying the subscriber.
     * @param subscriberPosition for tracking the subscriber.
     */
    void removeSubscriber(SubscriptionLink subscriptionLink, ReadablePosition subscriberPosition);
}
