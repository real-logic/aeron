/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.client;

/**
 * Consumer for descriptors of active archive recording {@link io.aeron.Subscription}s.
 */
@FunctionalInterface
public interface RecordingSubscriptionDescriptorConsumer
{
    /**
     * Descriptor for an active recording subscription on the archive.
     *
     * @param controlSessionId for the request.
     * @param correlationId    for the request.
     * @param subscriptionId   that can be used to stop the recording subscription.
     * @param streamId         the subscription was registered with.
     * @param strippedChannel  the subscription was registered with.
     */
    void onSubscriptionDescriptor(
        long controlSessionId,
        long correlationId,
        long subscriptionId,
        int streamId,
        String strippedChannel);
}
