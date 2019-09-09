/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.archive.codecs.RecordingTransitionType;

/**
 * Consumer of transition events related to the lifecycle of stream recordings.
 */
@FunctionalInterface
public interface RecordingTransitionConsumer
{
    /**
     * Transition event in the lifecycle of a recording.
     *
     * @param controlSessionId that initialed the action.
     * @param recordingId      which has transitioned.
     * @param subscriptionId   of the Subscription associated with the recording transition.
     * @param position         of the recorded stream at the point of transition.
     * @param transitionType   type of the transition the recording has undertaken.
     */
    void onTransition(
        long controlSessionId,
        long recordingId,
        long subscriptionId,
        long position,
        RecordingTransitionType transitionType);
}
