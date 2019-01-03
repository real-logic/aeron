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

/**
 * Feedback delay generator.
 */
public interface FeedbackDelayGenerator
{
    /**
     * Generate a new delay value
     *
     * @return delay value in nanoseconds
     */
    long generateDelay();

    /**
     * Should feedback be immediately sent?
     *
     * @return whether feedback should be immediate or not
     */
    default boolean shouldFeedbackImmediately()
    {
        return false;
    }
}
