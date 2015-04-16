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
package uk.co.real_logic.aeron.driver;

/**
 * Handler for transmitting NAK messages
 */
@FunctionalInterface
public interface NakMessageSender
{
    /**
     * Called when a gap has not been filled.
     *
     * @param termId     for the gap
     * @param termOffset for the beginning of the gap
     * @param length     of the gap
     */
    void onLossDetected(int termId, int termOffset, int length);
}
