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
package uk.co.real_logic.aeron.mediadriver;

public class DefaultSenderFlowControlStrategy implements SenderFlowControlStrategy
{
    private int rightEdgeOfWindow;

    public DefaultSenderFlowControlStrategy()
    {
        rightEdgeOfWindow = 0;
    }

    public int onStatusMessage(final long termId, final long highestContiguousSequenceNumber, final long receiverWindow)
    {
        // TODO: review this logic
        final int newRightEdgeOfWindow = (int) (highestContiguousSequenceNumber + receiverWindow);
        rightEdgeOfWindow = Math.max(rightEdgeOfWindow, newRightEdgeOfWindow);
        return rightEdgeOfWindow;
    }

}
