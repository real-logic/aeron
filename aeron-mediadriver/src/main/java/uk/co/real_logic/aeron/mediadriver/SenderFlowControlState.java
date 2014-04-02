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

/**
 * Encapsulate the flow control status of a sender channel
 */
public class SenderFlowControlState
{
    private int highestContiguousSequenceNumber;
    private int receiverWindow;

    public SenderFlowControlState(final int initialSeqNum, final int initialWindow)
    {
        this.highestContiguousSequenceNumber = initialSeqNum;
        this.receiverWindow = initialWindow;
    }

    public void reset(final int highSeqNum, final int window)
    {
        highestContiguousSequenceNumber = highSeqNum;
        receiverWindow = window;
    }

    public int rightEdgeOfWindow()
    {
        return highestContiguousSequenceNumber + receiverWindow;
    }
}
