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

public class ReceiverChannel
{
    private final Destination destination;
    private final long channelId;
    private final Receiver.DataHandler value;

    public ReceiverChannel(final Destination destination, final long channelId, final Receiver.DataHandler value)
    {
        this.destination = destination;
        this.channelId = channelId;
        this.value = value;
    }

    public boolean matches(final String destination, final long channelId)
    {
        return this.destination.equals(destination) && this.channelId == channelId;
    }

    public void process() throws Exception
    {

    }

}
