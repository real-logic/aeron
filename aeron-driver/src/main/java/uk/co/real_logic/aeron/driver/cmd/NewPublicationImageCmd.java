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
package uk.co.real_logic.aeron.driver.cmd;

import uk.co.real_logic.aeron.driver.PublicationImage;
import uk.co.real_logic.aeron.driver.Receiver;
import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;

public class NewPublicationImageCmd implements ReceiverCmd
{
    private final ReceiveChannelEndpoint channelEndpoint;
    private final PublicationImage publicationImage;

    public NewPublicationImageCmd(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage publicationImage)
    {
        this.channelEndpoint = channelEndpoint;
        this.publicationImage = publicationImage;
    }

    public void execute(final Receiver receiver)
    {
        receiver.onNewPublicationImage(channelEndpoint, publicationImage);
    }
}
