/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

/**
 * This class is responsible for holding an Aeron channel and all the stream Ids that are on it.
 * Created by bhorst on 3/4/15.
 */
public class ChannelDescriptor
{
    String channel;
    int[] streamIds;

    ChannelDescriptor()
    {
        channel = null;
        streamIds = null;
    }

    public String getChannel()
    {
        return channel;
    }

    public void setChannel(final String c)
    {
        channel = c;
    }

    public int[] getStreamIdentifiers()
    {
        return streamIds;
    }

    public void setStreamIdentifiers(final int[] ids)
    {
        streamIds = ids;
    }

    public String toString()
    {
        return channel;
    }
}
