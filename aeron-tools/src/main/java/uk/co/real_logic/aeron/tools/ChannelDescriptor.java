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
 */
public class ChannelDescriptor
{
    private String channel;
    private int[] streamIds;

    ChannelDescriptor()
    {
        channel = null;
        streamIds = null;
    }

    public String channel()
    {
        return channel;
    }

    public void channel(final String c)
    {
        channel = c;
    }

    public int[] streamIdentifiers()
    {
        return streamIds;
    }

    public void streamIdentifiers(final int[] ids)
    {
        streamIds = ids;
    }

    public String toString()
    {
        return channel;
    }
}
