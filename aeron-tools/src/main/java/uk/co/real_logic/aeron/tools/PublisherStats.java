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

public class PublisherStats extends TransportStats
{
    private long limit;

    public PublisherStats(final String channel)
    {
        parseChannel(channel);
        active = true;
    }

    public void setLimit(final long limit)
    {
        if (limit != this.limit)
        {
            this.limit = limit;
            active = true;
        }
    }

    public String toString()
    {
        final String s = String.format("%1$5s %2$8d %3$8d %4$10s:%5$5d %6$s%7$s %8$8s\n",
            proto, pos, limit, host, port, "0x", sessionId, active ? "ACTIVE" : "INACTIVE");
        active = false;

        return s;
    }
}
