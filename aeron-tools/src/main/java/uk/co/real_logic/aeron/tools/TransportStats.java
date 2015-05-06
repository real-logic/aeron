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

public class TransportStats
{
    protected String proto;
    protected String host;
    protected int port;
    protected long pos;
    protected String sessionId;
    protected boolean active;

    public TransportStats()
    {

    }

    public void setPos(final long pos)
    {
        if (pos != this.pos)
        {
            this.pos = pos;
            active = true;
        }
    }

    protected void parseChannel(final String channel)
    {
        String input = channel;
        proto = input.substring(0, input.indexOf(':'));
        input = input.substring(input.indexOf(':') + 3);
        host = input.substring(0, input.indexOf(':'));
        input = input.substring(input.indexOf(':') + 1);
        try
        {
            port = Integer.parseInt(input.substring(0, input.indexOf(' ')));
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }

        input = input.substring(input.indexOf(' ') + 1);
        sessionId = input.substring(0, input.indexOf(' '));

        input = input.substring(input.indexOf(' ') + 1);
    }
}
