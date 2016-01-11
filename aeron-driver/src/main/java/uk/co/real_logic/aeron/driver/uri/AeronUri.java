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
package uk.co.real_logic.aeron.driver.uri;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Parser for Aeron uri used for configuring channels.  The format is:
 *
 * <pre>
 * aeron-uri = "aeron:" media [ "?" param *( "|" param ) ]
 * media     = *( "[^?:]" )
 * param     = key "=" value
 * key       = *( "[^=]" )
 * value     = *( "[^|]" )
 * </pre>
 *
 * Multiple params with the same key are allowed, the last value specified 'wins'.
 */
public class AeronUri
{
    private enum State
    {
        MEDIA, PARAMS_KEY, PARAMS_VALUE
    }

    private static final String AERON_PREFIX = "aeron:";
    private final String scheme;
    private final String media;
    private final Map<String, String> params;

    public AeronUri(final String scheme, final String media, final Map<String, String> params)
    {
        this.scheme = scheme;
        this.media = media;
        this.params = params;
    }

    public String media()
    {
        return media;
    }

    public String scheme()
    {
        return scheme;
    }

    public String get(final String key)
    {
        return params.get(key);
    }

    public String get(final String key, final String defaultValue)
    {
        final String value = params.get(key);

        if (null != value)
        {
            return value;
        }

        return defaultValue;
    }

    public InetAddress getInetAddress(final String key) throws UnknownHostException
    {
        return InetAddress.getByName(get(key));
    }

    public InetSocketAddress getSocketAddress(final String key)
    {
        return SocketAddressUtil.parse(get(key));
    }

    public InetSocketAddress getSocketAddress(final String key, final int defaultPort, final InetSocketAddress defaultValue)
    {
        if (!containsKey(key))
        {
            return defaultValue;
        }

        return SocketAddressUtil.parse(get(key), defaultPort);
    }

    public InterfaceSearchAddress getInterfaceSearchAddress(final String key, final InterfaceSearchAddress defaultValue)
        throws UnknownHostException
    {
        if (!containsKey(key))
        {
            return defaultValue;
        }

        return InterfaceSearchAddress.parse(get(key));
    }

    public boolean containsKey(final String key)
    {
        return params.containsKey(key);
    }

    public boolean containsAnyKey(String[] keys)
    {
        for (final String key : keys)
        {
            if (params.containsKey(key))
            {
                return true;
            }
        }

        return false;
    }

    public static AeronUri parse(final CharSequence cs)
    {
        if (!startsWith(cs, AERON_PREFIX))
        {
            throw new IllegalArgumentException("AeronUri must start with 'aeron:', found: '" + cs + "'");
        }

        final StringBuilder builder = new StringBuilder();

        final String scheme = "aeron";
        final Map<String, String> params = new HashMap<>();
        String media = null;
        String key = null;

        State state = State.MEDIA;
        for (int i = AERON_PREFIX.length(); i < cs.length(); i++)
        {
            final char c = cs.charAt(i);

            switch (state)
            {
                case MEDIA:
                    switch (c)
                    {
                        case '?':
                            media = builder.toString();
                            builder.setLength(0);
                            state = State.PARAMS_KEY;
                            break;

                        case ':':
                            throw new IllegalArgumentException("Encountered ':' within media definition");

                        default:
                            builder.append(c);
                    }
                    break;

                case PARAMS_KEY:
                    switch (c)
                    {
                        case '=':
                            key = builder.toString();
                            builder.setLength(0);
                            state = State.PARAMS_VALUE;
                            break;

                        default:
                            builder.append(c);
                    }
                    break;

                case PARAMS_VALUE:
                    switch (c)
                    {
                        case '|':
                            params.put(key, builder.toString());
                            builder.setLength(0);
                            state = State.PARAMS_KEY;
                            break;

                        default:
                            builder.append(c);
                    }
                    break;

                default:
                    throw new IllegalStateException("Que?  State = " + state);
            }
        }

        switch (state)
        {
            case MEDIA:
                media = builder.toString();
                break;

            case PARAMS_VALUE:
                params.put(key, builder.toString());
                break;

            default:
                throw new IllegalArgumentException("No more input found, but was in state: " + state);
        }

        return new AeronUri(scheme, media, params);
    }

    private static boolean startsWith(final CharSequence input, final CharSequence prefix)
    {
        if (input.length() < prefix.length())
        {
            return false;
        }

        for (int i = 0; i < prefix.length(); i++)
        {
            if (input.charAt(i) != prefix.charAt(i))
            {
                return false;
            }
        }

        return true;
    }

    public static class Builder
    {
        private final Map<String, String> params = new HashMap<>();
        private String media;

        public Builder media(final String media)
        {
            this.media = media;
            return this;
        }

        public Builder param(final String key, final String value)
        {
            if (null != key && null != value)
            {
                params.put(key, value);
            }

            return this;
        }

        public AeronUri newInstance()
        {
            return new AeronUri("aeron", media, params);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }
}
