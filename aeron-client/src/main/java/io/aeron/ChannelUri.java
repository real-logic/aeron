/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.AsciiEncoding;
import org.agrona.collections.ArrayUtil;

import java.util.*;

import static io.aeron.CommonContext.*;

/**
 * Parser for Aeron channel URIs. The format is:
 * <pre>
 * aeron-uri = "aeron:" media [ "?" param *( "|" param ) ]
 * media     = *( "[^?:]" )
 * param     = key "=" value
 * key       = *( "[^=]" )
 * value     = *( "[^|]" )
 * </pre>
 * <p>
 * Multiple params with the same key are allowed, the last value specified takes precedence.
 * @see ChannelUriStringBuilder
 */
public class ChannelUri
{
    private enum State
    {
        MEDIA, PARAMS_KEY, PARAMS_VALUE
    }

    /**
     * URI Scheme for Aeron channels.
     */
    public static final String AERON_SCHEME = "aeron";

    /**
     * Qualifier for spy subscriptions which spy on outgoing network destined traffic efficiently.
     */
    public static final String SPY_QUALIFIER = "aeron-spy";

    public static final long INVALID_TAG = Aeron.NULL_VALUE;

    private static final int CHANNEL_TAG_INDEX = 0;
    private static final int ENTITY_TAG_INDEX = 1;

    private static final String AERON_PREFIX = AERON_SCHEME + ":";

    private String prefix;
    private String media;
    private final Map<String, String> params;
    private String[] tags;

    /**
     * Construct with the components provided to avoid parsing.
     *
     * @param prefix empty if no prefix is required otherwise expected to be 'aeron-spy'
     * @param media  for the channel which is typically "udp" or "ipc".
     * @param params for the query string as key value pairs.
     */
    public ChannelUri(final String prefix, final String media, final Map<String, String> params)
    {
        this.prefix = prefix;
        this.media = media;
        this.params = params;

        this.tags = splitTags(params.get(TAGS_PARAM_NAME));
    }

    /**
     * Construct with the components provided to avoid parsing.
     *
     * @param media  for the channel which is typically "udp" or "ipc".
     * @param params for the query string as key value pairs.
     */
    public ChannelUri(final String media, final Map<String, String> params)
    {
        this("", media, params);
    }

    /**
     * The prefix for the channel.
     *
     * @return the prefix for the channel.
     */
    public String prefix()
    {
        return prefix;
    }

    /**
     * Change the prefix from what has been parsed.
     *
     * @param prefix to replace the existing prefix.
     * @return this for a fluent API.
     */
    public ChannelUri prefix(final String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    /**
     * The media over which the channel operates.
     *
     * @return the media over which the channel operates.
     */
    public String media()
    {
        return media;
    }

    /**
     * Set the media over which the channel operates.
     *
     * @param media to replace the parsed value.
     * @return this for a fluent API.
     */
    public ChannelUri media(final String media)
    {
        this.media = media;
        return this;
    }

    /**
     * The scheme for the URI. Must be "aeron".
     *
     * @return the scheme for the URI.
     */
    public String scheme()
    {
        return AERON_SCHEME;
    }

    /**
     * Get a value for a given parameter key.
     *
     * @param key to lookup.
     * @return the value if set for the key otherwise null.
     */
    public String get(final String key)
    {
        return params.get(key);
    }

    /**
     * Get the value for a given parameter key or the default value provided if the key does not exist.
     *
     * @param key          to lookup.
     * @param defaultValue to be returned if no key match is found.
     * @return the value if set for the key otherwise the default value provided.
     */
    public String get(final String key, final String defaultValue)
    {
        final String value = params.get(key);
        if (null != value)
        {
            return value;
        }

        return defaultValue;
    }

    /**
     * Put a key and value pair in the map of params.
     *
     * @param key   of the param to be put.
     * @param value of the param to be put.
     * @return the existing value otherwise null.
     */
    public String put(final String key, final String value)
    {
        return params.put(key, value);
    }

    /**
     * Remove a key pair in the map of params.
     *
     * @param key of the param to be removed.
     * @return the previous value of the param or null.
     */
    public String remove(final String key)
    {
        return params.remove(key);
    }

    /**
     * Does the URI contain a value for the given key.
     *
     * @param key to be lookup.
     * @return true if the key has a value otherwise false.
     */
    public boolean containsKey(final String key)
    {
        return params.containsKey(key);
    }

    /**
     * Get the channel tag.
     *
     * @return channel tag.
     */
    public String channelTag()
    {
        return (tags.length > CHANNEL_TAG_INDEX) ? tags[CHANNEL_TAG_INDEX] : null;
    }

    /**
     * Get the entity tag.
     *
     * @return entity tag.
     */
    public String entityTag()
    {
        return (tags.length > ENTITY_TAG_INDEX) ? tags[ENTITY_TAG_INDEX] : null;
    }

    /**
     * Generate a String representation of the URI that is valid for an Aeron channel.
     *
     * @return a String representation of the URI that is valid for an Aeron channel.
     */
    public String toString()
    {
        final StringBuilder sb;
        if (prefix == null || "".equals(prefix))
        {
            sb = new StringBuilder((params.size() * 20) + 10);

        }
        else
        {
            sb = new StringBuilder((params.size() * 20) + 20);
            sb.append(prefix);
            if (!prefix.endsWith(":"))
            {
                sb.append(':');
            }
        }

        sb.append(AERON_PREFIX).append(media);

        if (params.size() > 0)
        {
            sb.append('?');

            for (final Map.Entry<String, String> entry : params.entrySet())
            {
                sb.append(entry.getKey()).append('=').append(entry.getValue()).append('|');
            }

            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    /**
     * Initialise a channel for restarting a publication at a given position.
     *
     * @param position      at which the publication should be started.
     * @param initialTermId what which the stream would start.
     * @param termLength    for the stream.
     */
    public void initialPosition(final long position, final int initialTermId, final int termLength)
    {
        final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        final int termId = LogBufferDescriptor.computeTermIdFromPosition(position, bitsToShift, initialTermId);
        final int termOffset = (int)(position & (termLength - 1));

        put(INITIAL_TERM_ID_PARAM_NAME, Integer.toString(initialTermId));
        put(TERM_ID_PARAM_NAME, Integer.toString(termId));
        put(TERM_OFFSET_PARAM_NAME, Integer.toString(termOffset));
        put(TERM_LENGTH_PARAM_NAME, Integer.toString(termLength));
    }

    /**
     * Parse a {@link CharSequence} which contains an Aeron URI.
     *
     * @param cs to be parsed.
     * @return a new {@link ChannelUri} representing the URI string.
     */
    public static ChannelUri parse(final CharSequence cs)
    {
        int position = 0;
        final String prefix;
        if (startsWith(cs, SPY_PREFIX))
        {
            prefix = SPY_QUALIFIER;
            position = SPY_PREFIX.length();
        }
        else
        {
            prefix = "";
        }

        if (!startsWith(cs, position, AERON_PREFIX))
        {
            throw new IllegalArgumentException("Aeron URIs must start with 'aeron:', found: '" + cs + "'");
        }
        else
        {
            position += AERON_PREFIX.length();
        }

        final StringBuilder builder = new StringBuilder();
        final Map<String, String> params = new HashMap<>();
        String media = null;
        String key = null;

        State state = State.MEDIA;
        for (int i = position; i < cs.length(); i++)
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
                    throw new IllegalStateException("Que? state=" + state);
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

        return new ChannelUri(prefix, media, params);
    }

    /**
     * Add a sessionId to a given channel.
     *
     * @param channel   to add sessionId to.
     * @param sessionId to add to channel.
     * @return new string that represents channel with sessionId added.
     */
    public static String addSessionId(final String channel, final int sessionId)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));

        return channelUri.toString();
    }

    /**
     * Is the param tagged? (starts with the "tag:" prefix)
     *
     * @param paramValue to check if tagged.
     * @return true if tagged or false if not.
     */
    public static boolean isTagged(final String paramValue)
    {
        return startsWith(paramValue, "tag:");
    }

    /**
     * Get the value of the tag from a given parameter.
     *
     * @param paramValue to extract the tag value from.
     * @return the value of the tag or {@link #INVALID_TAG} if not tagged.
     */
    public static long getTag(final String paramValue)
    {
        return isTagged(paramValue) ?
            AsciiEncoding.parseLongAscii(paramValue, 4, paramValue.length() - 4) : INVALID_TAG;
    }

    private static boolean startsWith(final CharSequence input, final int position, final CharSequence prefix)
    {
        if ((input.length() - position) < prefix.length())
        {
            return false;
        }

        for (int i = 0; i < prefix.length(); i++)
        {
            if (input.charAt(position + i) != prefix.charAt(i))
            {
                return false;
            }
        }

        return true;
    }

    private static boolean startsWith(final CharSequence input, final CharSequence prefix)
    {
        return startsWith(input, 0, prefix);
    }

    private static String[] splitTags(final CharSequence tags)
    {
        String[] stringArray = ArrayUtil.EMPTY_STRING_ARRAY;

        if (null != tags)
        {
            int currentStartIndex = 0;
            int tagIndex = 0;
            stringArray = new String[2];
            final int length = tags.length();

            for (int i = 0; i < length; i++)
            {
                if (tags.charAt(i) == ',')
                {
                    String tag = null;

                    if ((i - currentStartIndex) > 0)
                    {
                        tag = tags.subSequence(currentStartIndex, i).toString();
                        currentStartIndex = i + 1;
                    }

                    stringArray = ArrayUtil.ensureCapacity(stringArray, tagIndex + 1);
                    stringArray[tagIndex] = tag;
                    tagIndex++;
                }
            }

            if ((length - currentStartIndex) > 0)
            {
                stringArray = ArrayUtil.ensureCapacity(stringArray, tagIndex + 1);
                stringArray[tagIndex] = tags.subSequence(currentStartIndex, length).toString();
            }
        }

        return stringArray;
    }
}
