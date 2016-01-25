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
package uk.co.real_logic.aeron.samples;

import java.util.concurrent.TimeUnit;

/**
 * Configuration used for samples all in one place.
 */
public class SampleConfiguration
{
    public static final String CHANNEL_PROP = "aeron.sample.channel";
    public static final String STREAM_ID_PROP = "aeron.sample.streamId";

    public static final String PING_CHANNEL_PROP = "aeron.sample.ping.channel";
    public static final String PONG_CHANNEL_PROP = "aeron.sample.pong.channel";
    public static final String PING_STREAM_ID_PROP = "aeron.sample.ping.streamId";
    public static final String PONG_STREAM_ID_PROP = "aeron.sample.pong.streamId";
    public static final String WARMUP_NUMBER_OF_MESSAGES_PROP = "aeron.sample.warmup.messages";
    public static final String WARMUP_NUMBER_OF_ITERATIONS_PROP = "aeron.sample.warmup.iterations";
    public static final String RANDOM_MESSAGE_LENGTH_PROP = "aeron.sample.randomMessageLength";

    public static final String FRAME_COUNT_LIMIT_PROP = "aeron.sample.frameCountLimit";
    public static final String MESSAGE_LENGTH_PROP = "aeron.sample.messageLength";
    public static final String NUMBER_OF_MESSAGES_PROP = "aeron.sample.messages";
    public static final String LINGER_TIMEOUT_MS_PROP = "aeron.sample.lingerTimeout";
    public static final String EMBEDDED_MEDIA_DRIVER_PROP = "aeron.sample.embeddedMediaDriver";

    public static final String CHANNEL;
    public static final String PING_CHANNEL;
    public static final String PONG_CHANNEL;
    public static final int STREAM_ID;
    public static final int PING_STREAM_ID;
    public static final int PONG_STREAM_ID;
    public static final int FRAGMENT_COUNT_LIMIT;
    public static final int MESSAGE_LENGTH;
    public static final int NUMBER_OF_MESSAGES;
    public static final int WARMUP_NUMBER_OF_MESSAGES;
    public static final int WARMUP_NUMBER_OF_ITERATIONS;
    public static final long LINGER_TIMEOUT_MS;
    public static final boolean EMBEDDED_MEDIA_DRIVER;
    public static final boolean RANDOM_MESSAGE_LENGTH;

    static
    {
        CHANNEL = System.getProperty(CHANNEL_PROP, "aeron:udp?remote=localhost:40123");
        STREAM_ID = Integer.getInteger(STREAM_ID_PROP, 10);
        PING_CHANNEL = System.getProperty(PING_CHANNEL_PROP, "aeron:udp?remote=localhost:40123");
        PONG_CHANNEL = System.getProperty(PONG_CHANNEL_PROP, "aeron:udp?remote=localhost:40124");
        PING_STREAM_ID = Integer.getInteger(PING_STREAM_ID_PROP, 10);
        PONG_STREAM_ID = Integer.getInteger(PONG_STREAM_ID_PROP, 10);
        FRAGMENT_COUNT_LIMIT = Integer.getInteger(FRAME_COUNT_LIMIT_PROP, 256);
        MESSAGE_LENGTH = Integer.getInteger(MESSAGE_LENGTH_PROP, 256);
        RANDOM_MESSAGE_LENGTH = Boolean.getBoolean(RANDOM_MESSAGE_LENGTH_PROP);
        NUMBER_OF_MESSAGES = Integer.getInteger(NUMBER_OF_MESSAGES_PROP, 1_000_000);
        WARMUP_NUMBER_OF_MESSAGES = Integer.getInteger(WARMUP_NUMBER_OF_MESSAGES_PROP, 10_000);
        WARMUP_NUMBER_OF_ITERATIONS = Integer.getInteger(WARMUP_NUMBER_OF_ITERATIONS_PROP, 5);
        LINGER_TIMEOUT_MS = Long.getLong(LINGER_TIMEOUT_MS_PROP, TimeUnit.SECONDS.toMillis(5));
        EMBEDDED_MEDIA_DRIVER = Boolean.getBoolean(EMBEDDED_MEDIA_DRIVER_PROP);
    }
}
