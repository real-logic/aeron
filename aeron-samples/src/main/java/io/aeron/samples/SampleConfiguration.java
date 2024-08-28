/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.driver.Configuration;
import org.agrona.SystemUtil;
import org.agrona.concurrent.IdleStrategy;

/**
 * Configuration used for samples with defaults which can be overridden by system properties.
 */
@SuppressWarnings("JavadocVariable")
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
    public static final String EXCLUSIVE_PUBLICATIONS_PROP = "aeron.sample.exclusive.publications";
    public static final String IDLE_STRATEGY_PROP = "aeron.sample.idleStrategy";

    public static final String CHANNEL;
    public static final String PING_CHANNEL;
    public static final String PONG_CHANNEL;
    public static final String IDLE_STRATEGY_NAME;

    public static final boolean EMBEDDED_MEDIA_DRIVER;
    public static final boolean RANDOM_MESSAGE_LENGTH;
    public static final int STREAM_ID;
    public static final int PING_STREAM_ID;
    public static final int PONG_STREAM_ID;
    public static final int FRAGMENT_COUNT_LIMIT;
    public static final int MESSAGE_LENGTH;
    public static final int WARMUP_NUMBER_OF_ITERATIONS;
    public static final long WARMUP_NUMBER_OF_MESSAGES;
    public static final long NUMBER_OF_MESSAGES;
    public static final long LINGER_TIMEOUT_MS;
    public static final boolean EXCLUSIVE_PUBLICATIONS;

    static
    {
        CHANNEL = System.getProperty(CHANNEL_PROP, "aeron:udp?endpoint=localhost:20121");
        STREAM_ID = Integer.getInteger(STREAM_ID_PROP, 1001);
        PING_CHANNEL = System.getProperty(PING_CHANNEL_PROP, "aeron:udp?endpoint=localhost:20123");
        PONG_CHANNEL = System.getProperty(PONG_CHANNEL_PROP, "aeron:udp?endpoint=localhost:20124");
        IDLE_STRATEGY_NAME = System.getProperty(IDLE_STRATEGY_PROP, "org.agrona.concurrent.BusySpinIdleStrategy");
        LINGER_TIMEOUT_MS = Long.getLong(LINGER_TIMEOUT_MS_PROP, 0);
        PING_STREAM_ID = Integer.getInteger(PING_STREAM_ID_PROP, 1002);
        PONG_STREAM_ID = Integer.getInteger(PONG_STREAM_ID_PROP, 1003);
        FRAGMENT_COUNT_LIMIT = Integer.getInteger(FRAME_COUNT_LIMIT_PROP, 10);
        MESSAGE_LENGTH = SystemUtil.getSizeAsInt(MESSAGE_LENGTH_PROP, 32);
        RANDOM_MESSAGE_LENGTH = "true".equals(System.getProperty(RANDOM_MESSAGE_LENGTH_PROP));
        NUMBER_OF_MESSAGES = Long.getLong(NUMBER_OF_MESSAGES_PROP, 10_000_000);
        WARMUP_NUMBER_OF_MESSAGES = Long.getLong(WARMUP_NUMBER_OF_MESSAGES_PROP, 10_000);
        WARMUP_NUMBER_OF_ITERATIONS = Integer.getInteger(WARMUP_NUMBER_OF_ITERATIONS_PROP, 10);
        EMBEDDED_MEDIA_DRIVER = "true".equals(System.getProperty(EMBEDDED_MEDIA_DRIVER_PROP));
        EXCLUSIVE_PUBLICATIONS = "true".equals(System.getProperty(EXCLUSIVE_PUBLICATIONS_PROP));
    }

    /**
     * Create a new {@link IdleStrategy} based on the {@link #IDLE_STRATEGY_NAME}.
     *
     * @return a new {@link IdleStrategy} based on the {@link #IDLE_STRATEGY_NAME}.
     */
    public static IdleStrategy newIdleStrategy()
    {
        return Configuration.agentIdleStrategy(IDLE_STRATEGY_NAME, null);
    }
}
