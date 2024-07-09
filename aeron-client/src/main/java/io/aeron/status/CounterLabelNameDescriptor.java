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
package io.aeron.status;

import io.aeron.AeronCounters;

/**
 * Human-readable names for the Metadata Label of Stream Counters
 */
public class CounterLabelNameDescriptor
{
    public static final String CLIENT_HEARTBEAT_TIMESTAMP = "client-heartbeat";
    public static final String FLOW_CONTROL_RECEIVERS = "fc-receivers";
    public static final String MDC_DESTINATIONS = "mdc-num-dest";
    public static final String PUBLISHER_LIMIT = "pub-lmt";
    public static final String PUBLISHER_POS = "pub-pos (sampled)";
    public static final String RECEIVE_CHANNEL_STATUS = "rcv-channel";
    public static final String RECEIVE_LOCAL_SOCKET_ADDRESS = "rcv-local-sockaddr";
    public static final String RECEIVER_HWM = "rcv-hwm";
    public static final String RECEIVER_POS = "rcv-pos";
    public static final String SEND_CHANNEL_STATUS = "snd-channel";
    public static final String SENDER_BPE = "snd-bpe";
    public static final String SENDER_LIMIT = "snd-lmt";
    public static final String SENDER_POS = "snd-pos";
    public static final String SEND_LOCAL_SOCKET_ADDRESS = "snd-local-sockaddr";
    public static final String SUBSCRIBER_POS = "sub-pos";

    /**
     * Return the label name for a counter type identifier.
     *
     * @param typeId of the counter.
     * @return the label name as a String.
     */
    public static String labelName(final int typeId)
    {
        switch (typeId)
        {
            case AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID:
                return CounterLabelNameDescriptor.PUBLISHER_LIMIT;

            case AeronCounters.DRIVER_SENDER_POSITION_TYPE_ID:
                return CounterLabelNameDescriptor.SENDER_POS;

            case AeronCounters.DRIVER_RECEIVER_HWM_TYPE_ID:
                return CounterLabelNameDescriptor.RECEIVER_HWM;

            case AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID:
                return CounterLabelNameDescriptor.SUBSCRIBER_POS;

            case AeronCounters.DRIVER_RECEIVER_POS_TYPE_ID:
                return CounterLabelNameDescriptor.RECEIVER_POS;

            case AeronCounters.DRIVER_SENDER_LIMIT_TYPE_ID:
                return CounterLabelNameDescriptor.SENDER_LIMIT;

            case AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID:
                return CounterLabelNameDescriptor.PUBLISHER_POS;

            case AeronCounters.DRIVER_SENDER_BPE_TYPE_ID:
                return CounterLabelNameDescriptor.SENDER_BPE;

            default:
                return "<unknown>";
        }
    }
}
