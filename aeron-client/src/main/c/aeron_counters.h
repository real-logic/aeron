/*
 * Copyright 2014-2020 Real Logic Limited.
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

#ifndef AERON_C_COUNTERS_H
#define AERON_C_COUNTERS_H

#define AERON_COUNTER_SYSTEM_COUNTER_TYPE_ID (0)

#define AERON_COUNTER_PUBLISHER_LIMIT_NAME "pub-lmt"
#define AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID (1)

#define AERON_COUNTER_SENDER_POSITION_NAME "snd-pos"
#define AERON_COUNTER_SENDER_POSITION_TYPE_ID (2)

#define AERON_COUNTER_RECEIVER_HWM_NAME "rcv-hwm"
#define AERON_COUNTER_RECEIVER_HWM_TYPE_ID (3)

#define AERON_COUNTER_SUBSCRIPTION_POSITION_NAME "sub-pos"
#define AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID (4)

#define AERON_COUNTER_RECEIVER_POSITION_NAME "rcv-pos"
#define AERON_COUNTER_RECEIVER_POSITION_TYPE_ID (5)

#define AERON_COUNTER_SEND_CHANNEL_STATUS_NAME "snd-channel"
#define AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID (6)

#define AERON_COUNTER_RECEIVE_CHANNEL_STATUS_NAME "rcv-channel"
#define AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID (7)

#define AERON_COUNTER_SENDER_LIMIT_NAME "snd-lmt"
#define AERON_COUNTER_SENDER_LIMIT_TYPE_ID (9)

#define AERON_COUNTER_PER_IMAGE_TYPE_ID (10)

#define AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_NAME "client-heartbeat"
#define AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID (11)

#define AERON_COUNTER_PUBLISHER_POSITION_NAME "pub-pos (sampled)"
#define AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID (12)

#define AERON_COUNTER_SENDER_BPE_NAME "snd-bpe"
#define AERON_COUNTER_SENDER_BPE_TYPE_ID  (13)

#define AERON_COUNTER_RCV_LOCAL_SOCKADDR_NAME "rcv-local-sockaddr"
#define AERON_COUNTER_SND_LOCAL_SOCKADDR_NAME "snd-local-sockaddr"
#define AERON_COUNTER_LOCAL_SOCKADDR_TYPE_ID (14)

#endif //AERON_C_COUNTERS_H
