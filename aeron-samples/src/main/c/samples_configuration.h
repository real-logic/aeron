/*
 * Copyright 2014-2023 Real Logic Limited.
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

#ifndef AERON_SAMPLES_CONFIGURATION_H
#define AERON_SAMPLES_CONFIGURATION_H

#define DEFAULT_CHANNEL "aeron:udp?endpoint=localhost:20121"
#define DEFAULT_PING_CHANNEL "aeron:udp?endpoint=localhost:20123"
#define DEFAULT_PONG_CHANNEL "aeron:udp?endpoint=localhost:20124"
#define DEFAULT_STREAM_ID (1001)
#define DEFAULT_PING_STREAM_ID (1002)
#define DEFAULT_PONG_STREAM_ID (1003)
#define DEFAULT_NUMBER_OF_WARM_UP_MESSAGES (100000)
#define DEFAULT_NUMBER_OF_MESSAGES (10000000)
#define DEFAULT_MESSAGE_LENGTH (32)
#define DEFAULT_LINGER_TIMEOUT_MS (0)
#define DEFAULT_FRAGMENT_COUNT_LIMIT (10)
#define DEFAULT_RANDOM_MESSAGE_LENGTH (false)
#define DEFAULT_PUBLICATION_RATE_PROGRESS (false)

#endif //AERON_SAMPLES_CONFIGURATION_H
