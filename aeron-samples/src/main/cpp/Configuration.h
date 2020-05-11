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

#ifndef INCLUDED_AERON_SAMPLES_CONFIGURATION__
#define INCLUDED_AERON_SAMPLES_CONFIGURATION__

#include <string>

namespace aeron { namespace samples { namespace configuration {

const static std::string DEFAULT_CHANNEL = "aeron:udp?endpoint=localhost:20122";
const static std::string DEFAULT_PING_CHANNEL = "aeron:udp?endpoint=localhost:20123";
const static std::string DEFAULT_PONG_CHANNEL = "aeron:udp?endpoint=localhost:20124";
const static std::int32_t DEFAULT_STREAM_ID = 1001;
const static std::int32_t DEFAULT_PING_STREAM_ID = 1002;
const static std::int32_t DEFAULT_PONG_STREAM_ID = 1003;
const static long DEFAULT_NUMBER_OF_WARM_UP_MESSAGES = 100000;
const static long DEFAULT_NUMBER_OF_MESSAGES = 1000000;
const static int DEFAULT_MESSAGE_LENGTH = 32;
const static int DEFAULT_LINGER_TIMEOUT_MS = 0;
const static int DEFAULT_FRAGMENT_COUNT_LIMIT = 10;
const static bool DEFAULT_RANDOM_MESSAGE_LENGTH = false;
const static bool DEFAULT_PUBLICATION_RATE_PROGRESS = false;

}}}

#endif
