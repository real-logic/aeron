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

#ifndef INCLUDED_AERON_SAMPLES_CONFIGURATION__
#define INCLUDED_AERON_SAMPLES_CONFIGURATION__

#include <string>

namespace aeron { namespace samples {

namespace configuration {

    const static std::string DEFAULT_CHANNEL = "udp://localhost:40123";
    const static std::int32_t DEFAULT_STREAM_ID = 10;
    const static int DEFAULT_NUMBER_OF_MESSAGES = 1000000;
    const static int DEFAULT_LINGER_TIMEOUT_MS = 5000;

}

}}

#endif