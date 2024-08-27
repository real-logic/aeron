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
#ifndef AERON_ARCHIVE_VERSION_H
#define AERON_ARCHIVE_VERSION_H

#include "string"

#ifdef major
#undef major
#endif
#ifdef minor
#undef minor
#endif

namespace aeron { namespace archive { namespace client
{

class AeronArchiveVersion
{
public:
    static std::string text();
    static std::string gitSha();
    static int major();
    static int minor();
    static int patch();
};

}}}

#endif //AERON_ARCHIVE_VERSION_H
