/*
 * Copyright 2014-2025 Real Logic Limited.
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
#include "AeronArchiveVersion.h"

namespace aeron { namespace archive { namespace client
{

std::string AeronArchiveVersion::text()
{
    return AERON_VERSION_TXT;
}

std::string AeronArchiveVersion::gitSha()
{
    return AERON_VERSION_GITSHA;
}

int AeronArchiveVersion::major()
{
    return AERON_VERSION_MAJOR;
}

int AeronArchiveVersion::minor()
{
    return AERON_VERSION_MINOR;
}

int AeronArchiveVersion::patch()
{
    return AERON_VERSION_PATCH;
}

}}}