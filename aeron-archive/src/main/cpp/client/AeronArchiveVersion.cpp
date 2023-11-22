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
#include "AeronArchiveVersion.h"

const std::string aeron::archive::client::AeronArchiveVersion::VERSION = AERON_VERSION_TXT;
const std::string aeron::archive::client::AeronArchiveVersion::GIT_SHA = AERON_VERSION_GITSHA;
const int aeron::archive::client::AeronArchiveVersion::MAJOR_VERSION = AERON_VERSION_MAJOR;
const int aeron::archive::client::AeronArchiveVersion::MINOR_VERSION = AERON_VERSION_MINOR;
const int aeron::archive::client::AeronArchiveVersion::PATCH_VERSION = AERON_VERSION_PATCH;
