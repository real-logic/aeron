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

#if defined(__linux__)
#define _BSD_SOURCE
#endif

#include <stdlib.h>

#include "util/aeron_env.h"

int aeron_env_set(const char *key, const char *val)
{
#if defined(WIN32)
    return _putenv_s(key, val);
#else
    return setenv(key, val, 1);
#endif
}

int aeron_env_unset(const char *key)
{
#if defined(WIN32)
    return _putenv_s(key, "");
#else
    return unsetenv(key);
#endif
}
