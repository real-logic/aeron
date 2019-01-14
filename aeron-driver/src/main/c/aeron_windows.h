/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#ifndef AERON_WINDOWS_H
#define AERON_WINDOWS_H

#include "util/aeron_platform.h"

#if define AERON_COMPILER_GCC

#define aeron_micro_sleep usleep

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)

double erand48(unsigned short xsubi[3]);

typedef enum
{
    CLOCK_MONOTONIC_RAW,
    CLOCK_REALTIME
} clockid_t;

int clock_gettime(clockid_t type, struct timespec *tp);
#endif

#endif //AERON_WINDOWS_H
