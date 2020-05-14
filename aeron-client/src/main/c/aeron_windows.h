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

#ifndef AERON_WINDOWS_H
#define AERON_WINDOWS_H

#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_GCC)

#define aeron_erand48 erand48
#define aeron_srand48 srand48
#define aeron_drand48 drand48
#define aeron_strndup strndup

#elif defined(AERON_COMPILER_MSVC)

#include <basetsd.h>
#include <stddef.h>

double aeron_erand48(unsigned short xsubi[3]);
void aeron_srand48(UINT64 aeron_nano_clock);
double aeron_drand48();
int aeron_clock_gettime_monotonic(struct timespec *tp);
int aeron_clock_gettime_realtime(struct timespec *tp);
char *aeron_strndup(const char *value, size_t length);
void localtime_r(const time_t *timep, struct tm *result);

#endif

#endif //AERON_WINDOWS_H
