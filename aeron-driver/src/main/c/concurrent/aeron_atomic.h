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

#ifndef AERON_ATOMIC_H
#define AERON_ATOMIC_H

#include <util/aeron_platform.h>

#include <stdint.h>

#if defined(AERON_COMPILER_GCC)
    #if defined(AERON_CPU_X64)
        #include <concurrent/aeron_atomic64_gcc_x86_64.h>
    #else
        #include <concurrent/aeron_atomic64_gcc_c11.h>
    #endif
#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
    #include <concurrent/aeron_atomic64_msvc.h>
#else
    #error Unsupported platform!
#endif

#endif //AERON_ATOMIC_H
