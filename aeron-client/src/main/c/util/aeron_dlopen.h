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

#ifndef AERON_DLOPEN_H
#define AERON_DLOPEN_H

#include <util/aeron_platform.h>

#if defined(AERON_COMPILER_GCC)

#include <dlfcn.h>
#include <stddef.h>

#define aeron_dlsym dlsym
#define aeron_dlopen(x) dlopen(x, RTLD_LAZY | RTLD_GLOBAL)
#define aeron_dlerror dlerror

const char *aeron_dlinfo(const void *, char *buffer, size_t max_buffer_length);
const char *aeron_dlinfo_func(void (*func)(void), char *buffer, size_t max_buffer_length);

#elif defined(AERON_COMPILER_MSVC)

#define RTLD_DEFAULT ((void *)-123)
#define RTLD_NEXT ((void *)-124)

void *aeron_dlsym(void *module, const char *name);
void *aeron_dlopen(const char *filename);
char *aeron_dlerror();
const char *aeron_dlinfo(const void *addr, char *buffer, size_t max_buffer_length);
const char *aeron_dlinfo_func(void (*func)(void), char *buffer, size_t max_buffer_length);

#else
#error Unsupported platform!
#endif

typedef struct aeron_dl_loaded_lib_state_stct
{
    void *handle;
}
aeron_dl_loaded_lib_state_t;

typedef struct aeron_dl_loaded_libs_state_stct
{
    aeron_dl_loaded_lib_state_t *libs;
    size_t num_libs;
}
aeron_dl_loaded_libs_state_t;

int aeron_dl_load_libs(aeron_dl_loaded_libs_state_t **state, const char *libs);
int aeron_dl_load_libs_delete(aeron_dl_loaded_libs_state_t *state);

#endif //AERON_DLOPEN_H
