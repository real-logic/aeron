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

#ifndef AERON_ERROR_CLIENT_H
#define AERON_ERROR_CLIENT_H

#include "util/Platform.h"

#if defined(AERON_COMPILER_MSVC)
#include <WinSock2.h>
#include <windows.h>

typedef DWORD pthread_key_t;
typedef struct aeron_per_thread_error_stct
{
    int errcode;
    char* errmsg;
} aeron_per_thread_error_t;

BOOL aeron_error_dll_process_attach();
void aeron_error_dll_thread_detach();
void aeron_error_dll_process_detach();
int aeron_thread_key_delete(pthread_key_t key);
int aeron_thread_set_specific(pthread_key_t key, const void *pointer);

#endif

#endif //AERON_ERROR_CLIENT_H
