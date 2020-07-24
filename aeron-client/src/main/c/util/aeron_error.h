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

#ifndef AERON_ERROR_H
#define AERON_ERROR_H

#include "aeron_common.h"

typedef struct aeron_per_thread_error_stct
{
    int errcode;
    char errmsg[AERON_MAX_PATH];
}
aeron_per_thread_error_t;

int aeron_errcode();
const char *aeron_errmsg();
void aeron_set_err(int errcode, const char *format, ...);
void aeron_set_errno(int errcode);
void aeron_set_err_from_last_err_code(const char* format, ...);

const char *aeron_error_code_str(int errcode);

#if defined(AERON_COMPILER_MSVC)
bool aeron_error_dll_process_attach();
void aeron_error_dll_thread_detach();
void aeron_error_dll_process_detach();
#endif

#endif //AERON_ERROR_H
