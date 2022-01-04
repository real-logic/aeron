/*
 * Copyright 2014-2021 Real Logic Limited.
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

#ifndef AERON_SYMBOL_TABLE_H
#define AERON_SYMBOL_TABLE_H

#include "aeron_common.h"

#define AERON_SYMBOL_TABLE_NAME_MAX_LENGTH (1023)

struct aeron_symbol_table_obj_stct
{
    const char *alias;
    const char *name;
    void *object;
};
typedef struct aeron_symbol_table_obj_stct aeron_symbol_table_obj_t;

void* aeron_symbol_table_obj_load(
    const aeron_symbol_table_obj_t *table, size_t table_length, const char *name, const char *component_name);

struct aeron_symbol_table_func_stct
{
    const char *alias;
    const char *name;
    aeron_fptr_t function;
};
typedef struct aeron_symbol_table_func_stct aeron_symbol_table_func_t;

aeron_fptr_t aeron_symbol_table_func_load(
    const aeron_symbol_table_func_t *table,
    size_t table_length,
    const char *name,
    const char *component_name);

#endif // AERON_SYMBOL_TABLE_H
