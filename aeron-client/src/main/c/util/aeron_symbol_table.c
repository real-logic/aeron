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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>

#include "util/aeron_dlopen.h"
#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "aeron_symbol_table.h"


static void* aeron_symbol_table_obj_scan(const aeron_symbol_table_obj_t *table, size_t table_size, const char *symbol)
{
    void* result = NULL;

    for (size_t i = 0; i < table_size; i++)
    {
        const char *alias = table[(int)i].alias;
        const char *name = table[(int)i].name;

        if (NULL == alias || NULL == name)
        {
            break;
        }

        if (0 == strncmp(alias, symbol, AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1) ||
            0 == strncmp(name, symbol, AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1))
        {
            result = table[i].object;
            break;
        }
    }

    return result;
}

void* aeron_symbol_table_obj_load(
    const aeron_symbol_table_obj_t *table, size_t table_length, const char *name, const char *component_name)
{
    if (NULL == name)
    {
        AERON_SET_ERR(EINVAL, "%s", "name must not be null");
        return NULL;
    }

    if (NULL == component_name)
    {
        AERON_SET_ERR(EINVAL, "%s", "component_name must not be null");
        return NULL;
    }

    if (!aeron_str_length(name, AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1, NULL))
    {
        AERON_SET_ERR(EINVAL, "name must not exceed %d characters", AERON_SYMBOL_TABLE_NAME_MAX_LENGTH);
        return NULL;
    }

    void *obj = aeron_symbol_table_obj_scan(table, table_length, name);
    if (NULL == obj)
    {
        char copied_name[AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1] = { 0 };
        snprintf(copied_name, sizeof(copied_name), "%s", name);
        obj = aeron_dlsym(RTLD_DEFAULT, copied_name);
    }

    if (NULL == obj)
    {
        AERON_SET_ERR(EINVAL, "could not find %s object %s: dlsym - %s", component_name, name, aeron_dlerror());
        return NULL;
    }

    return obj;
}


static aeron_fptr_t aeron_symbol_table_func_scan(
    const aeron_symbol_table_func_t *table, size_t table_length, const char *symbol)
{
    aeron_fptr_t result = NULL;

    for (size_t i = 0; i < table_length; i++)
    {
        const char *alias = table[(int)i].alias;
        const char *name = table[(int)i].name;

        if (NULL == alias || NULL == name)
        {
            break;
        }

        if (0 == strncmp(alias, symbol, AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1) ||
            0 == strncmp(name, symbol, AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1))
        {
            result = table[i].function;
            break;
        }
    }

    return result;
}

aeron_fptr_t aeron_symbol_table_func_load(
    const aeron_symbol_table_func_t *table,
    size_t table_length,
    const char *name,
    const char *component_name)
{
    if (NULL == name)
    {
        AERON_SET_ERR(EINVAL, "%s", "name must not be null");
        return NULL;
    }

    if (NULL == component_name)
    {
        AERON_SET_ERR(EINVAL, "%s", "component_name must not be null");
        return NULL;
    }

    if (!aeron_str_length(name, AERON_SYMBOL_TABLE_NAME_MAX_LENGTH + 1, NULL))
    {
        AERON_SET_ERR(EINVAL, "name must not exceed %d characters", AERON_SYMBOL_TABLE_NAME_MAX_LENGTH);
        return NULL;
    }

    aeron_fptr_t func = aeron_symbol_table_func_scan(table, table_length, name);

    if (NULL == func)
    {
        *(void **)(&func) = aeron_dlsym(RTLD_DEFAULT, name);
    }

    if (NULL == func)
    {
        AERON_SET_ERR(EINVAL, "could not find %s %s: dlsym - %s", component_name, name, aeron_dlerror());
    }

    return func;
}
