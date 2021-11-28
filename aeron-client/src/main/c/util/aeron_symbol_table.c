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
#include "aeron_symbol_table.h"


static void* aeron_symbol_table_obj_scan(const aeron_symbol_table_obj_t *table, const char *symbol)
{
    void* result = NULL;

    int i = 0;
    do
    {
        const char *alias = table[i].alias;
        const char *name = table[i].name;

        if (NULL == alias || NULL == name)
        {
            break;
        }

        if (0 == strcmp(alias, symbol) || 0 == strcmp(name, symbol))
        {
            result = table[i].object;
            break;
        }

        i++;
    }
    while (true);

    return result;
}

void* aeron_symbol_table_obj_load(const aeron_symbol_table_obj_t *table, const char *name, const char *component_name)
{
    void *obj = aeron_symbol_table_obj_scan(table, name);
    if (NULL == obj)
    {
        char copied_name[AERON_MAX_PATH] = { 0 };

        snprintf(copied_name, sizeof(copied_name) - 1, "%s", name);
        obj = aeron_dlsym(RTLD_DEFAULT, copied_name);
    }

    if (NULL == obj)
    {
        AERON_SET_ERR(EINVAL, "could not find %s object %s: dlsym - %s", component_name, name, aeron_dlerror());
        return NULL;
    }

    return obj;
}


static aeron_symbol_table_fptr_t aeron_symbol_table_func_scan(const aeron_symbol_table_func_t *table, const char *symbol)
{
    aeron_symbol_table_fptr_t result = NULL;

    int i = 0;
    do
    {
        const char *alias = table[i].alias;
        const char *name = table[i].name;

        if (NULL == alias || NULL == name)
        {
            break;
        }

        if (0 == strcmp(alias, symbol) || 0 == strcmp(name, symbol))
        {
            result = table[i].function;
            break;
        }

        i++;
    }
    while (true);

    return result;
}

aeron_symbol_table_fptr_t aeron_symbol_table_func_load(
    const aeron_symbol_table_func_t *table,
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

    aeron_symbol_table_fptr_t func = aeron_symbol_table_func_scan(table, name);

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
