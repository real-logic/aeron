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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <string.h>
#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_context.h"

#ifdef _MSC_VER
#define strdup _strdup
#endif

#define AERON_NAME_RESOLVER_CSV_TABLE_ARGS_ENV_VAR "AERON_NAME_RESOLVER_CSV_LOOKUP_TABLE_ARGS"

#define AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS (4)

typedef struct aeron_name_resolver_csv_table_stct
{
    const char *content[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE][AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    size_t length;
}
aeron_name_resolver_csv_table_t;

int aeron_name_resolver_lookup_csv_table(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    const char **resolved_name)
{
    if (NULL == resolver->state)
    {
        return -1;
    }

    aeron_name_resolver_csv_table_t *table = (aeron_name_resolver_csv_table_t *)resolver->state;

    for (size_t i = 0; i < table->length; i++)
    {
        if (strcmp(name, table->content[i][0]) == 0 && strcmp(uri_param_name, table->content[i][1]) == 0)
        {
            int address_idx = is_re_resolution ? 3 : 2;
            *resolved_name = table->content[i][address_idx];
            return 1;
        }
    }

    return aeron_name_resolver_lookup_default(resolver, name, uri_param_name, is_re_resolution, resolved_name);
}

int aeron_name_resolver_supplier_csv_table(aeron_driver_context_t *context, aeron_name_resolver_t *resolver)
{
    resolver->resolve_func = aeron_name_resolver_resolve_default;
    resolver->lookup_func = aeron_name_resolver_lookup_csv_table;

    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    
    const char *config_str = getenv(AERON_NAME_RESOLVER_CSV_TABLE_ARGS_ENV_VAR);
    if (NULL == config_str)
    {
        aeron_set_err(errno, "%s", "No configuration specified for name_resolver_csv_lookup_table");
        return -1;
    }

    char *config_str_dup = strdup(config_str);
    if (NULL == config_str_dup)
    {
        aeron_set_err_from_last_err_code("Duplicating config string - %s:%d", __FILE__, __LINE__);
        return -1;
    }

    aeron_name_resolver_csv_table_t *lookup_table;
    const size_t lookup_table_size = AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE *
        AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS * sizeof(char *);
    if (aeron_alloc((void**) &lookup_table, lookup_table_size) < 0)
    {
        aeron_set_err_from_last_err_code("Allocating lookup table - %s:%d", __FILE__, __LINE__);
        aeron_free(config_str_dup);
        return -1;
    }

    int num_rows = aeron_tokenise(config_str_dup, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        aeron_set_err(num_rows, "%s", "Failed to parse rows for lookup table");
        return -1;
    }

    for (int i = num_rows; -1 < --i;)
    {
        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS == num_columns)
        {
            for (int k = num_columns, l = 0; -1 < --k; l++)
            {
                lookup_table->content[lookup_table->length][l] = columns[k];
            }
            lookup_table->length++;
        }
    }

    resolver->state = lookup_table;
    return 0;
}
