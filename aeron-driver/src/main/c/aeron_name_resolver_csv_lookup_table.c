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
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_context.h"

#ifdef _MSC_VER
#define strdup _strdup
#endif

#define AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS (4)

typedef struct aeron_name_resolver_csv_table_row_stct
{
    const char* row[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
}
aeron_name_resolver_csv_table_row_t;

typedef struct aeron_name_resolver_csv_table_stct
{
    aeron_name_resolver_csv_table_row_t *array;
    size_t length;
    size_t capacity;
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
        if (strcmp(name, table->array[i].row[0]) == 0 && strcmp(uri_param_name, table->array[i].row[1]) == 0)
        {
            int address_idx = is_re_resolution ? 3 : 2;
            *resolved_name = table->array[i].row[address_idx];
            return 1;
        }
    }

    return aeron_name_resolver_lookup_default(resolver, name, uri_param_name, is_re_resolution, resolved_name);
}

int aeron_name_resolver_supplier_csv_table(
    aeron_driver_context_t *context,
    aeron_name_resolver_t *resolver,
    const char *args)
{
    resolver->resolve_func = aeron_name_resolver_resolve_default;
    resolver->lookup_func = aeron_name_resolver_lookup_csv_table;

    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    
    char *config_csv = strdup(args);
    if (NULL == config_csv)
    {
        aeron_set_err_from_last_err_code("Duplicating config string - %s:%d", __FILE__, __LINE__);
        return -1;
    }

    aeron_name_resolver_csv_table_t *lookup_table;
    if (aeron_alloc((void**) &lookup_table, sizeof(lookup_table)) < 0)
    {
        aeron_set_err_from_last_err_code("Allocating lookup table - %s:%d", __FILE__, __LINE__);
        aeron_free(config_csv);
        return -1;
    }

    int num_rows = aeron_tokenise(config_csv, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        aeron_set_err(num_rows, "%s", "Failed to parse rows for lookup table");
        return -1;
    }

    lookup_table->length = 0;
    for (int i = num_rows; -1 < --i;)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*lookup_table), aeron_name_resolver_csv_table_row_t)
        if (ensure_capacity_result < 0)
        {
            aeron_set_err_from_last_err_code(
                "Failed to allocate rows for lookup table (%zu,%zu) - %s:%d",
                lookup_table->length, lookup_table->capacity, __FILE__, __LINE__);
            free(lookup_table->array);
            free(lookup_table);
            return -1;
        }

        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS == num_columns)
        {
            for (int k = num_columns, l = 0; -1 < --k; l++)
            {
                lookup_table->array[lookup_table->length].row[l] = columns[k];
            }
            lookup_table->length++;
        }
    }

    resolver->state = lookup_table;
    return 0;
}
