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

#include <string.h>
#include <inttypes.h>

#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver.h"

#define AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS (4)

typedef struct aeron_csv_table_name_resolver_row_stct
{
    const char *row[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
}
aeron_csv_table_name_resolver_row_t;

typedef struct aeron_csv_table_name_resolver_stct
{
    aeron_csv_table_name_resolver_row_t *array;
    size_t length;
    size_t capacity;
    char *saved_config_csv;
}
aeron_csv_table_name_resolver_t;

int aeron_csv_table_name_resolver_lookup(
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

    aeron_csv_table_name_resolver_t *table = (aeron_csv_table_name_resolver_t *)resolver->state;

    for (size_t i = 0; i < table->length; i++)
    {
        if (strncmp(name, table->array[i].row[0], strlen(table->array[i].row[0]) + 1) == 0 &&
            strncmp(uri_param_name, table->array[i].row[1], strlen(table->array[i].row[1]) + 1) == 0)
        {
            int address_idx = is_re_resolution ? 2 : 3;
            *resolved_name = table->array[i].row[address_idx];
            return 1;
        }
    }

    return aeron_default_name_resolver_lookup(resolver, name, uri_param_name, is_re_resolution, resolved_name);
}

int aeron_csv_table_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_csv_table_name_resolver_t *resolver_state = (aeron_csv_table_name_resolver_t *)resolver->state;

    aeron_free(resolver_state->saved_config_csv);
    aeron_free(resolver_state->array);
    aeron_free(resolver->state);
    return 0;
}

int aeron_csv_table_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    resolver->lookup_func = aeron_csv_table_name_resolver_lookup;
    resolver->close_func = aeron_csv_table_name_resolver_close;

    resolver->resolve_func = aeron_default_name_resolver_resolve;
    resolver->do_work_func = aeron_default_name_resolver_do_work;

    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];

    if (NULL == args)
    {
        AERON_SET_ERR(EINVAL, "No CSV configuration, please specify: %s", AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR);
        return -1;
    }
    
    char *config_csv = strdup(args);
    if (NULL == config_csv)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating config string");
        return -1;
    }

    aeron_csv_table_name_resolver_t *lookup_table;
    if (aeron_alloc((void **)&lookup_table, sizeof(aeron_csv_table_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Allocating lookup table");
        aeron_free(config_csv);
        return -1;
    }

    int num_rows = aeron_tokenise(config_csv, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        AERON_SET_ERR(num_rows, "%s", "Failed to parse rows for lookup table");
        return -1;
    }

    lookup_table->saved_config_csv = config_csv;

    for (int i = num_rows; -1 < --i;)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*lookup_table), aeron_csv_table_name_resolver_row_t)
        if (ensure_capacity_result < 0)
        {
            AERON_APPEND_ERR(
                "Failed to allocate rows for lookup table (%" PRIu64 ",%" PRIu64 ")",
                (uint64_t)lookup_table->length,
                (uint64_t)lookup_table->capacity);
            aeron_free(lookup_table->array);
            aeron_free(lookup_table);
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
