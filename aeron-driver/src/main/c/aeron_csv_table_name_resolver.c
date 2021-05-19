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
#include "util/aeron_netutil.h"
#include "aeron_csv_table_name_resolver.h"

#define AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS (3)

typedef struct aeron_csv_table_name_resolver_row_stct
{
    const char *name;
    const char *initial_resolution_host;
    const char *re_resolution_host;
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

int aeron_csv_table_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    const char *hostname = name;
    if (NULL != resolver->state)
    {
        aeron_csv_table_name_resolver_t *table = (aeron_csv_table_name_resolver_t *)resolver->state;

        for (size_t i = 0; i < table->length; i++)
        {
            if (strncmp(name, table->array[i].name, strlen(table->array[i].name) + 1) == 0)
            {
                hostname = is_re_resolution ?
                    table->array[i].re_resolution_host : table->array[i].initial_resolution_host;
            }
        }
    }

    int resolve = aeron_default_name_resolver_resolve(NULL, hostname, uri_param_name, is_re_resolution, address);

    char addr_str[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
    if (0 <= resolve)
    {
        aeron_format_source_identity(addr_str, AERON_NETUTIL_FORMATTED_MAX_LENGTH, address);
    }

    int64_t now_ns = aeron_nano_clock();
    printf(
        "[%f] Resolving: %s=%s to %s (%s) = %d '%s'\n",
        (double)now_ns / 1000000000,
        uri_param_name,
        name,
        hostname,
        is_re_resolution ? "true" : "false",
        resolve,
        addr_str);

    return resolve;
}

int aeron_csv_table_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_csv_table_name_resolver_t *resolver_state = (aeron_csv_table_name_resolver_t *)resolver->state;

    if (NULL != resolver_state)
    {
        aeron_free(resolver_state->saved_config_csv);
        aeron_free(resolver_state->array);
        aeron_free(resolver_state);
    }
    return 0;
}

int aeron_csv_table_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->close_func = aeron_csv_table_name_resolver_close;

    resolver->resolve_func = aeron_csv_table_name_resolver_resolve;
    resolver->do_work_func = aeron_default_name_resolver_do_work;

    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    char *config_csv = NULL;
    aeron_csv_table_name_resolver_t *lookup_table = NULL;

    if (NULL == args)
    {
        AERON_SET_ERR(EINVAL, "No CSV configuration, please specify: %s", AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR);
        goto error;
    }

    config_csv = strdup(args);
    if (NULL == config_csv)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating config string");
        goto error;
    }

    if (aeron_alloc((void **)&lookup_table, sizeof(aeron_csv_table_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Allocating lookup table");
        goto error;
    }

    int num_rows = aeron_tokenise(config_csv, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        AERON_SET_ERR(num_rows, "%s", "Failed to parse rows for lookup table");
        goto error;
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
            goto error;
        }

        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS == num_columns)
        {
            // Fields are in reverse order.
            lookup_table->array[lookup_table->length].re_resolution_host = columns[0];
            lookup_table->array[lookup_table->length].initial_resolution_host = columns[1];
            lookup_table->array[lookup_table->length].name = columns[2];

            lookup_table->length++;
        }
    }

    resolver->state = lookup_table;

    return 0;

error:
    if (NULL != lookup_table)
    {
        aeron_free(lookup_table->array);
        aeron_free(lookup_table);
    }
    aeron_free(config_csv);
    return -1;
}
