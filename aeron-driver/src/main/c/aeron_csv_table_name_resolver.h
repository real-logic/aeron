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
#ifndef AERON_CSV_TABLE_NAME_RESOLVER_H
#define AERON_CSV_TABLE_NAME_RESOLVER_H

#include "aeron_name_resolver.h"

#define AERON_NAME_RESOLVER_CSV_ENTRY_COUNTER_TYPE_ID (2001)
#define AERON_NAME_RESOLVER_CSV_DISABLE_RESOLUTION_OP INT64_C(-1)
#define AERON_NAME_RESOLVER_CSV_USE_INITIAL_RESOLUTION_HOST_OP INT64_C(0)
#define AERON_NAME_RESOLVER_CSV_USE_RE_RESOLUTION_HOST_OP INT64_C(1)

int aeron_csv_table_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context);

#endif //AERON_CSV_TABLE_NAME_RESOLVER_H
