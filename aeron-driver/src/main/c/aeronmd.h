/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_AERONMD_H
#define AERON_AERONMD_H

#include <stdbool.h>
#include <stdint.h>

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_stct aeron_driver_t;

/*
 * Public API for embedding driver.
 */

#define AERON_DIR_ENV_VAR "AERON_DIR"
#define AERON_THREADING_MODE_ENV_VAR "AERON_THREADING_MODE"
#define AERON_DIR_DELETE_ON_START_ENV_VAR "AERON_DIR_DELETE_ON_START"
#define AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR "AERON_CONDUCTOR_BUFFER_LENGTH"
#define AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR "AERON_CLIENTS_BUFFER_LENGTH"
#define AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR "AERON_COUNTERS_BUFFER_LENGTH"
#define AERON_ERROR_BUFFER_LENGTH_ENV_VAR "AERON_ERROR_BUFFER_LENGTH"
#define AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR "AERON_CLIENT_LIVENESS_TIMEOUT"

/* load settings from Java properties file (https://en.wikipedia.org/wiki/.properties) and set env vars */
int aeron_driver_load_properties_file(const char *filename);

/* create and init context */
int aeron_driver_context_init(aeron_driver_context_t **context);
int aeron_driver_context_set(const char *setting, const char *value);
int aeron_driver_context_close(aeron_driver_context_t *context);

/* create and init driver from context */
int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context);
int aeron_driver_start(aeron_driver_t *driver, bool manual_main_loop);
int aeron_driver_main_do_work(aeron_driver_t *driver);
void aeron_driver_main_idle_strategy(aeron_driver_t *driver, int work_count);
int aeron_driver_close(aeron_driver_t *driver);

int aeron_dir_delete(const char *dirname);

typedef int64_t (*aeron_clock_func_t)();

int64_t aeron_nanoclock();
int64_t aeron_epochclock();

typedef void (*aeron_log_func_t)(const char *);
bool aeron_is_driver_active(const char *dirname, int64_t timeout, int64_t now, aeron_log_func_t log_func);

#endif //AERON_AERONMD_H
