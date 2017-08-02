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
#define AERON_TERM_BUFFER_LENGTH_ENV_VAR "AERON_TERM_BUFFER_LENGTH"
#define AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR "AERON_IPC_TERM_BUFFER_LENGTH"
#define AERON_TERM_BUFFER_SPARSE_FILE_ENV_VAR "AERON_TERM_BUFFER_SPARSE_FILE"
#define AERON_MTU_LENGTH_ENV_VAR "AERON_MTU_LENGTH"
#define AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR "AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH"
#define AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR "AERON_PUBLICATION_TERM_WINDOW_LENGTH"
#define AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR "AERON_PUBLICATION_LINGER_TIMEOUT"
#define AERON_SOCKET_SO_RCVBUF_ENV_VAR "AERON_SOCKET_SO_RCVBUF"
#define AERON_SOCKET_SO_SNDBUF_ENV_VAR "AERON_SOCKET_SO_SNDBUF"
#define AERON_SOCKET_MULTICAST_TTL_ENV_VAR "AERON_SOCKET_MULTICAST_TTL"
#define AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR "AERON_SEND_TO_STATUS_POLL_RATIO"
#define AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR "AERON_RCV_STATUS_MESSAGE_TIMEOUT"
#define AERON_MULTICAST_FLOWCONTROL_SUPPLIER_ENV_VAR "AERON_MULTICAST_FLOWCONTROL_SUPPLIER"
#define AERON_UNICAST_FLOWCONTROL_SUPPLIER_ENV_VAR "AERON_UNICAST_FLOWCONTROL_SUPPLIER"
#define AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR "AERON_IMAGE_LIVENESS_TIMEOUT"
#define AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR "AERON_RCV_INITIAL_WINDOW_LENGTH"
#define AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR "AERON_CONGESTIONCONTROL_SUPPLIER"
#define AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR "AERON_LOSS_REPORT_BUFFER_LENGTH"
#define AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR "AERON_PUBLICATION_UNBLOCK_TIMEOUT"
#define AERON_SENDER_IDLE_STRATEGY_ENV_VAR "AERON_SENDER_IDLE_STRATEGY"
#define AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR "AERON_CONDUCTOR_IDLE_STRATEGY"
#define AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR "AERON_RECEIVER_IDLE_STRATEGY"
#define AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR "AERON_SHAREDNETWORK_IDLE_STRATEGY"
#define AERON_SHARED_IDLE_STRATEGY_ENV_VAR "AERON_SHARED_IDLE_STRATEGY"

#define AERON_IPC_CHANNEL "aeron:ipc"
#define AERON_IPC_CHANNEL_LEN strlen(AERON_IPC_CHANNEL)
#define AERON_SPY_PREFIX "aeron-spy:"

/* create and init context */
int aeron_driver_context_init(aeron_driver_context_t **context);
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

int aeron_errcode();
const char *aeron_errmsg();

#endif //AERON_AERONMD_H
