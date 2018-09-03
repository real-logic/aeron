/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifdef __cplusplus
extern "C"
{
#endif

#include <stdbool.h>
#include <stdint.h>

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_stct aeron_driver_t;

/**
 * Environment variables used for setting values of an aeron_driver_context_t.
 */

/**
 * The top level Aeron directory used for communication between a Media Driver and client.
 */
#define AERON_DIR_ENV_VAR "AERON_DIR"

/**
 * Threading Mode to be used by the driver.
 */
#define AERON_THREADING_MODE_ENV_VAR "AERON_THREADING_MODE"

/**
 * Attempt to delete directories on start if they exist
 */
#define AERON_DIR_DELETE_ON_START_ENV_VAR "AERON_DIR_DELETE_ON_START"

/**
 * Length (in bytes) of the conductor buffer for control commands from the clients to the media driver conductor.
 */
#define AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR "AERON_CONDUCTOR_BUFFER_LENGTH"

/**
 * Length (in bytes) of the broadcast buffers from the media driver to the clients.
 */
#define AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR "AERON_CLIENTS_BUFFER_LENGTH"

/**
 * Length (in bytes) of the value buffer for the system counters.
 */
#define AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR "AERON_COUNTERS_BUFFER_LENGTH"

/**
 * Length (in bytes) of the buffer for the distinct error log.
 */
#define AERON_ERROR_BUFFER_LENGTH_ENV_VAR "AERON_ERROR_BUFFER_LENGTH"

/**
 * Client liveness timeout in nanoseconds
 */
#define AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR "AERON_CLIENT_LIVENESS_TIMEOUT"

/**
 * Length (in bytes) of the log buffers for publication terms.
 */
#define AERON_TERM_BUFFER_LENGTH_ENV_VAR "AERON_TERM_BUFFER_LENGTH"

/**
 * Length (in bytes) of the log buffers for IPC publication terms.
 */
#define AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR "AERON_IPC_TERM_BUFFER_LENGTH"

/**
 * Should term buffers be created sparse.
 */
#define AERON_TERM_BUFFER_SPARSE_FILE_ENV_VAR "AERON_TERM_BUFFER_SPARSE_FILE"

/**
 * Should storage checks should be performed when allocating files.
 */
#define AERON_PERFORM_STORAGE_CHECKS_ENV_VAR "AERON_PERFORM_STORAGE_CHECKS"

/**
 * Should a spy subscription simulate a connection to a network publication.
 */
#define AERON_SPIES_SIMULATE_CONNECTION_ENV_VAR "AERON_SPIES_SIMULATE_CONNECTION"

/**
 * Page size for alignment of all files.
 */
#define AERON_FILE_PAGE_SIZE_ENV_VAR "AERON_FILE_PAGE_SIZE"

/**
 * Length (in bytes) of the maximum transmission unit of the publication
 */
#define AERON_MTU_LENGTH_ENV_VAR "AERON_MTU_LENGTH"

/**
 * Length (in bytes) of the maximum transmission unit of the IPC publication
 */
#define AERON_IPC_MTU_LENGTH_ENV_VAR "AERON_IPC_MTU_LENGTH"

/**
 * Window limit on IPC Publication side.
 */
#define AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR "AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH"

/**
 * Window limit on Publication side.
 */
#define AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR "AERON_PUBLICATION_TERM_WINDOW_LENGTH"

/**
 * Linger timeout in nanoseconds on publications.
 */
#define AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR "AERON_PUBLICATION_LINGER_TIMEOUT"

/**
 * SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
 */
#define AERON_SOCKET_SO_RCVBUF_ENV_VAR "AERON_SOCKET_SO_RCVBUF"

/**
 * SO_SNDBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
 */
#define AERON_SOCKET_SO_SNDBUF_ENV_VAR "AERON_SOCKET_SO_SNDBUF"

/**
 * IP_MULTICAST_TTL setting on outgoing UDP sockets.
 */
#define AERON_SOCKET_MULTICAST_TTL_ENV_VAR "AERON_SOCKET_MULTICAST_TTL"

/**
 * Ratio of sending data to polling status messages in the Sender.
 */
#define AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR "AERON_SEND_TO_STATUS_POLL_RATIO"

/**
 * Status Message timeout in nanoseconds.
 */
#define AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR "AERON_RCV_STATUS_MESSAGE_TIMEOUT"

/**
 * Supplier for flow control structure to be employed for multicast channels.
 */
#define AERON_MULTICAST_FLOWCONTROL_SUPPLIER_ENV_VAR "AERON_MULTICAST_FLOWCONTROL_SUPPLIER"

/**
 * Supplier for flow control structure to be employed for unicast channels.
 */
#define AERON_UNICAST_FLOWCONTROL_SUPPLIER_ENV_VAR "AERON_UNICAST_FLOWCONTROL_SUPPLIER"

/**
 * Image liveness timeout in nanoseconds
 */
#define AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR "AERON_IMAGE_LIVENESS_TIMEOUT"

/**
 * Length of the initial window which must be sufficient for Bandwidth Delay Product (BDP).
 */
#define AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR "AERON_RCV_INITIAL_WINDOW_LENGTH"

/**
 * Supplier for congestion control structure to be employed for Images.
 */
#define AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR "AERON_CONGESTIONCONTROL_SUPPLIER"

/**
 * Length (in bytes) of the buffer for the loss report log.
 */
#define AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR "AERON_LOSS_REPORT_BUFFER_LENGTH"

/**
 * Timeout for publication unblock in nanoseconds.
 */
#define AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR "AERON_PUBLICATION_UNBLOCK_TIMEOUT"

/**
 * Timeout for publication connection in nanoseconds.
 */
#define AERON_PUBLICATION_CONNECTION_TIMEOUT_ENV_VAR "AERON_PUBLICATION_CONNECTION_TIMEOUT"

/**
 * Interval (in nanoseconds) between checks for timers and timeouts.
 */
#define AERON_TIMER_INTERVAL_ENV_VAR "AERON_TIMER_INTERVAL"

/**
 * Idle strategy to be employed by Sender for DEDICATED Threading Mode.
 */
#define AERON_SENDER_IDLE_STRATEGY_ENV_VAR "AERON_SENDER_IDLE_STRATEGY"

/**
 * Idle strategy to be employed by Conductor for DEDICATED or SHARED_NETWORK Threading Mode.
 */
#define AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR "AERON_CONDUCTOR_IDLE_STRATEGY"

/**
 * Idle strategy to be employed by Receiver for DEDICATED Threading Mode.
 */
#define AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR "AERON_RECEIVER_IDLE_STRATEGY"

/**
 * Idle strategy to be employed by Sender and Receiver for SHARED_NETWORK Threading Mode.
 */
#define AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR "AERON_SHAREDNETWORK_IDLE_STRATEGY"

/**
 * Idle strategy to be employed by Conductor, Sender, and Receiver for SHARED Threading Mode.
 */
#define AERON_SHARED_IDLE_STRATEGY_ENV_VAR "AERON_SHARED_IDLE_STRATEGY"

/**
 * Function name to call on start of each agent.
 */
#define AERON_AGENT_ON_START_FUNCTION_ENV_VAR "AERON_AGENT_ON_START_FUNCTION"

/**
 * Timeout for freed counters before they can be reused.
 */
#define AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_ENV_VAR "AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT"

#define AERON_IPC_CHANNEL "aeron:ipc"
#define AERON_IPC_CHANNEL_LEN strlen(AERON_IPC_CHANNEL)
#define AERON_SPY_PREFIX "aeron-spy:"

/**
 * Create a aeron_driver_context_t struct and initialize with default values.
 *
 * @param context to create and initialize
 * @return 0 for success and -1 for error.
 */
int aeron_driver_context_init(aeron_driver_context_t **context);

/**
 * Close and delete aeron_driver_context_t struct.
 *
 * @param context to close and delete
 * @return 0 for success and -1 for error.
 */
int aeron_driver_context_close(aeron_driver_context_t *context);

/**
 * Create a aeron_driver_t struct and initialize from the aeron_driver_context_t struct.
 *
 * The given aeron_driver_context_t struct will be used exclusively by the driver. Do not reuse between drivers.
 *
 * @param driver to create and initialize.
 * @param context to use for initialization.
 * @return 0 for success and -1 for error.
 */
int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context);

/**
 * Start an aeron_driver_t given the threading mode. This may spawn threads for the Sender, Receiver, and Conductor
 * depending on threading mode used.
 *
 * @param driver to start.
 * @param manual_main_loop to be called by the caller for the Conductor do_work cycle.
 * @return 0 for success and -1 for error.
 */
int aeron_driver_start(aeron_driver_t *driver, bool manual_main_loop);

/**
 * Call the Conductor (or Shared) main do_work duty cycle once.
 *
 * Driver must have been created with manual_main_loop set to true.
 *
 * @param driver to call do_work duty cycle on.
 * @return 0 for success and -1 for error.
 */
int aeron_driver_main_do_work(aeron_driver_t *driver);

/**
 * Call the Conductor (or Shared) Idle Strategy.
 *
 * @param driver to idle.
 * @param work_count to pass to idle strategy.
 */
void aeron_driver_main_idle_strategy(aeron_driver_t *driver, int work_count);

/**
 * Close and delete aeron_driver_t struct.
 *
 * @param driver to close and delete
 * @return 0 for success and -1 for error.
 */
int aeron_driver_close(aeron_driver_t *driver);

/**
 * Delete the given aeron directory.
 *
 * @param dirname to delete.
 * @return 0 for success and -1 for error.
 */
int aeron_dir_delete(const char *dirname);

/**
 * Clock function used by aeron.
 */
typedef int64_t (*aeron_clock_func_t)();

/**
 * Return time in nanoseconds for machine. Is not wall clock time.
 *
 * @return nanoseconds since epoch for machine.
 */
int64_t aeron_nano_clock();

/**
 * Return time in milliseconds since epoch. Is wall clock time.
 *
 * @return milliseconds since epoch.
 */
int64_t aeron_epoch_clock();

/**
 * Function to return logging information.
 */
typedef void (*aeron_log_func_t)(const char *);

/**
 * Determine if an aeron driver is using a given aeron directory.
 *
 * @param dirname for aeron directory
 * @param timeout to use to determine activity for aeron directory
 * @param now current time in nanoseconds. @see aeron_nano_clock.
 * @param log_func to call during activity check to log diagnostic information.
 * @return true for active driver or false for no active driver.
 */
bool aeron_is_driver_active(const char *dirname, int64_t timeout, int64_t now, aeron_log_func_t log_func);

/**
 * Return current aeron error code (errno) for calling thread.
 *
 * @return aeron error code for calling thread.
 */
int aeron_errcode();

/**
 * Return the current aeron error message for calling thread.
 *
 * @return aeron error message for calling thread.
 */
const char *aeron_errmsg();

#ifdef __cplusplus
}
#endif

#endif //AERON_AERONMD_H
