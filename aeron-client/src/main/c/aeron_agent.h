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

#ifndef AERON_AGENT_H
#define AERON_AGENT_H

#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_common.h"
#include "aeronc.h"

typedef int (*aeron_agent_do_work_func_t)(void *);

typedef void (*aeron_agent_on_close_func_t)(void *);

#define AERON_AGENT_STATE_UNUSED 0
#define AERON_AGENT_STATE_INITED 1
#define AERON_AGENT_STATE_STARTED 2
#define AERON_AGENT_STATE_MANUAL 3
#define AERON_AGENT_STATE_STOPPING 4
#define AERON_AGENT_STATE_STOPPED 5

typedef struct aeron_idle_strategy_stct
{
    aeron_idle_strategy_func_t idle;
    aeron_idle_strategy_init_func_t init;
}
aeron_idle_strategy_t;

#define AERON_IDLE_STRATEGY_BACKOFF_MAX_SPINS (10)
#define AERON_IDLE_STRATEGY_BACKOFF_MAX_YIELDS (20)
#define AERON_IDLE_STRATEGY_BACKOFF_MIN_PARK_PERIOD_NS (1000LL)
#define AERON_IDLE_STRATEGY_BACKOFF_MAX_PARK_PERIOD_NS (1 * 1000 * 1000LL)

void aeron_idle_strategy_sleeping_idle(void *state, int work_count);

void aeron_idle_strategy_yielding_idle(void *state, int work_count);

void aeron_idle_strategy_busy_spinning_idle(void *state, int work_count);

void aeron_idle_strategy_noop_idle(void *state, int work_count);

void aeron_idle_strategy_backoff_idle(void *state, int work_count);

int aeron_idle_strategy_backoff_state_init(
    void **state, uint64_t max_spins, uint64_t max_yields, uint64_t min_park_period_ns, uint64_t max_park_period_ns);

int aeron_idle_strategy_init_null(void **state, const char *env_var, const char *load_args);

typedef struct aeron_agent_runner_stct
{
    const char *role_name;
    void *agent_state;
    void *idle_strategy_state;
    void *on_start_state;
    aeron_agent_on_start_func_t on_start;
    aeron_agent_do_work_func_t do_work;
    aeron_agent_on_close_func_t on_close;
    aeron_idle_strategy_func_t idle_strategy;
    aeron_thread_t thread;
    volatile bool running;
    uint8_t state;
}
aeron_agent_runner_t;

aeron_idle_strategy_func_t aeron_idle_strategy_load(
    const char *idle_strategy_name,
    void **idle_strategy_state,
    const char *env_var,
    const char *load_args);

aeron_agent_on_start_func_t aeron_agent_on_start_load(const char *name);

int aeron_agent_init(
    aeron_agent_runner_t *runner,
    const char *role_name,
    void *state,
    aeron_agent_on_start_func_t on_start,
    void *on_start_state,
    aeron_agent_do_work_func_t do_work,
    aeron_agent_on_close_func_t on_close,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_agent_start(aeron_agent_runner_t *runner);

inline int aeron_agent_do_work(aeron_agent_runner_t *runner)
{
    return runner->do_work(runner->agent_state);
}

inline bool aeron_agent_is_running(aeron_agent_runner_t *runner)
{
    bool running;
    AERON_GET_ACQUIRE(running, runner->running);
    return running;
}

inline void aeron_agent_idle(aeron_agent_runner_t *runner, int work_count)
{
    runner->idle_strategy(runner->idle_strategy_state, work_count);
}

int aeron_agent_stop(aeron_agent_runner_t *runner);

int aeron_agent_close(aeron_agent_runner_t *runner);

#endif //AERON_AGENT_H
