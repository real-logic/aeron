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

#ifndef AERON_AERON_AGENT_H
#define AERON_AERON_AGENT_H

#include <stdatomic.h>

#if !defined(_MSC_VER)
#include <pthread.h>
typedef pthread_t aeron_thread_t;
#else
/* Win32 Threads */
typedef HANDLE aeron_thread_t;
#endif

typedef int (*aeron_agent_do_work_func_t)(void *);
typedef void (*aeron_agent_on_close_func_t)(void *);

typedef void (*aeron_idle_strategy_func_t)(void *, int);
typedef int (*aeron_idle_strategy_init_func_t)(void **);

typedef struct aeron_agent_runner_stct
{
    const char *role_name;
    void *agent_state;
    void *idle_strategy_state;
    aeron_agent_do_work_func_t do_work;
    aeron_agent_on_close_func_t on_close;
    aeron_idle_strategy_func_t idle_strategy;
    aeron_thread_t thread;
    atomic_bool running;
}
aeron_agent_runner_t;

int aeron_agent_init(
    aeron_agent_runner_t **runner,
    const char *role_name,
    const char *idle_strategy_name,
    void *state,
    aeron_agent_do_work_func_t do_work,
    aeron_agent_on_close_func_t on_close);

int aeron_agent_start(aeron_agent_runner_t *runner);
int aeron_agent_close(aeron_agent_runner_t *runner);

#endif //AERON_AERON_AGENT_H
