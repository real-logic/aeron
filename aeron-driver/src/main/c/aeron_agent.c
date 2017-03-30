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

#include <string.h>
#include <stdio.h>
#include <dlfcn.h>
#include "aeron_agent.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"

int aeron_agent_init(
    aeron_agent_runner_t **runner,
    const char *role_name,
    const char *idle_strategy_name,
    void *state,
    aeron_agent_do_work_func_t do_work,
    aeron_agent_on_close_func_t on_close)
{
    char idle_func_name[AERON_MAX_PATH];
    aeron_agent_runner_t *_runner;
    aeron_idle_strategy_func_t idle_func = NULL;
    aeron_idle_strategy_init_func_t idle_init_func = NULL;
    void *idle_state = NULL;

    if (NULL == runner || NULL == do_work)
    {
        /* TODO: EINVAL */
        return -1;
    }

    snprintf(idle_func_name, sizeof(idle_func_name) - 1, "%s_idle", idle_strategy_name);
    if ((idle_func = (aeron_idle_strategy_func_t)dlsym(RTLD_DEFAULT, idle_func_name)) == NULL)
    {
        /* TODO: dlerror and EINVAL */
        return -1;
    }

    snprintf(idle_func_name, sizeof(idle_func_name) - 1, "%s_init", idle_strategy_name);
    if ((idle_init_func = (aeron_idle_strategy_init_func_t)dlsym(RTLD_DEFAULT, idle_func_name)) == NULL)
    {
        /* TODO: dlerror and EINVAL */
        return -1;
    }

    if (idle_init_func(&idle_state) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_runner, sizeof(aeron_agent_runner_t)) < 0)
    {
        return -1;
    }

    _runner->agent_state = state;
    _runner->do_work = do_work;
    _runner->on_close = on_close;
    _runner->role_name = strndup(role_name, AERON_MAX_PATH);
    _runner->idle_strategy_state = idle_state;
    _runner->idle_strategy = idle_func;
    atomic_init(&_runner->running, true);

    *runner = _runner;
    return 0;
}

static void *agent_main(void *arg)
{
    aeron_agent_runner_t *runner = (aeron_agent_runner_t *)arg;

    while (atomic_load(&runner->running))
    {
        runner->idle_strategy(runner->idle_strategy_state, runner->do_work(runner->agent_state));
    }

    return NULL;
}

int aeron_agent_start(aeron_agent_runner_t *runner)
{
    pthread_attr_t attr;
    int pthread_result = 0;

    if (NULL == runner)
    {
        /* TODO: EINVAL */
        return -1;
    }

    if ((pthread_result = pthread_attr_init(&attr)) != 0)
    {
        /* TODO: pthread_result has error */
        return -1;
    }

    if ((pthread_result = pthread_create(&runner->thread, &attr, agent_main, runner)) != 0)
    {
        /* TODO: pthread-result has error */
        return -1;
    }

    return 0;
}

int aeron_agent_close(aeron_agent_runner_t *runner)
{
    int pthread_result = 0;

    if (NULL == runner)
    {
        /* TODO: EINVAL */
        return -1;
    }

    atomic_store(&runner->running, false);

    /* TODO: should use timed pthread_join _np version when available? */

    if ((pthread_result = pthread_join(runner->thread, NULL)))
    {
        /* TODO: pthread-result has error */
        return -1;
    }

    if (NULL != runner->on_close)
    {
        runner->on_close(runner->agent_state);
    }

    return 0;
}

