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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <string.h>
#include <stdio.h>
#include <dlfcn.h>
#include <sched.h>
#include <errno.h>
#include "aeron_agent.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"
#include "util/aeron_error.h"

static void aeron_idle_strategy_sleeping_idle(void *state, int work_count)
{
    if (work_count > 0)
    {
        return;
    }

    nanosleep(&(struct timespec){.tv_nsec=1}, NULL);
}

static void aeron_idle_strategy_yielding_idle(void *state, int work_count)
{
    if (work_count > 0)
    {
        return;
    }

    sched_yield();
}

static void aeron_idle_strategy_noop_idle(void *state, int work_count)
{
    if (work_count > 0)
    {
        return;
    }

    __asm__ volatile("pause\n": : :"memory");
}

static int aeron_idle_strategy_init_null(void **state)
{
    *state = NULL;
    return 0;
}

aeron_idle_strategy_t aeron_idle_strategy_sleeping =
    {
        aeron_idle_strategy_sleeping_idle,
        aeron_idle_strategy_init_null
    };

aeron_idle_strategy_t aeron_idle_strategy_yielding =
    {
        aeron_idle_strategy_yielding_idle,
        aeron_idle_strategy_init_null
    };

aeron_idle_strategy_t aeron_idle_strategy_noop =
    {
        aeron_idle_strategy_noop_idle,
        aeron_idle_strategy_init_null
    };

aeron_idle_strategy_func_t aeron_idle_strategy_load(
    const char *idle_strategy_name,
    void **idle_strategy_state)
{
    char idle_func_name[AERON_MAX_PATH];
    aeron_idle_strategy_func_t idle_func = NULL;
    aeron_idle_strategy_init_func_t idle_init_func = NULL;
    void *idle_state = NULL;

    if (NULL == idle_strategy_name || NULL == idle_strategy_state)
    {
        aeron_set_err(EINVAL, "%s", "invalid idle strategy name or state");
        return NULL;
    }

    *idle_strategy_state = NULL;

    if (strncmp(idle_strategy_name, "sleeping", sizeof("sleeping")) == 0)
    {
        idle_func = aeron_idle_strategy_sleeping_idle;
    }
    else if (strncmp(idle_strategy_name, "yielding", sizeof("yielding")) == 0)
    {
        idle_func = aeron_idle_strategy_yielding_idle;
    }
    else if (strncmp(idle_strategy_name, "noop", sizeof("noop")) == 0)
    {
        idle_func = aeron_idle_strategy_noop_idle;
    }
    else
    {
        aeron_idle_strategy_t *idle_strat = NULL;

        snprintf(idle_func_name, sizeof(idle_func_name) - 1, "%s", idle_strategy_name);
        if ((idle_strat = (aeron_idle_strategy_t *)dlsym(RTLD_DEFAULT, idle_func_name)) == NULL)
        {
            aeron_set_err(EINVAL, "could not find idle strategy %s: dlsym - %s", idle_func_name, dlerror());
            return NULL;
        }

        idle_func = idle_strat->idle;
        idle_init_func = idle_strat->init;

        if (idle_init_func(&idle_state) < 0)
        {
            return NULL;
        }

        *idle_strategy_state = idle_state;
    }

    return idle_func;
}

aeron_agent_on_start_func_t aeron_agent_on_start_load(const char *name)
{
    aeron_agent_on_start_func_t func = NULL;
    if ((func = (aeron_agent_on_start_func_t)dlsym(RTLD_DEFAULT, name)) == NULL)
    {
        aeron_set_err(EINVAL, "could not find agent on_start func %s: dlsym - %s", name, dlerror());
        return NULL;
    }

    return func;
}

int aeron_agent_init(
    aeron_agent_runner_t *runner,
    const char *role_name,
    void *state,
    aeron_agent_on_start_func_t on_start,
    void *on_start_state,
    aeron_agent_do_work_func_t do_work,
    aeron_agent_on_close_func_t on_close,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state)
{
    size_t role_name_length = strlen(role_name);

    if (NULL == runner || NULL == do_work || NULL == idle_strategy_func)
    {
        aeron_set_err(EINVAL, "%s", "invalid argument");
        return -1;
    }

    runner->agent_state = state;
    runner->on_start = on_start;
    runner->on_start_state = on_start_state;
    runner->do_work = do_work;
    runner->on_close = on_close;
    if (aeron_alloc((void **)&runner->role_name, role_name_length + 1) < 0)
    {
        int err_code = errno;

        aeron_set_err(err_code, "%s:%d: %s", __FILE__, __LINE__, strerror(err_code));
        return -1;
    }
    memcpy((char *)runner->role_name, role_name, role_name_length);

    runner->idle_strategy_state = idle_strategy_state;
    runner->idle_strategy = idle_strategy_func;
    runner->running = true;
    runner->state = AERON_AGENT_STATE_INITED;

    return 0;
}

static void *agent_main(void *arg)
{
    aeron_agent_runner_t *runner = (aeron_agent_runner_t *)arg;

#if defined(Darwin)
    pthread_setname_np(runner->role_name);
#else
    pthread_setname_np(pthread_self(), runner->role_name);
#endif

    if (NULL != runner->on_start)
    {
        runner->on_start(runner->on_start_state, runner->role_name);
    }

    while (aeron_agent_is_running(runner))
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
        aeron_set_err(EINVAL, "%s", "invalid argument");
        return -1;
    }

    if ((pthread_result = pthread_attr_init(&attr)) != 0)
    {
        aeron_set_err(pthread_result, "pthread_attr_init: %s", strerror(pthread_result));
        return -1;
    }

    if ((pthread_result = pthread_create(&runner->thread, &attr, agent_main, runner)) != 0)
    {
        aeron_set_err(pthread_result, "pthread_create: %s", strerror(pthread_result));
        return -1;
    }

    runner->state = AERON_AGENT_STATE_STARTED;

    return 0;
}

extern int aeron_agent_do_work(aeron_agent_runner_t *runner);
extern bool aeron_agent_is_running(aeron_agent_runner_t *runner);
extern void aeron_agent_idle(aeron_agent_runner_t *runner, int work_count);

int aeron_agent_stop(aeron_agent_runner_t *runner)
{
    int pthread_result = 0;

    if (NULL == runner)
    {
        aeron_set_err(EINVAL, "%s", "invalid argument");
        return -1;
    }

    AERON_PUT_ORDERED(runner->running, false);

    if (AERON_AGENT_STATE_STARTED == runner->state)
    {
        runner->state = AERON_AGENT_STATE_STOPPING;

        /* TODO: should use timed pthread_join _np version when available? */

        if ((pthread_result = pthread_join(runner->thread, NULL)))
        {
            aeron_set_err(pthread_result, "pthread_join: %s", strerror(pthread_result));
            return -1;
        }

        runner->state = AERON_AGENT_STATE_STOPPED;
    }
    else if (AERON_AGENT_STATE_MANUAL == runner->state)
    {
        runner->state = AERON_AGENT_STATE_STOPPED;
    }

    return 0;
}

int aeron_agent_close(aeron_agent_runner_t *runner)
{
    if (NULL == runner)
    {
        aeron_set_err(EINVAL, "%s", "invalid argument");
        return -1;
    }

    aeron_free((char *)runner->role_name);

    if (NULL != runner->on_close)
    {
        runner->on_close(runner->agent_state);
    }

    return 0;
}

