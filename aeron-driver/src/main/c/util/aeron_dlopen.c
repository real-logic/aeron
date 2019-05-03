/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#include <stdio.h>
#include "aeron_dlopen.h"

#if defined(AERON_COMPILER_GCC)

const char *aeron_dlinfo(const void *addr, char *buffer, size_t max_buffer_length)
{
    buffer[0] = '\0';
    Dl_info info;

    if (dladdr(addr, &info) <= 0)
    {
        return buffer;
    }

    snprintf(buffer, max_buffer_length - 1, "(%s:%s)", info.dli_fname, info.dli_sname);
    return buffer;
}

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)

#include "aeron_flow_control.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver.h"

#ifdef AERON_DRIVER
int aeron_max_multicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity);

int aeron_unicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity);

int aeron_static_window_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    size_t channel_length,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager);

void* aeron_dlsym_fallback(LPCSTR name)
{
    if (strcmp(name, "aeron_unicast_flow_control_strategy_supplier") == 0)
    {
        return aeron_unicast_flow_control_strategy_supplier;
    }

    if (strcmp(name, "aeron_max_multicast_flow_control_strategy_supplier") == 0)
    {
        return aeron_max_multicast_flow_control_strategy_supplier;
    }

    if (strcmp(name, "aeron_static_window_congestion_control_strategy_supplier") == 0)
    {
        return aeron_static_window_congestion_control_strategy_supplier;
    }

    return NULL;
}
#else
void* aeron_dlsym_fallback(LPCSTR name)
{
    return NULL;
}
#endif


static HMODULE* modules = NULL;
static size_t modules_size = 0;
static size_t modules_capacity = 10;

HMODULE GetCurrentModule()
{
    HMODULE hModule = NULL;
    if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS, (LPCTSTR)GetCurrentModule, &hModule))
    {
        return hModule;
    }

    return NULL;
}

void aeron_init_dlopen_support()
{
	if (modules == NULL)
	{
		modules = (HMODULE*)malloc(sizeof(HMODULE) * modules_capacity);
		memset(modules, 0, sizeof(HMODULE) * modules_capacity);
		modules[0] = GetCurrentModule();
		modules_size = modules[0] != NULL;
	}
}

void* aeron_dlsym(HMODULE module, LPCSTR name)
{
    aeron_init_dlopen_support();

    if (module == (HMODULE)RTLD_DEFAULT)
    {
        for (size_t i = 1; i <= modules_size; i++)
        {
            void* res = aeron_dlsym(modules[modules_size - i], name);
            if (res != NULL)
            {
                return res;
            }
        }

        return aeron_dlsym_fallback(name);
    }

    if (module == (HMODULE)RTLD_NEXT)
    {
        BOOL firstFound = FALSE;
		for (size_t i = 1; i <= modules_size; i++)
		{
            void* res = aeron_dlsym(modules[modules_size - i], name);
			if (res != NULL && firstFound)
			{
                return res;
			}

            if (res != NULL && !firstFound)
            {
                firstFound = TRUE;
            }
        }

        return aeron_dlsym_fallback(name);
    }

    return GetProcAddress(module, name);
}

HMODULE aeron_dlopen(LPCSTR filename)
{
    aeron_init_dlopen_support();

    HMODULE module = LoadLibraryA(filename);

	if (modules_size == modules_capacity)
	{
		modules_capacity = modules_capacity * 2;
		modules = (HMODULE*)realloc(modules, sizeof(HMODULE) * modules_capacity);
	}

	modules[modules_size++] = module;

    return module;
}

char* aeron_dlerror()
{
    DWORD errorMessageID = GetLastError();
    LPSTR messageBuffer = NULL;
    size_t size = FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        errorMessageID,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&messageBuffer,
        0,
        NULL);

    // Leak

    return messageBuffer;
}

const char *aeron_dlinfo(const void *addr)
{
    return "";
}

#else
#error Unsupported platform!
#endif
