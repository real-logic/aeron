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

#include "aeron_dlopen.h"


#if defined(AERON_COMPILER_GCC)

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)

	static HMODULE* modules = NULL;
	static size_t modules_size = 0;
	static size_t modules_capacity = 10;

	HMODULE GetCurrentModule()
    {
        HMODULE hModule = NULL;
        GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS, (LPCTSTR)GetCurrentModule, &hModule);
        return hModule;
    }

	void aeron_init_dlopen_support()
	{
		if (modules == NULL)
		{
			modules = (HMODULE*)malloc(sizeof(HMODULE) * modules_capacity);
			memset(modules, 0, sizeof(HMODULE) * modules_capacity);
			modules[0] = GetCurrentModule();
			modules_size = 1;
		}
	}

    void* dlsym(HMODULE module, LPCSTR name)
    {
        aeron_init_dlopen_support();

        if (module == (HMODULE)RTLD_DEFAULT)
        {
            for (size_t i = 0; i < modules_size; i++)
            {
                auto res = dlsym(modules[i], name);
                if (res != NULL)
                    return res;
            }
        }

        if (module == (HMODULE)RTLD_NEXT)
        {
            BOOL firstFound = FALSE;
			for (size_t i = 0; i < modules_size; i++)
			{
				auto res = dlsym(modules[i], name);
				if (res != NULL && firstFound)
                    return res;
                if (res != NULL && !firstFound)
                    firstFound = TRUE;
            }
        }

        return GetProcAddress(module, name);
    }

    HMODULE dlopen(LPCSTR filename)
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

    char* dlerror()
    {
        DWORD errorMessageID = GetLastError();
        LPSTR messageBuffer = NULL;
        size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

        // Leak

        return messageBuffer;
    }

#else
#error Unsupported platform!
#endif