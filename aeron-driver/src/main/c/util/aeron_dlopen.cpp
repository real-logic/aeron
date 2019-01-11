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
    #include <vector>


    HMODULE GetCurrentModule()
    {
        HMODULE hModule = NULL;
        GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS, (LPCTSTR)GetCurrentModule, &hModule);
        return hModule;
    }


    static std::vector<HMODULE> modules { GetCurrentModule() };

    extern "C" 
    {
        void* dlsym(HMODULE module, LPCSTR name)
        {
            if (module == (HMODULE)RTLD_DEFAULT)
            {
                for (auto& m : modules)
                {
                    auto res = dlsym(m, name);
                    if (res != nullptr)
                        return res;
                }
            }

            if (module == (HMODULE)RTLD_NEXT)
            {
                bool firstFound = false;
                for (auto& m : modules)
                {
                    auto res = dlsym(m, name);
                    if (res != nullptr && firstFound)
                        return res;
                    if (res != nullptr && !firstFound)
                        firstFound = true;
                }
            }

            return GetProcAddress(module, name);
        }

        HMODULE dlopen(LPCSTR filename)
        {
            HMODULE module = LoadLibraryA(filename);
            modules.push_back(module);
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
    }

#else
#error Unsupported platform!
#endif