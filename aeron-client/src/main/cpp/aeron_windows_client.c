/*
 * Copyright 2014-2020 Real Logic Limited.
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
#include "util/Platform.h"

#if defined(AERON_COMPILER_MSVC)
#include "aeron_windows_client.h"
#include "util/aeron_error_client.h"

#include <WinSock2.h>
#include <Windows.h>

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    switch (fdwReason)
    {
        case DLL_PROCESS_ATTACH:
            if (!aeron_error_dll_process_attach())
            {
                return FALSE;
            }
            break;

        case DLL_THREAD_DETACH:
            aeron_error_dll_thread_detach();
            break;

        case DLL_PROCESS_DETACH:
            aeron_error_dll_process_detach();
            break;

        default:
            break;
    }

    return TRUE;
}

#endif
