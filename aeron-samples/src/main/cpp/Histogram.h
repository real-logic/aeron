/*
* Copyright 2014-2017 Real Logic Ltd.
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

// Avoid warnings from hdr_histogram
extern "C"
{
#if !defined(_MSC_VER)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored  "-Wpedantic"
#include <hdr_histogram.h>
#pragma GCC diagnostic pop
#else
#pragma warning(push)
#pragma warning(disable : 4200) 
#include <hdr_histogram.h>
#pragma warning(pop)
#endif
}
