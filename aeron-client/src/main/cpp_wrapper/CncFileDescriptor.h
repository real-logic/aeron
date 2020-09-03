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

#ifndef AERON_CNC_FILE_DESCRIPTOR_H
#define AERON_CNC_FILE_DESCRIPTOR_H

#include "util/Index.h"
#include "util/MacroUtil.h"
#include "concurrent/AtomicBuffer.h"

namespace aeron
{

using namespace aeron::util;
using namespace aeron::concurrent;

/**
* Description of the command and control file used between driver and clients
*
* File Layout
* <pre>
*  +-----------------------------+
*  |          Meta Data          |
*  +-----------------------------+
*  |      to-driver Buffer       |
*  +-----------------------------+
*  |      to-clients Buffer      |
*  +-----------------------------+
*  |   Counters Metadata Buffer  |
*  +-----------------------------+
*  |    Counters Values Buffer   |
*  +-----------------------------+
*  |          Error Log          |
*  +-----------------------------+
* </pre>
* <p>
* Meta Data Layout {@link #CNC_VERSION}
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |                      Aeron CnC Version                        |
*  +---------------------------------------------------------------+
*  |                   to-driver buffer length                     |
*  +---------------------------------------------------------------+
*  |                  to-clients buffer length                     |
*  +---------------------------------------------------------------+
*  |               Counters Metadata buffer length                 |
*  +---------------------------------------------------------------+
*  |                Counters Values buffer length                  |
*  +---------------------------------------------------------------+
*  |                   Error Log buffer length                     |
*  +---------------------------------------------------------------+
*  |                   Client Liveness Timeout                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                    Driver Start Timestamp                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                         Driver PID                            |
*  |                                                               |
*  +---------------------------------------------------------------+
* </pre>
*/
namespace CncFileDescriptor
{
static const std::string CNC_FILE = "cnc.dat";
}
}
#endif
