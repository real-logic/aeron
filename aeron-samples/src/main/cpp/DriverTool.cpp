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

#include <iostream>
#include <atomic>
#include <thread>
#include <signal.h>
#include <Context.h>
#include <cstdio>
#include <stdlib.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <iomanip>

#include <util/MemoryMappedFile.h>
#include <util/CommandOptionParser.h>
#include <Aeron.h>

using namespace aeron;
using namespace aeron::util;
using namespace aeron::concurrent;
using namespace std::chrono;

static const char optHelp = 'h';
static const char optDirectory = 'd';
static const char optPidOnly = 'P';
static const char optTerminateDriver = 'T';

struct Settings
{
    std::string directory = Context::defaultAeronPath();
    bool pidOnly = false;
    bool terminateDriver = false;
};

Settings parseCmdLine(CommandOptionParser& cp, int argc, char** argv)
{
    cp.parse(argc, argv);
    if (cp.getOption(optHelp).isPresent())
    {
        cp.displayOptionsHelp(std::cout);
        exit(0);
    }

    Settings s;

    s.pidOnly = cp.getOption(optPidOnly).isPresent();
    s.terminateDriver = cp.getOption(optTerminateDriver).isPresent();
    s.directory = cp.getOption(optDirectory).getParam(0, s.directory);

    return s;
}

std::string formatDate(const char *format, std::int64_t millisecondsSinceEpoch)
{
    milliseconds msSinceEpoch(millisecondsSinceEpoch);
    milliseconds msAfterSec(millisecondsSinceEpoch % 1000);
    system_clock::time_point tp(msSinceEpoch);

    std::time_t tm = system_clock::to_time_t(tp);

    char timeBuffer[80];

    std::strftime(timeBuffer, sizeof(timeBuffer) - 1, format, std::localtime(&tm));

    return std::string(timeBuffer);
}

int main (int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,            0, 0, "                Displays help information."));
    cp.addOption(CommandOption(optPidOnly,         0, 0, "                Print PID only without anything else."));
    cp.addOption(CommandOption(optTerminateDriver, 0, 0, "                Request driver to terminate."));
    cp.addOption(CommandOption(optDirectory,       1, 1, "basePath        Base Path to shared memory. Default: " + Context::defaultAeronPath()));

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        const std::string cncFilename = settings.directory + "/" + CncFileDescriptor::CNC_FILE;
        MemoryMappedFile::ptr_t cncFile = MemoryMappedFile::mapExisting(cncFilename.c_str());

        const std::int32_t cncVersion = CncFileDescriptor::cncVersionVolatile(cncFile);

        if (cncVersion != CncFileDescriptor::CNC_VERSION)
        {
            std::cerr << "CNC version not supported: file version=" << cncVersion << std::endl;
            return EXIT_FAILURE;
        }

        const std::int64_t pid = CncFileDescriptor::pid(cncFile);
        AtomicBuffer toDriverBuffer(CncFileDescriptor::createToDriverBuffer(cncFile));
        ManyToOneRingBuffer ringBuffer(toDriverBuffer);

        if (settings.pidOnly)
        {
            std::cout << pid << std::endl;
        }
        else if (settings.terminateDriver)
        {
            DriverProxy driverProxy(ringBuffer);

            driverProxy.terminateDriver(nullptr, 0);
        }
        else
        {
            std::cout << "Command 'n Control file: " << cncFilename << std::endl;
            std::cout << "Version: " << cncVersion << ", PID: " << pid << std::endl;
            std::cout << formatDate("%H:%M:%S", currentTimeMillis());
            std::cout << " (start: " << formatDate("%Y-%m-%d %H:%M:%S", CncFileDescriptor::startTimestamp(cncFile));
            std::cout << ", activity: " << formatDate("%Y-%m-%d %H:%M:%S", ringBuffer.consumerHeartbeatTime());
            std::cout << ")" << std::endl;
        }
    }
    catch (const CommandOptionException& e)
    {
        std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
        cp.displayOptionsHelp(std::cerr);
        return -1;
    }
    catch (const SourcedException& e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << e.where() << std::endl;
        return -1;
    }
    catch (const std::exception& e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << std::endl;
        return -1;
    }

    return 0;
}
