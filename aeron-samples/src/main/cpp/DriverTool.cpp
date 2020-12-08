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

#include <iostream>
#include <thread>
#include <cstdlib>
#include <cinttypes>

#include "Context.h"
#include "util/Exceptions.h"
#include "util/CommandOptionParser.h"

#include "aeronc.h"
extern "C"
{
#include "aeron_common.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_mpsc_rb.h"
}

using namespace aeron;
using namespace aeron::util;
using namespace std::chrono;

static const char optHelp = 'h';
static const char optDirectory = 'd';
static const char optPidOnly = 'P';
static const char optTimeout = 't';
static const char optTerminateDriver = 'T';

struct Settings
{
    Settings() : directory(Context::defaultAeronPath()), pidOnly(false), terminateDriver(false)
    {
    }

    std::string directory;
    bool pidOnly = false;
    bool terminateDriver = false;
    long long timeoutMs = 10 * 1000;
};

Settings parseCmdLine(CommandOptionParser &cp, int argc, char **argv)
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
    s.timeoutMs = cp.getOption(optTimeout).getParamAsLong(0, 0, 60 * 1000, s.timeoutMs);

    return s;
}

std::string formatDate(const char *format, std::int64_t millisecondsSinceEpoch)
{
    milliseconds msSinceEpoch(millisecondsSinceEpoch);
    system_clock::time_point tp(msSinceEpoch);

    std::time_t tm = system_clock::to_time_t(tp);

    char timeBuffer[80];

    struct tm localTm{};

#ifdef _MSC_VER
    localtime_s(&localTm, &tm);
#else
    ::localtime_r(&tm, &localTm);
#endif

    std::strftime(timeBuffer, sizeof(timeBuffer) - 1, format, &localTm);

    return std::string(timeBuffer);
}

int main(int argc, char **argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,            0, 0, "           Displays help information."));
    cp.addOption(CommandOption(optPidOnly,         0, 0, "           Print PID only without anything else."));
    cp.addOption(CommandOption(optTerminateDriver, 0, 0, "           Request driver to terminate."));
    cp.addOption(CommandOption(optDirectory,       1, 1, "basePath   Base Path to shared memory. Default: " + Context::defaultAeronPath()));
    cp.addOption(CommandOption(optTimeout,         1, 1, "timeout    Number of milliseconds to wait to see if the driver metadata is available.  Default 10,000"));

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        aeron_cnc_metadata_t *aeronCncMetadata;
        aeron_mapped_file_t cncFile = {};
        std::int64_t deadlineMs = aeron_epoch_clock() + settings.timeoutMs;
        do
        {
            aeron_cnc_load_result_t result = aeron_cnc_map_file_and_load_metadata(
                settings.directory.data(), &cncFile, &aeronCncMetadata);

            if (AERON_CNC_LOAD_FAILED == result)
            {
                AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
            else if (AERON_CNC_LOAD_SUCCESS == result)
            {
                break;
            }
            else
            {
                aeron_micro_sleep(16 * 1000);
            }

            if (deadlineMs <= aeron_epoch_clock())
            {
                throw TimeoutException("Timed out trying to get driver's CnC metadata", SOURCEINFO);
            }
        }
        while (true); // Timeout...

        const std::int64_t pid = aeronCncMetadata->pid;

        if (settings.pidOnly)
        {
            std::cout << pid << std::endl;
        }
        else if (settings.terminateDriver)
        {
            Context::requestDriverTermination(settings.directory, nullptr, 0);
        }
        else
        {
            char cncFilename[AERON_MAX_PATH];
            aeron_cnc_filename(settings.directory.data(), cncFilename, AERON_MAX_PATH);

            system_clock::time_point now = system_clock::now();
            const milliseconds ms = duration_cast<milliseconds>(now.time_since_epoch());

            uint8_t *toDriverBuffer = aeron_cnc_to_driver_buffer(aeronCncMetadata);
            aeron_mpsc_rb_t toDriverRingBuffer = {};
            aeron_mpsc_rb_init(&toDriverRingBuffer, toDriverBuffer, aeronCncMetadata->to_driver_buffer_length);
            const int64_t heartbeatTimestamp = aeron_mpsc_rb_consumer_heartbeat_time_value(&toDriverRingBuffer);

            std::cout << "Command 'n Control cncFile: " << cncFilename << std::endl;
            std::cout << "Version: " << aeronCncMetadata->cnc_version << ", PID: " << pid << std::endl;
            std::cout << formatDate("%H:%M:%S", ms.count());
            std::cout << " (start: " << formatDate("%Y-%m-%d %H:%M:%S", aeronCncMetadata->start_timestamp);
            std::cout << ", activity: " << formatDate("%Y-%m-%d %H:%M:%S", heartbeatTimestamp);
            std::cout << ")" << std::endl;
        }

        aeron_unmap(&cncFile);
    }
    catch (const CommandOptionException &e)
    {
        std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
        cp.displayOptionsHelp(std::cerr);
        return -1;
    }
    catch (const SourcedException &e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << e.where() << std::endl;
        return -1;
    }
    catch (const std::exception &e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << std::endl;
        return -1;
    }

    return 0;
}
