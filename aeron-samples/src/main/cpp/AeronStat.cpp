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

#include <util/MemoryMappedFile.h>
#include <concurrent/CountersReader.h>
#include <util/CommandOptionParser.h>

#include <iostream>
#include <atomic>
#include <thread>
#include <signal.h>
#include <Context.h>
#include <cstdio>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

using namespace aeron;
using namespace aeron::util;
using namespace aeron::concurrent;
using namespace std::chrono;


std::atomic<bool> running (true);

void sigIntHandler(int param)
{
    running = false;
}

static const char optHelp   = 'h';
static const char optPath   = 'p';
static const char optPeriod = 'u';

struct Settings
{
    std::string basePath = Context::defaultAeronPath();
    int updateIntervalMs = 1000;
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

    s.basePath = cp.getOption(optPath).getParam(0, s.basePath);
    s.updateIntervalMs = cp.getOption(optPeriod).getParamAsInt(0, 1, 1000000, s.updateIntervalMs);

    return s;
}

int main (int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,   0, 0, "                Displays help information."));
    cp.addOption(CommandOption(optPath,   1, 1, "basePath        Base Path to shared memory. Default: " + Context::defaultAeronPath()));
    cp.addOption(CommandOption(optPeriod, 1, 1, "update period   Update period in milliseconds. Default: 1000ms"));

    signal (SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        MemoryMappedFile::ptr_t cncFile = MemoryMappedFile::mapExistingReadOnly(
            (settings.basePath + "/" + CncFileDescriptor::CNC_FILE).c_str());

        const std::int32_t cncVersion = CncFileDescriptor::cncVersionVolatile(cncFile);

        if (cncVersion != CncFileDescriptor::CNC_VERSION)
        {
            std::cerr << "CNC version not supported: file version=" << cncVersion << std::endl;
            return -1;
        }

        const std::int64_t clientLivenessTimeoutNs = CncFileDescriptor::clientLivenessTimeout(cncFile);
        const std::int64_t pid = CncFileDescriptor::pid(cncFile);

        AtomicBuffer metadataBuffer = CncFileDescriptor::createCounterMetadataBuffer(cncFile);
        AtomicBuffer valuesBuffer = CncFileDescriptor::createCounterValuesBuffer(cncFile);

        CountersReader counters(metadataBuffer, valuesBuffer);

        while(running)
        {
            time_t rawtime;
            char currentTime[80];

            ::time(&rawtime);
            struct tm localTm;

#ifdef _MSC_VER
            _localtime_s(&localTm, &rawTime);
#else
            ::localtime_r(&rawtime, &localTm);
#endif
            ::strftime(currentTime, sizeof(currentTime) - 1, "%H:%M:%S", &localTm);

            std::printf("\033[H\033[2J");

            std::printf(
                "%s - Aeron Stat (CnC v%" PRId32 "), pid %" PRId64 ", client liveness %s ns\n",
                currentTime, cncVersion, pid, toStringWithCommas(clientLivenessTimeoutNs).c_str());
            std::printf("===========================\n");

            counters.forEach([&](std::int32_t counterId, std::int32_t, const AtomicBuffer&, const std::string& l)
            {
                std::int64_t value = counters.getCounterValue(counterId);

                std::printf("%3d: %20s - %s\n", counterId, toStringWithCommas(value).c_str(), l.c_str());
            });

            std::this_thread::sleep_for(std::chrono::milliseconds(settings.updateIntervalMs));
        }

        std::cout << "Exiting..." << std::endl;
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
