/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#include <concurrent/CountersManager.h>
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

void sigIntHandler (int param)
{
    running = false;
}

static const char optHelp   = 'h';
static const char optPath   = 'p';
static const char optPeriod = 'u';

struct Settings
{
    std::string basePath = Context::defaultAeronPath();
    int updateIntervalms = 1000;
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
    s.updateIntervalms = cp.getOption(optPeriod).getParamAsInt(0, 1, 1000000, s.updateIntervalms);

    return s;
}

int main (int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption (optHelp,   0, 0, "                Displays help information."));
    cp.addOption(CommandOption (optPath,   1, 1, "basePath        Base Path to shared memory. Default: " + Context::defaultAeronPath()));
    cp.addOption(CommandOption (optPeriod, 1, 1, "update period   Update period in millseconds. Default: 1000ms"));

    signal (SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        MemoryMappedFile::ptr_t cncFile =
            MemoryMappedFile::mapExisting((settings.basePath + "/" + CncFileDescriptor::CNC_FILE).c_str());

        AtomicBuffer labelsBuffer = CncFileDescriptor::createCounterLabelsBuffer(cncFile);
        AtomicBuffer valuesBuffer = CncFileDescriptor::createCounterValuesBuffer(cncFile);

        CountersManager counters(labelsBuffer, valuesBuffer);

        while(running)
        {
            time_t rawtime;
            char currentTime[80];

            ::time(&rawtime);
            ::strftime(currentTime, sizeof(currentTime) - 1, "%H:%M:%S", localtime(&rawtime));

            std::printf("\033[H\033[2J");

            std::printf("%s - Aeron Stat\n", currentTime);
            std::printf("===========================\n");

            ::setlocale(LC_NUMERIC, "");

            counters.forEach([&](int id, const std::string l)
            {
                std::int64_t value = valuesBuffer.getInt64Volatile(counters.counterOffset(id));

                std::printf("%3d: %'20" PRId64 " - %s\n", id, value, l.c_str());
            });

            std::this_thread::sleep_for(std::chrono::milliseconds(settings.updateIntervalms));
        }

        std::cout << "Exiting..." << std::endl;
    }
    catch (CommandOptionException& e)
    {
        std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
        cp.displayOptionsHelp(std::cerr);
        return -1;
    }
    catch (SourcedException& e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << e.where() << std::endl;
        return -1;
    }
    catch (std::exception& e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << std::endl;
        return -1;
    }

    return 0;
}