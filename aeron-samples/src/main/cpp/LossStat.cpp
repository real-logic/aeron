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
#include <concurrent/reports/LossReportReader.h>
#include <util/CommandOptionParser.h>

#include <iostream>
#include <atomic>
#include <thread>
#include <signal.h>
#include <Context.h>
#include <cstdio>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <iomanip>

using namespace aeron;
using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::reports;
using namespace std::chrono;

static const char optHelp = 'h';
static const char optPath = 'p';

struct Settings
{
    std::string basePath = Context::defaultAeronPath();
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

    return s;
}

std::string formatDate(std::int64_t millisecondsSinceEpoch)
{
    // yyyy-MM-dd HH:mm:ss.SSSZ
    milliseconds msSinceEpoch(millisecondsSinceEpoch);
    milliseconds msAfterSec(millisecondsSinceEpoch % 1000);
    system_clock::time_point tp(msSinceEpoch);

    std::time_t tm = system_clock::to_time_t(tp);

    char timeBuffer[80];
    char msecBuffer[8];
    char tzBuffer[8];
    struct tm localTm;

#ifdef _MSC_VER
    _localtime_s(&localTm, &tm);
#else
    ::localtime_r(&tm, &localTm);
#endif

    std::strftime(timeBuffer, sizeof(timeBuffer) - 1, "%Y-%m-%d %H:%M:%S.", &localTm);
    std::snprintf(msecBuffer, sizeof(msecBuffer) - 1, "%03" PRId64, msAfterSec.count());
    std::strftime(tzBuffer, sizeof(tzBuffer) - 1, "%z", &localTm);

    return std::string(timeBuffer) + std::string(msecBuffer) + std::string(tzBuffer);
}

int main (int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,   0, 0, "                Displays help information."));
    cp.addOption(CommandOption(optPath,   1, 1, "basePath        Base Path to shared memory. Default: " + Context::defaultAeronPath()));

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);
        const std::string filename = LossReportDescriptor::file(settings.basePath);

        if (MemoryMappedFile::getFileSize(filename.c_str()) < 0)
        {
            std::cerr << "Loss report does not exist: " << filename << std::endl;
            std::exit(1);
        }

        MemoryMappedFile::ptr_t lossReportFile = MemoryMappedFile::mapExistingReadOnly(filename.c_str());
        AtomicBuffer buffer(lossReportFile->getMemoryPtr(), lossReportFile->getMemorySize());

        std::cout <<
            "OBSERVATION_COUNT, " <<
            "TOTAL_BYTES_LOST, " <<
            "FIRST_OBSERVATION, " <<
            "LAST_OBSERVATION, " <<
            "SESSION_ID, " <<
            "STREAM_ID, " <<
            "CHANNEL, " <<
            "SOURCE" << std::endl;

        const int entriesRead = LossReportReader::read(
            buffer,
            [](
                std::int64_t observationCount,
                std::int64_t totalBytesLost,
                std::int64_t firstObservationTimestamp,
                std::int64_t lastObservationTimestamp,
                std::int32_t sessionId,
                std::int32_t streamId,
                const std::string &channel,
                const std::string &source)
            {
                std::cout << std::to_string(observationCount) << ",";
                std::cout << std::to_string(totalBytesLost) << ",";
                std::cout << formatDate(firstObservationTimestamp) << ",";
                std::cout << formatDate(lastObservationTimestamp) << ",";
                std::cout << std::to_string(sessionId) << ",";
                std::cout << std::to_string(streamId) << ",";
                std::cout << channel << ",";
                std::cout << source << std::endl;
            });

        std::cout << std::to_string(entriesRead) << " entries read" << std::endl;
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

