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
#include <cstdint>
#include <cstdio>
#include <signal.h>
#include <util/CommandOptionParser.h>
#include <thread>
#include <array>
#include "MediaDriver.h"

using namespace aeron::util;
using namespace aeron;

std::atomic<bool> running (true);

void sigIntHandler (int param)
{
    running = false;
}

static const char optHelp     = 'h';
static const char optPrefix   = 'p';

struct Settings
{
    std::string dirPrefix = "";
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

    s.dirPrefix = cp.getOption(optPrefix).getParam(0, s.dirPrefix);

    return s;
}

int main(int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp, 0, 0, "                Displays help information."));
    cp.addOption(CommandOption(optPrefix, 1, 1, "dir             Prefix directory for aeron driver."));

    signal(SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

//        if (settings.dirPrefix != "")
//        {
//            context.aeronDir(settings.dirPrefix);
//        }

        while (running)
        {

        }

        std::cout << "Shutting Down..." << std::endl;
    }
    catch (CommandOptionException& e)
    {
        std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
        cp.displayOptionsHelp(std::cerr);
        return -1;
    }
    catch (std::exception& e)
    {
        std::cerr << "FAILED: " << e.what() << " : " << std::endl;
        return -1;
    }

    return 0;
}
