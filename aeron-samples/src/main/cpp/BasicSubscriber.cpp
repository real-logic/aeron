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
#include "Configuration.h"
#include <Aeron.h>

using namespace aeron::util;
using namespace aeron;

std::atomic<bool> running (true);

void sigIntHandler (int param)
{
    running = false;
}

static const char optHelp     = 'h';
static const char optPrefix   = 'p';
static const char optChannel  = 'c';
static const char optStreamId = 's';

static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS(1);
static const int FRAGMENTS_LIMIT = 10;

struct Settings
{
    std::string dirPrefix = "";
    std::string channel = samples::configuration::DEFAULT_CHANNEL;
    std::int32_t streamId = samples::configuration::DEFAULT_STREAM_ID;
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
    s.channel = cp.getOption(optChannel).getParam(0, s.channel);
    s.streamId = cp.getOption(optStreamId).getParamAsInt(0, 1, INT32_MAX, s.streamId);

    return s;
}

fragment_handler_t printStringMessage()
{
    return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        std::cout << "Message to stream " << header.streamId() << " from session " << header.sessionId();
        std::cout << "(" << length << "@" << offset << ") <<";
        std::cout << std::string((char *)buffer.buffer() + offset, (unsigned long)length) << ">>" << std::endl;
    };
}

int main(int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption (optHelp,     0, 0, "                Displays help information."));
    cp.addOption(CommandOption (optPrefix,   1, 1, "dir             Prefix directory for aeron driver."));
    cp.addOption(CommandOption (optChannel,  1, 1, "channel         Channel."));
    cp.addOption(CommandOption (optStreamId, 1, 1, "streamId        Stream ID."));

    signal (SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        std::cout << "Subscribing to channel " << settings.channel << " on Stream ID " << settings.streamId << std::endl;

        aeron::Context context;

        if (settings.dirPrefix != "")
        {
            context.aeronDir(settings.dirPrefix);
        }

        context.newSubscriptionHandler(
            [](const std::string& channel, std::int32_t streamId, std::int64_t correlationId)
            {
                std::cout << "Subscription: " << channel << " " << correlationId << ":" << streamId << std::endl;
            });

        context.newConnectionHandler([](
            const std::string& channel,
            std::int32_t streamId,
            std::int32_t sessionId,
            std::int64_t joiningPosition,
            const std::string& sourceIdentity)
            {
                std::cout << "New connection on " << channel << " streamId=" << streamId << " sessionId=" << sessionId;
                std::cout << " at position=" << joiningPosition << " from " << sourceIdentity << std::endl;
            });

        context.inactiveConnectionHandler(
            [](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t position)
            {
                std::cout << "Inactive connection on " << channel << "streamId=" << streamId << " sessionId=" << sessionId;
                std::cout << " at position=" << position << std::endl;
            });

        Aeron aeron(context);

        // add the subscription to start the process
        std::int64_t id = aeron.addSubscription(settings.channel, settings.streamId);

        std::shared_ptr<Subscription> subscription = aeron.findSubscription(id);
        // wait for the subscription to be valid
        while (!subscription)
        {
            std::this_thread::yield();
            subscription = aeron.findSubscription(id);
        }

        fragment_handler_t handler = printStringMessage();
        SleepingIdleStrategy idleStrategy(IDLE_SLEEP_MS);

        while (running)
        {
            const int fragmentsRead = subscription->poll(handler, FRAGMENTS_LIMIT);

            idleStrategy.idle(fragmentsRead);
        }

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
