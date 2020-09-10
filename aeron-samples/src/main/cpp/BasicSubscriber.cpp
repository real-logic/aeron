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

#include <cstdint>
#include <thread>
#include <csignal>

#include "Configuration.h"
#include "util/CommandOptionParser.h"
#include "concurrent/SleepingIdleStrategy.h"
#include "Aeron.h"

using namespace aeron::util;
using namespace aeron;

std::atomic<bool> running(true);

void sigIntHandler(int)
{
    running = false;
}

static const char optHelp = 'h';
static const char optPrefix = 'p';
static const char optChannel = 'c';
static const char optStreamId = 's';

static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS(1);
static const int FRAGMENTS_LIMIT = 10;

struct Settings
{
    std::string dirPrefix = "";
    std::string channel = samples::configuration::DEFAULT_CHANNEL;
    std::int32_t streamId = samples::configuration::DEFAULT_STREAM_ID;
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

    s.dirPrefix = cp.getOption(optPrefix).getParam(0, s.dirPrefix);
    s.channel = cp.getOption(optChannel).getParam(0, s.channel);
    s.streamId = cp.getOption(optStreamId).getParamAsInt(0, 1, INT32_MAX, s.streamId);

    return s;
}

fragment_handler_t printStringMessage()
{
    return
        [&](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header)
        {
            std::cout << "Message to stream " << header.streamId() << " from session " << header.sessionId();
            std::cout << "(" << length << "@" << offset << ") <<";
            std::cout << std::string(reinterpret_cast<const char *>(buffer.buffer()) + offset, static_cast<std::size_t>(length)) << ">>" << std::endl;
        };
}

int main(int argc, char **argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,     0, 0, "            Displays help information."));
    cp.addOption(CommandOption(optPrefix,   1, 1, "dir         Prefix directory for aeron driver."));
    cp.addOption(CommandOption(optChannel,  1, 1, "channel     Channel."));
    cp.addOption(CommandOption(optStreamId, 1, 1, "streamId    Stream ID."));

    signal(SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        std::cout << "Subscribing to channel " << settings.channel << " on Stream ID " << settings.streamId << std::endl;

        aeron::Context context;

        if (!settings.dirPrefix.empty())
        {
            context.aeronDir(settings.dirPrefix);
        }

        context.newSubscriptionHandler(
            [](const std::string &channel, std::int32_t streamId, std::int64_t correlationId)
            {
                std::cout << "Subscription: " << channel << " " << correlationId << ":" << streamId << std::endl;
            });

        context.availableImageHandler(
            [](Image &image)
            {
                std::cout << "Available image correlationId=" << image.correlationId() << " sessionId=" << image.sessionId();
                std::cout << " at position=" << image.position() << " from " << image.sourceIdentity() << std::endl;
            });

        std::shared_ptr<Aeron> aeron = Aeron::connect(context);

        // add the subscription to start the process
        std::int64_t id = aeron->addSubscription(settings.channel, settings.streamId);

        std::shared_ptr<Subscription> subscription = aeron->findSubscription(id);
        // wait for the subscription to be valid
        while (!subscription)
        {
            std::this_thread::yield();
            subscription = aeron->findSubscription(id);
        }

        const std::int64_t channelStatus = subscription->channelStatus();

        std::cout << "Subscription channel status (id=" << subscription->channelStatusId() << ") "
                  << (channelStatus == ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE ? "ACTIVE" : std::to_string(channelStatus))
                  << std::endl;

        fragment_handler_t handler = printStringMessage();
        SleepingIdleStrategy idleStrategy(IDLE_SLEEP_MS);

        while (running)
        {
            const int fragmentsRead = subscription->poll(handler, FRAGMENTS_LIMIT);
            idleStrategy.idle(fragmentsRead);
        }

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
