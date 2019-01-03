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

#include <cstdint>
#include <cstdio>
#include <signal.h>
#include <util/CommandOptionParser.h>
#include <thread>
#include <Aeron.h>
#include <array>
#include <concurrent/BusySpinIdleStrategy.h>
#include "FragmentAssembler.h"
#include "Configuration.h"

using namespace aeron::util;
using namespace aeron;

std::atomic<bool> running (true);

void sigIntHandler (int param)
{
    running = false;
}

static const char optHelp         = 'h';
static const char optPrefix       = 'p';
static const char optPingChannel  = 'c';
static const char optPongChannel  = 'C';
static const char optPingStreamId = 's';
static const char optPongStreamId = 'S';
static const char optFrags        = 'f';

struct Settings
{
    std::string dirPrefix = "";
    std::string pingChannel = samples::configuration::DEFAULT_PING_CHANNEL;
    std::string pongChannel = samples::configuration::DEFAULT_PONG_CHANNEL;
    std::int32_t pingStreamId = samples::configuration::DEFAULT_PING_STREAM_ID;
    std::int32_t pongStreamId = samples::configuration::DEFAULT_PONG_STREAM_ID;
    int fragmentCountLimit = samples::configuration::DEFAULT_FRAGMENT_COUNT_LIMIT;
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
    s.pingChannel = cp.getOption(optPingChannel).getParam(0, s.pingChannel);
    s.pongChannel = cp.getOption(optPongChannel).getParam(0, s.pongChannel);
    s.pingStreamId = cp.getOption(optPingStreamId).getParamAsInt(0, 1, INT32_MAX, s.pingStreamId);
    s.pongStreamId = cp.getOption(optPongStreamId).getParamAsInt(0, 1, INT32_MAX, s.pongStreamId);
    s.fragmentCountLimit = cp.getOption(optFrags).getParamAsInt(0, 1, INT32_MAX, s.fragmentCountLimit);
    return s;
}

int main(int argc, char **argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption (optHelp,         0, 0, "                Displays help information."));
    cp.addOption(CommandOption (optPrefix,       1, 1, "dir             Prefix directory for aeron driver."));
    cp.addOption(CommandOption (optPingChannel,  1, 1, "channel         Ping Channel."));
    cp.addOption(CommandOption (optPongChannel,  1, 1, "channel         Pong Channel."));
    cp.addOption(CommandOption (optPingStreamId, 1, 1, "streamId        Ping Stream ID."));
    cp.addOption(CommandOption (optPongStreamId, 1, 1, "streamId        Pong Stream ID."));
    cp.addOption(CommandOption (optFrags,        1, 1, "limit           Fragment Count Limit."));

    signal (SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        std::cout << "Subscribing Ping at " << settings.pingChannel << " on Stream ID " << settings.pingStreamId << std::endl;
        std::cout << "Publishing Pong at " << settings.pongChannel << " on Stream ID " << settings.pongStreamId << std::endl;

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

        context.newPublicationHandler(
            [](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t correlationId)
            {
                std::cout << "Publication: " << channel << " " << correlationId << ":" << streamId << ":" << sessionId << std::endl;
            });

        context.availableImageHandler([](Image &image)
            {
                std::cout << "Available image correlationId=" << image.correlationId() << " sessionId=" << image.sessionId();
                std::cout << " at position=" << image.position() << " from " << image.sourceIdentity() << std::endl;
            });

        context.unavailableImageHandler([](Image &image)
            {
                std::cout << "Unavailable image on correlationId=" << image.correlationId() << " sessionId=" << image.sessionId();
                std::cout << " at position=" << image.position() << " from " << image.sourceIdentity() << std::endl;
            });

        Aeron aeron(context);

        std::int64_t subscriptionId = aeron.addSubscription(settings.pingChannel, settings.pingStreamId);
        std::int64_t publicationId = aeron.addPublication(settings.pongChannel, settings.pongStreamId);

        std::shared_ptr<Subscription> pingSubscription = aeron.findSubscription(subscriptionId);
        while (!pingSubscription)
        {
            std::this_thread::yield();
            pingSubscription = aeron.findSubscription(subscriptionId);
        }

        std::shared_ptr<Publication> pongPublication = aeron.findPublication(publicationId);
        while (!pongPublication)
        {
            std::this_thread::yield();
            pongPublication = aeron.findPublication(publicationId);
        }

        Publication &pongPublicationRef = *pongPublication;
        Subscription &pingSubscriptionRef = *pingSubscription;

        BusySpinIdleStrategy idleStrategy;
        BusySpinIdleStrategy pingHandlerIdleStrategy;
        FragmentAssembler fragmentAssembler(
            [&](AtomicBuffer& buffer, index_t offset, index_t length, const Header& header)
            {
                if (pongPublicationRef.offer(buffer, offset, length) > 0L)
                {
                    return;
                }

                while (pongPublicationRef.offer(buffer, offset, length) < 0L)
                {
                    pingHandlerIdleStrategy.idle();
                }
            });

        fragment_handler_t handler = fragmentAssembler.handler();

        while (running)
        {
            idleStrategy.idle(pingSubscriptionRef.poll(handler, settings.fragmentCountLimit));
        }

        std::cout << "Shutting down...\n";
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
