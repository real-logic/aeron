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
#include <cstdio>
#include <csignal>
#include <thread>

#include "util/CommandOptionParser.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "Configuration.h"
#include "RateReporter.h"
#include "FragmentAssembler.h"
#include "Aeron.h"

using namespace aeron::util;
using namespace aeron;

std::atomic<bool> running(true);

void sigIntHandler(int param)
{
    running = false;
}

static const char optHelp = 'h';
static const char optPrefix = 'p';
static const char optChannel = 'c';
static const char optStreamId = 's';
static const char optFrags = 'f';

struct Settings
{
    std::string dirPrefix = "";
    std::string channel = samples::configuration::DEFAULT_CHANNEL;
    std::int32_t streamId = samples::configuration::DEFAULT_STREAM_ID;
    int fragmentCountLimit = samples::configuration::DEFAULT_FRAGMENT_COUNT_LIMIT;
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
    s.fragmentCountLimit = cp.getOption(optFrags).getParamAsInt(0, 1, INT32_MAX, s.fragmentCountLimit);

    return s;
}

void printRate(double messagesPerSec, double bytesPerSec, int64_t totalFragments, int64_t totalBytes)
{
    std::printf(
        "%.04g msgs/sec, %.04g bytes/sec, totals %lld messages %lld MB payloads\n",
        messagesPerSec, bytesPerSec, totalFragments, totalBytes / (1024 * 1024));
}

fragment_handler_t rateReporterHandler(RateReporter &rateReporter)
{
    return
        [&](AtomicBuffer &, util::index_t, util::index_t length, Header &)
        {
            rateReporter.onMessage(1, length);
        };
}

int main(int argc, char **argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,     0, 0, "                Displays help information."));
    cp.addOption(CommandOption(optPrefix,   1, 1, "dir             Prefix directory for aeron driver."));
    cp.addOption(CommandOption(optChannel,  1, 1, "channel         Channel."));
    cp.addOption(CommandOption(optStreamId, 1, 1, "streamId        Stream ID."));
    cp.addOption(CommandOption(optFrags,    1, 1, "limit           Fragment Count Limit."));

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

        context.unavailableImageHandler(
            [](Image &image)
            {
                std::cout << "Unavailable image on correlationId=" << image.correlationId() << " sessionId=" << image.sessionId();
                std::cout << " at position=" << image.position() << " from " << image.sourceIdentity() << std::endl;
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

        BusySpinIdleStrategy idleStrategy;
        RateReporter rateReporter(std::chrono::seconds(1), printRate);
        FragmentAssembler fragmentAssembler(rateReporterHandler(rateReporter));
        fragment_handler_t handler = fragmentAssembler.handler();

        std::thread rateReporterThread(
            [&]()
            {
                rateReporter.run();
            });

        while (running)
        {
            idleStrategy.idle(subscription->poll(handler, settings.fragmentCountLimit));
        }

        std::cout << "Shutting down...\n";

        rateReporter.halt();
        rateReporterThread.join();
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
