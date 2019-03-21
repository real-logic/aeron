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
#include "Configuration.h"
#include <Aeron.h>
#include <array>

using namespace aeron::util;
using namespace aeron;

std::atomic<bool> running (true);

void sigIntHandler(int param)
{
    running = false;
}

static const char optHelp     = 'h';
static const char optPrefix   = 'p';
static const char optChannel  = 'c';
static const char optStreamId = 's';
static const char optMessages = 'm';
static const char optLinger   = 'l';

struct Settings
{
    std::string dirPrefix = "";
    std::string channel = samples::configuration::DEFAULT_CHANNEL;
    std::int32_t streamId = samples::configuration::DEFAULT_STREAM_ID;
    int numberOfMessages = samples::configuration::DEFAULT_NUMBER_OF_MESSAGES;
    int lingerTimeoutMs = samples::configuration::DEFAULT_LINGER_TIMEOUT_MS;
};

typedef std::array<std::uint8_t, 256> buffer_t;

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
    s.numberOfMessages = cp.getOption(optMessages).getParamAsInt(0, 0, INT32_MAX, s.numberOfMessages);
    s.lingerTimeoutMs = cp.getOption(optLinger).getParamAsInt(0, 0, 60 * 60 * 1000, s.lingerTimeoutMs);

    return s;
}

int main(int argc, char** argv)
{
    CommandOptionParser cp;
    cp.addOption(CommandOption(optHelp,     0, 0, "                Displays help information."));
    cp.addOption(CommandOption(optPrefix,   1, 1, "dir             Prefix directory for aeron driver."));
    cp.addOption(CommandOption(optChannel,  1, 1, "channel         Channel."));
    cp.addOption(CommandOption(optStreamId, 1, 1, "streamId        Stream ID."));
    cp.addOption(CommandOption(optMessages, 1, 1, "number          Number of Messages."));
    cp.addOption(CommandOption(optLinger,   1, 1, "milliseconds    Linger timeout in milliseconds."));

    signal (SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);

        std::cout << "Publishing to channel " << settings.channel << " on Stream ID " << settings.streamId << std::endl;

        aeron::Context context;

        if (!settings.dirPrefix.empty())
        {
            context.aeronDir(settings.dirPrefix);
        }

        context.newPublicationHandler(
            [](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t correlationId)
            {
                std::cout << "Publication: " << channel << " " << correlationId << ":" << streamId << ":" << sessionId << std::endl;
            });

        std::shared_ptr<Aeron> aeron = Aeron::connect(context);

        // add the publication to start the process
        std::int64_t id = aeron->addPublication(settings.channel, settings.streamId);

        std::shared_ptr<Publication> publication = aeron->findPublication(id);
        // wait for the publication to be valid
        while (!publication)
        {
            std::this_thread::yield();
            publication = aeron->findPublication(id);
        }

        const std::int64_t channelStatus = publication->channelStatus();

        std::cout << "Publication channel status (id=" << publication->channelStatusId() << ") "
            << (channelStatus == ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE ?
                "ACTIVE" : std::to_string(channelStatus))
            << std::endl;

        AERON_DECL_ALIGNED(buffer_t buffer, 16);
        concurrent::AtomicBuffer srcBuffer(&buffer[0], buffer.size());
        char message[256];

        for (int i = 0; i < settings.numberOfMessages && running; i++)
        {
#if _MSC_VER
            const int messageLen = ::sprintf_s(message, sizeof(message), "Hello World! %d", i);
#else
            const int messageLen = ::snprintf(message, sizeof(message), "Hello World! %d", i);
#endif

            srcBuffer.putBytes(0, reinterpret_cast<std::uint8_t *>(message), messageLen);

            std::cout << "offering " << i << "/" << settings.numberOfMessages << " - ";
            std::cout.flush();

            const std::int64_t result = publication->offer(srcBuffer, 0, messageLen);

            if (result < 0)
            {
                if (BACK_PRESSURED == result)
                {
                    std::cout << "Offer failed due to back pressure" << std::endl;
                }
                else if (NOT_CONNECTED == result)
                {
                    std::cout << "Offer failed because publisher is not connected to subscriber" << std::endl;
                }
                else if (ADMIN_ACTION == result)
                {
                    std::cout << "Offer failed because of an administration action in the system" << std::endl;
                }
                else if (PUBLICATION_CLOSED == result)
                {
                    std::cout << "Offer failed publication is closed" << std::endl;
                }
                else
                {
                    std::cout << "Offer failed due to unknown reason" << result << std::endl;
                }
            }
            else
            {
                std::cout << "yay!" << std::endl;
            }

            if (!publication->isConnected())
            {
                std::cout << "No active subscribers detected" << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        std::cout << "Done sending." << std::endl;

        if (settings.lingerTimeoutMs > 0)
        {
            std::cout << "Lingering for " << settings.lingerTimeoutMs << " milliseconds." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(settings.lingerTimeoutMs));
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
