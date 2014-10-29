
#include <util/MemoryMappedFile.h>
#include <concurrent/CountersManager.h>
#include <util/CommandOptionParser.h>

#include <iostream>
#include <atomic>
#include <thread>
#include <signal.h>

using namespace aeron::common::util;
using namespace aeron::common::concurrent;
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
    std::string basePath = "/dev/shm/aeron";
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
    cp.addOption(CommandOption (optPath,   1, 1, "basePath        Base Path to shared memory. Default: /dev/shm/aeron"));
    cp.addOption(CommandOption (optPeriod, 1, 1, "update period   Update period in millseconds. Default: 1000ms"));

    signal (SIGINT, sigIntHandler);

    try
    {
        Settings settings = parseCmdLine(cp, argc, argv);
        
        MemoryMappedFile::ptr_t labelsFile   = MemoryMappedFile::mapExisting((settings.basePath + "/counters/labels").c_str());
        MemoryMappedFile::ptr_t countersFile = MemoryMappedFile::mapExisting((settings.basePath + "/counters/values").c_str());

        AtomicBuffer labelsBuffer(labelsFile->getMemoryPtr(), labelsFile->getMemorySize());
        AtomicBuffer countersBuffer(countersFile->getMemoryPtr(), countersFile->getMemorySize());

        CountersManager counters(labelsBuffer, countersBuffer);

        while(running)
        {
            steady_clock::time_point now = steady_clock::now();
            milliseconds ms = duration_cast<milliseconds>(now.time_since_epoch());

            counters.forEach([&](int id, const std::string l)
            {
                std::int64_t value = countersBuffer.getInt64Atomic(counters.counterOffset(id));
                std::cout << std::fixed << ms.count() / 1000.0 << ": " << l << ": " << value << std::endl;
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