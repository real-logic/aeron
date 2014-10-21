
#include <util/MemoryMappedFile.h>
#include <concurrent/CountersManager.h>

#include <iostream>
#include <atomic>
#include <thread>
#include <signal.h>

using namespace aeron::common::util;
using namespace aeron::common::concurrent;

std::atomic_bool running (true);

void sigIntHandler (int param)
{
    running = false;
}

int main ()
{
    signal (SIGINT, sigIntHandler);

    try
    {
        MemoryMappedFile::ptr_t labelsFile = MemoryMappedFile::mapExisting("/dev/shm/aeron/counters/labels");
        MemoryMappedFile::ptr_t countersFile = MemoryMappedFile::mapExisting("/dev/shm/aeron/counters/values");

        AtomicBuffer labelsBuffer(labelsFile->getMemoryPtr(), labelsFile->getMemorySize());
        AtomicBuffer countersBuffer(countersFile->getMemoryPtr(), countersFile->getMemorySize());

        CountersManager counters(labelsBuffer, countersBuffer);

        while(running)
        {
            counters.forEach([&](int id, const std::string l)
            {
                std::int64_t value = countersBuffer.getInt64Atomic(counters.counterOffset(id));
                std::cout << l << " : " << value << std::endl;
            });

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        std::cout << "Exiting..." << std::endl;
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