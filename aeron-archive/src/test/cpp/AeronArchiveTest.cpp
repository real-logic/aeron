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

#if defined(__linux__) || defined(Darwin)
#include <unistd.h>
#include <signal.h>
#else
#error "must spawn Java archive per test"
#endif

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "client/AeronArchive.h"

using namespace aeron::archive::client;

class AeronArchiveTest : public testing::Test
{
public:
    virtual void SetUp()
    {
        m_pid = ::fork();
        if (0 == m_pid)
        {
            if (::execl(m_java.c_str(),
                "java",
                "-Daeron.dir.delete.on.start=true",
                "-cp",
                m_aeronAllJar.c_str(),
                "io.aeron.archive.ArchivingMediaDriver",
                NULL) < 0)
            {
                perror("execl");
                ::exit(EXIT_FAILURE);
            }
        }

        std::cout << "ArchivingMediaDriver PID " << std::to_string(m_pid) << std::endl;
    }

    virtual void TearDown()
    {
        if (0 != m_pid)
        {
            int result = ::kill(m_pid, SIGINT);
            std::cout << "Shutting down PID " << m_pid << " " << result << std::endl;
            if (result < 0)
            {
                perror("kill");
            }

            ::wait(NULL);
        }
    }
protected:
    const std::string m_java = JAVA_EXECUTABLE;
    const std::string m_aeronAllJar = AERON_ALL_JAR;
    pid_t m_pid = 0;
};

//TEST_F(AeronArchiveTest, shouldSpinUpArchiveAndShutdown)
//{
//    std::cout << m_java << std::endl;
//    std::cout << m_aeronAllJar << std::endl;
//
//    std::this_thread::sleep_for(std::chrono::seconds(2));
//}

TEST_F(AeronArchiveTest, shouldBeAbleToConnectToArchive)
{
    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect();
}
