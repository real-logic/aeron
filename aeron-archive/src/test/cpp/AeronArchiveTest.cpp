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
#include <ftw.h>
#include <stdio.h>
#else
#error "must spawn Java archive per test"
#endif

#include <chrono>
#include <thread>
#include <iostream>
#include <iosfwd>

#include <gtest/gtest.h>
#include <client/RecordingEventsAdapter.h>

#include "client/AeronArchive.h"

using namespace aeron::archive::client;

class AeronArchiveTest : public testing::Test
{
public:
    ~AeronArchiveTest()
    {
        if (m_debug)
        {
            std::cout << m_stream.str();
        }
    }

    static int unlink_func(const char *path, const struct stat *sb, int type_flag, struct FTW *ftw)
    {
        if (remove(path) != 0)
        {
            perror("remove");
        }

        return 0;
    }

    static int deleteDir(const std::string& dirname)
    {
        return nftw(dirname.c_str(), unlink_func, 64, FTW_DEPTH | FTW_PHYS);
    }

    void SetUp() final
    {
        m_pid = ::fork();
        if (0 == m_pid)
        {
            if (::execl(m_java.c_str(),
                "java",
                "-Daeron.dir.delete.on.start=true",
                "-Daeron.archive.dir.delete.on.start=true",
                "-Daeron.threading.mode=INVOKER",
                "-Daeron.archive.threading.mode=SHARED",
                "-Daeron.archive.file.sync.level=0",
                "-Daeron.spies.simulate.connection=true",
                "-Daeron.mtu.length=4k",
                "-Daeron.term.buffer.sparse.file=true",
                ("-Daeron.archive.dir=" + m_archiveDir).c_str(),
                "-cp",
                m_aeronAllJar.c_str(),
                "io.aeron.archive.ArchivingMediaDriver",
                NULL) < 0)
            {
                perror("execl");
                ::exit(EXIT_FAILURE);
            }
        }

        m_stream << "ArchivingMediaDriver PID " << std::to_string(m_pid) << std::endl;
    }

    void TearDown() final
    {
        if (0 != m_pid)
        {
            int result = ::kill(m_pid, SIGINT);
            m_stream << "Shutting down PID " << m_pid << " " << result << std::endl;
            if (result < 0)
            {
                perror("kill");
            }

            ::wait(NULL);

            m_stream << "Deleting " << aeron::Context::defaultAeronPath() << std::endl;
            deleteDir(aeron::Context::defaultAeronPath());
            m_stream << "Deleting " << m_archiveDir << std::endl;
            deleteDir(m_archiveDir);
        }
    }
protected:
    const std::string m_java = JAVA_EXECUTABLE;
    const std::string m_aeronAllJar = AERON_ALL_JAR;
    const std::string m_archiveDir = ARCHIVE_DIR;
    pid_t m_pid = 0;

    std::ostringstream m_stream;
    bool m_debug = false;
};

TEST_F(AeronArchiveTest, shouldSpinUpArchiveAndShutdown)
{
    m_stream << m_java << std::endl;
    m_stream << m_aeronAllJar << std::endl;
    m_stream << m_archiveDir << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(1));
}

TEST_F(AeronArchiveTest, shouldBeAbleToConnectToArchive)
{
    std::shared_ptr<AeronArchive> aeronArchive = AeronArchive::connect();
}

TEST_F(AeronArchiveTest, shouldBeAbleToConnectToArchiveViaAsync)
{
    std::shared_ptr<AeronArchive::AsyncConnect> asyncConnect = AeronArchive::asyncConnect();
    aeron::concurrent::YieldingIdleStrategy idle;

    std::shared_ptr<AeronArchive> aeronArchive = asyncConnect->poll();
    while (!aeronArchive)
    {
        idle.idle();
        aeronArchive = asyncConnect->poll();
    }
}
