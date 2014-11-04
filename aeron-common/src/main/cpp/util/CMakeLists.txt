#
# Copyright 2014 Real Logic Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SET(SOURCE
    MemoryMappedFile.cpp
    CommandOption.cpp
    CommandOptionParser.cpp
    StringUtil.cpp)

SET(HEADERS
    MemoryMappedFile.h
    CommandOption.h
    CommandOptionParser.h
    StringUtil.h
    Exceptions.h
    MacroUtil.h
    ScopeUtils.h
    BitUtil.h)

# static library
add_library(aeron_util STATIC ${SOURCE} ${HEADERS})

# executable for this modules tests
add_executable(utilTests
               tests/testUtil.cpp
               tests/testMemoryMappedFile.cpp)

target_link_libraries(utilTests
                      aeron_util
                      gmock_main)

# make test
add_test(NAME testUtil COMMAND utilTests)

