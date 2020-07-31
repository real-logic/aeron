#
# Copyright 2014-2020 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

find_package(Java REQUIRED)

include_directories(${AERON_CLIENT_SOURCE_PATH})
include_directories(${AERON_ARCHIVE_SOURCE_PATH})

add_definitions(-DAERON_ALL_JAR="${AERON_ALL_JAR}")
add_definitions(-DJAVA_EXECUTABLE="${Java_JAVA_EXECUTABLE}")
add_definitions(-DARCHIVE_DIR="${CMAKE_CURRENT_BINARY_DIR}/archive")
add_definitions(-DJAVA_MAJOR_VERSION=${Java_VERSION_MAJOR})
add_definitions(-DJAVA_MINOR_VERSION=${Java_VERSION_MINOR})

function(aeron_archive_client_test name file)
    add_executable(${name} ${file})
    target_link_libraries(${name} aeron_client aeron_archive_client ${GMOCK_LIBS} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies(${name} gmock aeron-all-jar)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

aeron_archive_client_test(archiveTest AeronArchiveTest.cpp)
set_tests_properties(archiveTest PROPERTIES RUN_SERIAL ON)
