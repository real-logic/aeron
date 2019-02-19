#
# Copyright 2014-2019 Real Logic Ltd.
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

find_package(Java REQUIRED)

set(CODEC_SCHEMA ${ARCHIVE_CODEC_SCHEMA_DIR}/aeron-archive-codecs.xml)
set(GENERATED_CODECS ${ARCHIVE_CODEC_TARGET_DIR})

add_custom_command(OUTPUT ${GENERATED_CODECS}
    COMMAND ./gradlew -Dcodec.target.dir=${ARCHIVE_CODEC_TARGET_DIR} :aeron-archive:generateCppCodecs
    DEPENDS ${CODEC_SCHEMA}
    WORKING_DIRECTORY ${ARCHIVE_CODEC_WORKING_DIR}
    COMMENT "Generating C++ Archive codecs")

add_custom_target(codecs DEPENDS ${GENERATED_CODECS})

SET(SOURCE
    client/ArchiveProxy.cpp)

SET(HEADERS
    client/ArchiveProxy.h)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

# static library
add_library(aeron_archive_client STATIC ${SOURCE} ${HEADERS})

add_dependencies(aeron_archive_client codecs)

target_include_directories(aeron_archive_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE ${ARCHIVE_CODEC_TARGET_DIR})

# relative file paths for use in exceptions
target_compile_definitions(aeron_archive_client
    PUBLIC "__PROJECT_SOURCE_DIR__=\"${PROJECT_SOURCE_DIR}\"")

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif()

target_link_libraries(aeron_archive_client
    PRIVATE aeron_client
    INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(TARGETS aeron_archive_client ARCHIVE DESTINATION lib)
    install(DIRECTORY . DESTINATION  include FILES_MATCHING PATTERN "*.h")
endif()
