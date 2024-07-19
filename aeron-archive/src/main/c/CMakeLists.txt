#
# Copyright 2014-2024 Real Logic Limited.
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

set(CODEC_SCHEMA ${ARCHIVE_CODEC_SCHEMA_DIR}/aeron-archive-codecs.xml)
set(GENERATED_C_CODECS
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/archiveIdRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/attachSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/authConnectRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/booleanType.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/boundedReplayRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/catalogHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/challenge.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/challengeResponse.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/closeSessionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/connectRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/controlResponse.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/controlResponseCode.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/deleteDetachedSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/detachSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/extendRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/extendRecordingRequest2.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/findLastMatchingRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/keepAliveRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingSubscriptionsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingsForUriRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/maxRecordedPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/messageHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/migrateSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/purgeRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/purgeSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingDescriptor.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingDescriptorHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingProgress.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingSignal.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingSignalEvent.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingStarted.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingState.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingStopped.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingSubscriptionDescriptor.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replayRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replayTokenRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replicateRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replicateRequest2.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/sourceLocation.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/startPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/startRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/startRecordingRequest2.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopAllReplaysRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopRecordingByIdentityRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopRecordingSubscriptionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopReplayRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopReplicationRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/taggedReplicateRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/truncateRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/varAsciiEncoding.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/varDataEncoding.h)

add_custom_command(OUTPUT ${GENERATED_C_CODECS}
    COMMAND ${CMAKE_COMMAND} -E env JAVA_HOME=$ENV{JAVA_HOME} BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME} BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION} ${GRADLE_WRAPPER} -Dcodec.target.dir=${ARCHIVE_C_CODEC_TARGET_DIR} :aeron-archive:generateCCodecs --no-daemon --console=plain -q
    DEPENDS ${CODEC_SCHEMA} aeron-all-jar
    WORKING_DIRECTORY ${ARCHIVE_CODEC_WORKING_DIR}
    COMMENT "Generating C Archive codecs")

add_custom_target(c_codecs DEPENDS ${GENERATED_C_CODECS})

SET(SOURCE
    client/aeron_archive_async_connect.c
    client/aeron_archive_client.c
    client/aeron_archive_configuration.c
    client/aeron_archive_context.c
    client/aeron_archive_control_response_poller.c
    client/aeron_archive_proxy.c
    client/aeron_archive_something.c
)

SET(HEADERS
    client/aeron_archive.h
    client/aeron_archive_client.h
    client/aeron_archive_configuration.h
    client/aeron_archive_context.h
    client/aeron_archive_control_response_poller.h
    client/aeron_archive_proxy.h
    client/aeron_archive_something.h
)

# static library
add_library(aeron_archive_c_client STATIC ${SOURCE} ${HEADERS})

add_dependencies(aeron_archive_c_client c_codecs)

target_include_directories(aeron_archive_c_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC ${AERON_C_CLIENT_SOURCE_PATH}
    PRIVATE ${ARCHIVE_CODEC_TARGET_DIR})

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_archive_c_client
    PRIVATE aeron_client
    INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(TARGETS aeron_archive_c_client ARCHIVE DESTINATION lib)
    install(DIRECTORY . DESTINATION include FILES_MATCHING PATTERN "*.h")
endif ()
