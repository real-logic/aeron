#
# Copyright 2014 - 2015 Real Logic Ltd.
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
    Publication.cpp
    Subscription.cpp
    ClientConductor.cpp
    Aeron.cpp
    LogBuffers.cpp)

SET(HEADERS
    ClientConductor.h
    Context.h
    Aeron.h
    Publication.h
    Subscription.h
    DriverProxy.h
    DriverListenerAdapter.h
    LogBuffers.h)

# static library
add_library(aeron_client STATIC ${SOURCE} ${HEADERS})

target_link_libraries(aeron_client aeron_common)