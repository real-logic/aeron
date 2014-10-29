Aeron
=====

Efficient reliable unicast and multicast transport protocol.

For details of usage, protocol specification, FAQ, etc. please check out the
[Wiki](https://github.com/real-logic/Aeron/wiki).

License (See LICENSE file for full license)
-------------------------------------------
Copyright 2014 Real Logic Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Directory Structure
-------------------

Client API

    aeron-client

Samples

    aeron-samples

Media Driver

    aeron-driver

Common Classes/Methods

    aeron-common

Build
-----

You require the following to build Aeron:

* Latest stable [Oracle JDK 8](http://www.oracle.com/technetwork/java/)

### Gradle Build

The preferred way to build is using the Gradle script included.

Full clean and build of all modules

    $ ./gradlew
    
### C++ Build

You require the following to build Aeron with C++:

* 2.8 or higher of [CMake](http://www.cmake.org/)
* C++11 supported compiler for the supported platform

Full clean and build of all modules

    $ cd cppbuild
    $ ./cppbuild

Running Samples
---------------

Start up a media driver

    $ java -cp aeron-samples/build/libs/samples.jar uk.co.real_logic.aeron.driver.MediaDriver

You can run the `ExampleSubscriber` from a command line

    $ java -cp aeron-samples/build/libs/samples.jar uk.co.real_logic.aeron.examples.ExampleSubscriber
    
You can run the `ExamplePublisher` from a command line

    $ java -cp aeron-samples/build/libs/samples.jar uk.co.real_logic.aeron.examples.ExamplePublisher

You can run the `AeronStat` utility to read system counters from a command line
    
    $ java -cp aeron-samples/build/libs/samples.jar uk.co.real_logic.aeron.examples.AeronStat


Media Driver Packaging
----------------------

The Media Driver is packaged by the default build into an application that can be found here

    aeron-driver/build/distributions/aeron-driver-${VERSION}.zip

