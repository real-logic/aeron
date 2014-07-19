Aeron
=====

Efficient reliable unicast and multicast transport protocol.

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

Client API and protocol processing

    aeron-client

Examples

    aeron-examples

Media Driver

    aeron-driver

Common Classes/Methods

    aeron-common

Benchmarks

    aeron-benchmark

Build
-----

You require the following to build Aeron:

* Latest stable [Oracle JDK 8](http://www.oracle.com/technetwork/java/)

### Gradle Build

The preferred way to build is using the gradle script included.

Full clean and build of all modules

    $ ./gradlew

Running Examples
----------------

You can run the `ExamplePublisher` with its own Media Driver via Gradle

    $ ./gradlew pub

You can run the `ExampleSubscriber` with its own Media Driver via Gradle

    $ ./gradlew sub

Media Driver Packaging
----------------------

The Media Driver is packaged by the default build into an application that can be found here

    aeron-driver/build/distributions/aeron-driver-${VERSION}.zip

The Media Driver can also be run directly from Gradle

    $ ./gradlew aeron-driver:run
