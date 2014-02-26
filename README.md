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

API and protocol processing

    aeron-core

Examples

    aeron-examples

Media/IO Driver

    aeron-iodriver

Utility Class/Methods

    aeron-util

Benchmarks (TBD)

    aeron-benchmark

Build
-----

You require the following to build Aeron:

* Latest stable [Oracle JDK 8](http://www.oracle.com/technetwork/java/)
* Latest [Simple Binary Encoding (SBE)] (https://github.com/real-logic/simple-binary-encoding) installed in local maven repo

### Gradle Build

The preferred way to build is using the gradle script included.

Full clean and build of all modules

    ./gradlew clean build
