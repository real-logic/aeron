
SET(SOURCE
    AtomicBuffer.cpp
    AtomicCounter.cpp
    CountersManager.cpp)

SET(HEADERS
    AtomicBuffer.h
    AtomicCounter.h
    CountersManager.h)

# static library
add_library(aeron_concurrent STATIC ${SOURCE} ${HEADERS})

# executable for this modules tests
add_executable(concurrentTests
                tests/testConcurrent.cpp
                tests/testCountersManager.cpp)

target_link_libraries(concurrentTests
                        aeron_concurrent
                        aeron_util
                        gmock_main)

# make test
add_test(NAME testConcurrent COMMAND concurrentTests)

