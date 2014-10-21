
# static library 
add_library(aeron_concurrent STATIC 
			AtomicBuffer.cpp
			AtomicCounter.cpp
			CountersManager.cpp)

# executable for this modules tests
add_executable(concurrentTests tests/testConcurrent.cpp)
target_link_libraries(concurrentTests aeron_concurrent gtest_main)

# make test
add_test(testConcurrent concurrentTests)

