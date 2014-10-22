# static library 
add_library(aeron_util STATIC 
			MemoryMappedFile.cpp)

# executable for this modules tests
add_executable(utilTests
                tests/testUtil.cpp
                tests/testMemoryMappedFile.cpp)

target_link_libraries(utilTests aeron_util gmock_main)

# make test
add_test(NAME testUtil COMMAND utilTests)

