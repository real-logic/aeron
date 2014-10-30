SET(SOURCE
    MemoryMappedFile.cpp
    CommandOption.cpp
    CommandOptionParser.cpp
    StringUtil.cpp)

SET(HEADERS
    MemoryMappedFile.h
    CommandOption.h
    CommandOptionParser.h
    StringUtil.h
    Exceptions.h
    MacroUtil.h
    ScopeUtils.h)

# static library
add_library(aeron_util STATIC ${SOURCE} ${HEADERS})

# executable for this modules tests
add_executable(utilTests
               tests/testUtil.cpp
               tests/testMemoryMappedFile.cpp)

target_link_libraries(utilTests
                      aeron_util
                      gmock_main)

# make test
add_test(NAME testUtil COMMAND utilTests)

