SET(SOURCE
    MediaDriver.cpp)

SET(HEADERS
    concurrent/OneToOneConcurrentArrayQueue.h)

add_library(aeron_driver ${SOURCE} ${HEADERS})
add_executable(MediaDriver MediaDriverMain.cpp)

target_link_libraries(
    aeron_driver
    aeron_client
    ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(
    MediaDriver
    aeron_client
    aeron_driver
    ${CMAKE_THREAD_LIBS_INIT})
