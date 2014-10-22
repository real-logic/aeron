project(MintSystem)

file(GLOB INCFILES ../../include/mintsystem/*.h)
file(GLOB PRIVATE_INCFILES ../../include/mintsystem/private/*.h)
source_group("Header Files\\private" FILES ${PRIVATE_INCFILES})
include_directories(../../include)

if(${MSVC})
    file(GLOB SRCFILES msvc/*.c)
elseif(${UNIX})
    file(GLOB SRCFILES gcc/*.c)
endif()
add_library(MintSystem ${SRCFILES} ${INCFILES} ${PRIVATE_INCFILES})
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # Linux requires linking with -lrt in order to call clock_getres from mint_timer_get.
    target_link_libraries(MintSystem rt)
endif()
include(../../cmake/BuildSettings.cmake)
