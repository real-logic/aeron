project(MintPack)

file(GLOB SRCFILES *.cpp)
file(GLOB INCFILES ../../include/mintpack/*.h)
include_directories(../../include)
add_library(MintPack ${SRCFILES} ${INCFILES})
include(../../cmake/BuildSettings.cmake)
