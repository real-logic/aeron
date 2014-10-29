
add_executable(AeronStat AeronStat.cpp)

target_link_libraries(AeronStat 
					  aeron_concurrent 
					  aeron_util)

