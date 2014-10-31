
SET(HEADERS	
	ConnectionMessageFlyweight.h
	ConnectionReadyFlyweight.h
	ControlProtocolEvents.h
	CorrelatedMessageFlyweight.h
	PublicationMessageFlyweight.h
	PublicationReadyFlyweight.h
	ReadyFlyweight.h
	RemoveMessageFlyweight.h
	SubscriptionMessageFlyweight.h)

# header only library
add_custom_target(aeron_command SOURCES ${HEADERS})

# executable for this modules tests
add_executable(commandTests
               tests/testCommand.cpp)

target_link_libraries(commandTests
                      aeron_concurrent
                      aeron_util
                      gmock_main)

# make test
add_test(NAME testCommand COMMAND commandTests)