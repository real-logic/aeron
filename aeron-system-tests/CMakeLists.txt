if (AERON_SYSTEM_TESTS)
    add_test(
        NAME java_system_tests_c_media_driver
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND ${GRADLE_WRAPPER} -Daeron.test.system.aeronmd.path=$<TARGET_FILE:aeronmd> :aeron-system-tests:cleanTest :aeron-system-tests:test --no-daemon)
    set_tests_properties(java_system_tests_c_media_driver PROPERTIES RUN_SERIAL TRUE)
endif ()

if (AERON_SLOW_SYSTEM_TESTS)
    add_test(
        NAME java_slow_system_tests_c_media_driver
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND ${GRADLE_WRAPPER} -Daeron.test.system.aeronmd.path=$<TARGET_FILE:aeronmd> :aeron-system-tests:cleanSlowTest :aeron-system-tests:slowTest --no-daemon)
    set_tests_properties(java_slow_system_tests_c_media_driver PROPERTIES RUN_SERIAL TRUE)
endif ()
