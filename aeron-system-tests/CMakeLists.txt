add_test(
    NAME java_system_tests_c_media_driver
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMAND ./gradlew -Daeron.test.system.aeronmd.path=$<TARGET_FILE:aeronmd> :aeron-system-tests:cleanTest :aeron-system-tests:test --no-daemon)