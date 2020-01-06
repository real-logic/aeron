add_test(
    NAME system_tests_c_media_driver
    WORKING_DIRECTORY ../..
    COMMAND ./gradlew -Daeron.test.system.aeronmd.path=$<TARGET_FILE:aeronmd> :aeron-system-tests:cleanTest :aeron-system-tests:test --no-daemon)