cd /d "%~dp0.."
call gradlew.bat :aeron-agent:checkstyleMain
call gradlew.bat :aeron-archive:checkstyleMain
call gradlew.bat :aeron-cluster:checkstyleMain
call gradlew.bat :aeron-driver:checkstyleMain
call gradlew.bat :aeron-samples:checkstyleMain
call gradlew.bat :aeron-system-tests:checkstyleMain
call gradlew.bat :aeron-test-support:checkstyleMain
