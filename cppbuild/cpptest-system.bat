cd /d "%~dp0.."
call gradlew.bat -Daeron.test.system.aeronmd.path=%CD%\build\binaries\aeronmd.exe :aeron-system-tests:cleanTest :aeron-system-tests:test --no-daemon
::call gradlew.bat -Daeron.test.system.aeronmd.path=build\binaries\aeronmd :aeron-system-tests:cleanTest :aeron-system-tests:slowTest --no-daemon
pause