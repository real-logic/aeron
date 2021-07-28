@if "%DEBUG%" == "" @echo off
setlocal EnableDelayedExpansion

set "DIR=%~dp0"
set "SOURCE_DIR=%DIR%\.."
set "BUILD_DIR=%DIR%\Release"
set "BUILD_CONFIG=Release"
set "EXTRA_CMAKE_ARGS="
set "AERON_SKIP_RMDIR="

for %%o in (%*) do (
    if "%%o"=="--help" (
        echo %0 [--c-warnings-as-errors] [--cxx-warnings-as-errors] [--build-aeron-driver] [--link-samples-client-shared] [--build-archive-api] [--skip-rmdir] [--slow-system-tests] [--no-system-tests] [--debug-build] [--sanitise-build] [--help]
        exit /b
    ) else if "%%o"=="--c-warnings-as-errors" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DC_WARNINGS_AS_ERRORS=ON"
        echo "Enabling warnings as errors for c"
    ) else if "%%o"=="--cxx-warnings-as-errors" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DCXX_WARNINGS_AS_ERRORS=ON"
        echo "Enabling warnings as errors for c++"
    ) else if "%%o"=="--build-aeron-driver" (
        echo "Enabling building of Aeron driver is the default"
    ) else if "%%o"=="--link-samples-client-shared" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DLINK_SAMPLES_CLIENT_SHARED=ON"
    ) else if "%%o"=="--build-archive-api" (
        echo "Enabling building of Aeron Archive API is the default"
    ) else if "%%o"=="--skip-rmdir" (
        set "AERON_SKIP_RMDIR=yes"
        echo "Disabling build directory removal"
    ) else if "%%o"=="--slow-system-tests" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_SLOW_SYSTEM_TESTS=ON"
        echo "Enabling slow system tests"
    ) else if "%%o"=="--no-system-tests" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_SYSTEM_TESTS=OFF"
        echo "Disabling system tests"
    ) else if "%%o"=="--debug-build" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DCMAKE_BUILD_TYPE=Debug"
        set "BUILD_DIR=%DIR%\Debug"
        set "BUILD_CONFIG=Debug"
        echo "Enabling debug build"
    ) else if "%%o"=="--sanitise-build" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DSANITISE_BUILD=ON"
        echo "Enabling sanitise build"
    ) else (
        echo "Unknown option %%o"
        echo "Use --help for help"
        exit /b 1
    )
)

call "%DIR%\vs-helper.cmd"
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

if "%AERON_SKIP_RMDIR%" equ "yes" goto :start_build
if EXIST %BUILD_DIR% rd /S /Q %BUILD_DIR%
:start_build

md %BUILD_DIR%
pushd %BUILD_DIR%

cmake %EXTRA_CMAKE_ARGS% %SOURCE_DIR%
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

cmake --build . --config %BUILD_CONFIG%
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

ctest -C %BUILD_CONFIG% --output-on-failure
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
