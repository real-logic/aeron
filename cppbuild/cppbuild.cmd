@if "%DEBUG%" == "" @echo off
setlocal EnableDelayedExpansion

set "DIR=%~dp0"
set "SOURCE_DIR=%DIR%\.."
set "BUILD_DIR=%DIR%\Release"
set "BUILD_CONFIG=Release"
set "EXTRA_CMAKE_ARGS="
set "AERON_SKIP_RMDIR="
if "%NUMBER_OF_PROCESSORS%"=="" (
    set "CMAKE_BUILD_PARALLEL_LEVEL=1"
) else (
    set "CMAKE_BUILD_PARALLEL_LEVEL=%NUMBER_OF_PROCESSORS%"
)

:loop
if not "%1"=="" (
    if "%1"=="--help" (
        echo %0 [--c-warnings-as-errors] [--cxx-warnings-as-errors] [--build-aeron-driver] [--link-samples-client-shared] [--build-archive-api] [--skip-rmdir] [--slow-system-tests] [--no-system-tests] [--debug-build] [--sanitise-build]  [--gradle-wrapper path_to_gradle] [--help]
        exit /b
    ) else if "%1"=="--c-warnings-as-errors" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DC_WARNINGS_AS_ERRORS=ON"
        echo "Enabling warnings as errors for c"
    ) else if "%1"=="--cxx-warnings-as-errors" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DCXX_WARNINGS_AS_ERRORS=ON"
        echo "Enabling warnings as errors for c++"
    ) else if "%1"=="--build-aeron-driver" (
        echo "Enabling building of Aeron driver is the default"
    ) else if "%1"=="--link-samples-client-shared" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DLINK_SAMPLES_CLIENT_SHARED=ON"
    ) else if "%1"=="--build-archive-api" (
        echo "Enabling building of Aeron Archive API is the default"
    ) else if "%1"=="--skip-rmdir" (
        set "AERON_SKIP_RMDIR=yes"
        echo "Disabling build directory removal"
    ) else if "%1"=="--no-tests" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_TESTS=OFF"
        echo "Disabling all tests"
    ) else if "%1"=="--no-system-tests" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_SYSTEM_TESTS=OFF"
        echo "Disabling system tests"
    ) else if "%1"=="--slow-system-tests" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_SLOW_SYSTEM_TESTS=ON"
        echo "Enabling slow system tests"
    ) else if "%1"=="--debug-build" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DCMAKE_BUILD_TYPE=Debug"
        set "BUILD_DIR=%DIR%\Debug"
        set "BUILD_CONFIG=Debug"
        echo "Enabling debug build"
    ) else if "%1"=="--sanitise-build" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DSANITISE_BUILD=ON"
        echo "Enabling sanitise build"
    ) else if "%1"=="--gradle-wrapper" (
        set "EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DGRADLE_WRAPPER=%2"
        echo "Setting -DGRADLE_WRAPPER=%2"
        shift
    ) else if "%1"=="--no-parallel" (
        set "CMAKE_BUILD_PARALLEL_LEVEL=1"
        echo "Disabling parallel build"
    ) else if "%1"=="--parallel-cpus" (
        set CMAKE_BUILD_PARALLEL_LEVEL=%2
        echo Using !CMAKE_BUILD_PARALLEL_LEVEL! cpus
        shift
    ) else (
        echo "Unknown option %%o"
        echo "Use --help for help"
        exit /b 1
    )

    shift
    goto :loop
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
