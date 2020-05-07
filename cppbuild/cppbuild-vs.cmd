@if "%DEBUG%" == "" @echo off
setlocal
Setlocal EnableDelayedExpansion

set SOURCE_DIR=%CD%
set BUILD_DIR=%CD%\cppbuild\Release
set BUILD_CONFIG=Release
set "EXTRA_CMAKE_ARGS="

for %%o in (%*) do (

    set PROCESSED=0

    if "%%o"=="--help" (
        echo cppbuild-vs.cmd [--c-warnings-as-errors] [--cxx-warnings-as-errors] [--build-aeron-driver]
        exit /b
    )

    if "%%o"=="--c-warnings-as-errors" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DC_WARNINGS_AS_ERRORS=ON
        set PROCESSED=1
    )

    if "%%o"=="--cxx-warnings-as-errors" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DCXX_WARNINGS_AS_ERRORS=ON
        set PROCESSED=1
    )

    if "%%o"=="--build-archive-api" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DBUILD_AERON_ARCHIVE_API=ON
        set PROCESSED=1
    )

    if "%%o"=="--slow-system-tests" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_SLOW_SYSTEM_TESTS=ON
        set PROCESSED=1
    )

    if "%%o"=="--debug-build" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DCMAKE_BUILD_TYPE=Debug
        set BUILD_DIR=%CD%\cppbuild\Debug
        set BUILD_CONFIG=Debug
        set PROCESSED=1
    )

    if "%%o"=="--build-aeron-driver" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DBUILD_AERON_DRIVER=ON
        set PROCESSED=1
    )

    if "%%o"=="--link-samples-client-shared" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DLINK_SAMPLES_CLIENT_SHARED=ON
        set PROCESSED=1
    )

    if "%%o"=="--no-system-tests" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DAERON_SYSTEM_TESTS=OFF
        set PROCESSED=1
    )
)

call cppbuild/vs-helper.cmd
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

if EXIST %BUILD_DIR% rd /S /Q %BUILD_DIR%

md %BUILD_DIR%
pushd %BUILD_DIR%
cmake %EXTRA_CMAKE_ARGS% %SOURCE_DIR%
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

cmake --build . --config %BUILD_CONFIG%
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

ctest -C %BUILD_CONFIG% --output-on-failure
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
