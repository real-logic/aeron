@if "%DEBUG%" == "" @echo off
setlocal
Setlocal EnableDelayedExpansion

set SOURCE_DIR=%CD%
set ZLIB_ZIP=%CD%\cppbuild\zlib1211.zip
set BUILD_DIR=%CD%\cppbuild\Release
set ZLIB_BUILD_DIR=%BUILD_DIR%\zlib-build
set ZLIB_INSTALL_DIR=%BUILD_DIR%\zlib64

for %%o in (%*) do (

    set PROCESSED=0

    if "%%o"=="--help" (
	    echo cppbuild.cmd [--c-warnings-as-errors] [--cxx-warnings-as-errors] [--build-aeron-driver]
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

    if "%%o"=="--build-aeron-driver" (
        set EXTRA_CMAKE_ARGS=!EXTRA_CMAKE_ARGS! -DBUILD_AERON_DRIVER=ON
	    set PROCESSED=1
	)
)


if EXIST %BUILD_DIR% rd /S /Q %BUILD_DIR%

md %BUILD_DIR%
pushd %BUILD_DIR%
md %ZLIB_BUILD_DIR%
pushd %ZLIB_BUILD_DIR%
7z x %ZLIB_ZIP%
pushd zlib-1.2.11
md build
pushd build
cmake -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=%ZLIB_INSTALL_DIR% ..
cmake --build . --target install

pushd %BUILD_DIR%
cmake -G "Visual Studio 15 2017 Win64" %EXTRA_CMAKE_ARGS% %SOURCE_DIR%
cmake --build . --config Release
ctest -C Release
