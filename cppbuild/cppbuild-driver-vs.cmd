@if "%DEBUG%" == "" @echo off
setlocal

set SOURCE_DIR=%CD%
set BUILD_DIR=%CD%\cppbuild\Release

if EXIST %BUILD_DIR% rd /S /Q %BUILD_DIR%

md %BUILD_DIR%
pushd %BUILD_DIR%

cmake -G "Visual Studio 16 2019" -DBUILD_AERON_DRIVER=ON %SOURCE_DIR%
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

cmake --build . --clean-first --config Release
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

ctest -C Release
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
