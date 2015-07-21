@if "%DEBUG%" == "" @echo off
setlocal

set SOURCE_DIR=%CD%
set BUILD_DIR=%CD%\cppbuild\Release

if EXIST %BUILD_DIR% rd /S /Q %BUILD_DIR%

md %BUILD_DIR%
pushd %BUILD_DIR%

cmake -G "Visual Studio 14 Win64" %SOURCE_DIR%
cmake --build . --clean-first --config Release
ctest -C Release
