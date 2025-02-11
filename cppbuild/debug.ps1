#
# Copyright 2014-2025 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

$CmakeBuildParallelLevel = [Environment]::ProcessorCount
$DeleteBuildDir = $true
$BuildConfig = "Release"
$BuildDir = "$PSScriptRoot\$BuildConfig"
$SourceDir = "$PSScriptRoot\.."
$CMakeVersion = "3.30.0"
$CMakeDirName = "cmake-$CMakeVersion-windows-x86_64"
$CMakeArchive = "$CMakeDirName.zip"
$CMakePath = "$PSScriptRoot\$CMakeDirName"
$OldPath = $env:Path

try
{
    if (-not (Test-Path $CMakePath))
    {
        Write-Host "Installing $CMakeArchive ..."

        $client = New-Object System.Net.WebClient
        $client.DownloadFile("https://github.com/Kitware/CMake/releases/download/v$CMakeVersion/$CMakeArchive", "$PSScriptRoot\$CMakeArchive")

        Push-Location $PSScriptRoot
        Expand-Archive -LiteralPath "$CMakeArchive" -DestinationPath "$PSScriptRoot"
        Remove-Item "$CMakeArchive"
        Pop-Location

        Write-Host "Success: $CMakePath"
    }

    Write-Host "Installing ProcessMonitor ..."

    $client = New-Object System.Net.WebClient
    $client.DownloadFile("https://download.sysinternals.com/files/ProcessMonitor.zip", "$PSScriptRoot\ProcessMonitor.zip")

    Push-Location $PSScriptRoot
    Expand-Archive -LiteralPath "ProcessMonitor.zip" -DestinationPath "$PSScriptRoot\ProcessMonitor"
    Remove-Item "ProcessMonitor.zip"

    Write-Host "Starting $PSScriptRoot\ProcessMonitor..."

    .\ProcessMonitor\Procmon.exe /AcceptEula /NoFilter /Backingfile $PSScriptRoot\procmon.log
    Pop-Location

    Write-Host "Running?"
    Get-Process procmon | Format-List *

    if ((Test-Path $BuildDir) -and ($DeleteBuildDir))
    {
        Remove-Item -Path $BuildDir -Force -Recurse
    }

    if (-not (Test-Path $BuildDir))
    {
        [void](New-Item -Path $BuildDir -Type Directory -Force)
    }

    Push-Location -Path $BuildDir

    $vsPath = &(Join-Path ${env:ProgramFiles(x86)} "\Microsoft Visual Studio\Installer\vswhere.exe") -property installationpath
    Write-Host $vsPath
    Import-Module (Join-Path $vsPath "Common7\Tools\Microsoft.VisualStudio.DevShell.dll")
    Enter-VsDevShell -VsInstallPath $vsPath -SkipAutomaticLocation

    $env:Path = "$CMakePath\bin;$env:Path"

    Get-ChildItem -Path $PSScriptRoot

    cmake -DAERON_SYSTEM_TESTS=OFF $SourceDir
    cmake --build . --config $BuildConfig --parallel $CmakeBuildParallelLevel
    if (-not $?)
    {
        Write-Host "Compile Failed"
        Exit 1
    }

    ctest -C $BuildConfig --output-on-failure --timeout 2000

    Push-Location $PSScriptRoot
    Write-Host "Stopping $PSScriptRoot\ProcessMonitor..."
    .\ProcessMonitor\Procmon.exe /Terminate

    Write-Host "Stopped?"
    Get-Process procmon | Format-List *

    Pop-Location

    Get-ChildItem -Path $PSScriptRoot
}
finally
{
    Pop-Location
    $env:Path = $OldPath
}


