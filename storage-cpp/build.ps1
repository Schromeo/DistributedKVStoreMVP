$ErrorActionPreference = "Stop"

$GppPath = "C:\msys64\ucrt64\bin\g++.exe"
if (!(Test-Path $GppPath)) { throw "g++ not found at $GppPath" }

$RootDir = $PSScriptRoot
$OutDir  = Join-Path $RootDir "build"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$SrcPath    = Join-Path $RootDir "kv.cpp"
$DllPath    = Join-Path $OutDir  "kv.dll"
$ImplibPath = Join-Path $OutDir  "libkv.dll.a"

$IncDir = "C:/msys64/ucrt64/include"
$LibDir = "C:/msys64/ucrt64/lib"

# IMPORTANT: use static rocksdb lib (contains full C++ symbols)
$RocksStaticLib = "C:/msys64/ucrt64/lib/librocksdb.a"

$args = @(
    "-O2",
    "-shared",
    "-std=c++20",
    "-I$IncDir",
    $SrcPath,
    "-o", $DllPath,

    "-L$LibDir",
    "-Wl,--out-implib=$ImplibPath",

    "-pthread",

    # Force-include RocksDB static library
    "-Wl,--whole-archive",
    $RocksStaticLib,
    "-Wl,--no-whole-archive",

    # Common deps (if missing, we’ll install via pacman)
    "-lsnappy",
    "-lzstd",
    "-llz4",
    "-lbz2",
    "-lz",

    # Windows system libs
    "-lshlwapi",
    "-lrpcrt4",
    "-lws2_32",

    "-static-libgcc",
    "-static-libstdc++"
)

& $GppPath @args

Write-Host "Built: $DllPath"
Write-Host "Built: $ImplibPath"
