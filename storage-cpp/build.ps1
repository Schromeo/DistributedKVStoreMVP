$ErrorActionPreference = "Stop"

$Gpp = "C:\msys64\ucrt64\bin\g++.exe"
if (!(Test-Path $Gpp)) {
    throw "g++ not found at $Gpp"
}

$Root   = $PSScriptRoot
$OutDir = Join-Path $Root "build"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$src = Join-Path $Root "kv.cpp"
$dll = Join-Path $OutDir "kv.dll"
$lib = Join-Path $OutDir "libkv.dll.a"

# 参数数组（PowerShell-safe）
$args = @(
    "-O2",
    "-shared",
    "-std=c++17",
    $src,
    "-o", $dll,
    "-Wl,--out-implib=$lib",
    "-static-libgcc",
    "-static-libstdc++"
)

& $Gpp @args

Write-Host "Built: $dll"
Write-Host "Built: $lib"
