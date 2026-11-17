param(
    [Parameter(Mandatory = $true)][string]$CppRoot,
    [string]$BuildDir = "build/battle-script-oracle"
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$CppRoot = (Resolve-Path $CppRoot).Path
$BuildDir = [IO.Path]::GetFullPath((Join-Path $repo $BuildDir))
$native = Join-Path $BuildDir 'native'
New-Item -ItemType Directory -Force $native | Out-Null
$head = (& git -C $CppRoot rev-parse HEAD).Trim()
$fixture = Get-Content (Join-Path $repo 'tests/fixtures/battle_script_combo_head.json') -Raw | ConvertFrom-Json
if ($head -ne $fixture.cpp_head) { throw "Fixture uses C++ $($fixture.cpp_head); checkout is $head" }
$archive = Join-Path $BuildDir 'cpp-head.tar'
& git -C $CppRoot archive --format=tar "--output=$archive" HEAD cpp
if ($LASTEXITCODE) { throw 'git archive failed' }
& tar -xf $archive -C $native
if ($LASTEXITCODE) { throw 'tar extraction failed' }
Copy-Item (Join-Path $PSScriptRoot 'CMakeLists.txt') $native
$test = Join-Path $native 'cpp/tests/turn_battle_engine_test/battle_script_test.cpp'
$source = Get-Content $test -Raw -Encoding utf8
$probe = Get-Content (Join-Path $PSScriptRoot 'oracle.inc') -Raw -Encoding utf8
$marker = '}  // namespace'
if (($source.Split(@($marker), [StringSplitOptions]::None).Count - 1) -ne 1) { throw 'Unexpected C++ fixture structure' }
$source = $source.Replace($marker, "$probe`n$marker")
Set-Content $test -Value $source -Encoding utf8 -NoNewline
$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
$libtool = & $vswhere -latest -products '*' -find 'VC/Tools/MSVC/**/bin/Hostx64/x64/lib.exe' | Select-Object -First 1
if (-not $libtool) { throw 'Visual Studio x64 lib.exe not found' }
foreach ($lib in @('proto', 'table')) {
    $inputLib = Join-Path $CppRoot "lib/$lib.lib"
    $pattern = if ($lib -eq 'proto') {
        '(battle_data|battle_node|player_battle|actor_comp|player_battle_result_comp|player_pet_comp|player_pet)\.pb\.obj$'
    } else { '(dungeon_table|monster_table|pet_table)\.pb\.obj$' }
    $members = & $libtool /nologo /list $inputLib
    if ($LASTEXITCODE) { throw "Cannot list $inputLib" }
    $remove = @($members | Where-Object { $_ -match $pattern } | ForEach-Object { "/REMOVE:$_" })
    $expectedCount = if ($lib -eq 'proto') { 7 } else { 3 }
    if ($remove.Count -ne $expectedCount) { throw "Unexpected $lib library members: $($remove.Count)" }
    & $libtool /nologo "/OUT:$native/$lib-head-filtered.lib" $inputLib @remove
    if ($LASTEXITCODE) { throw "Cannot filter $inputLib" }
}
& cmake -S $native -B "$BuildDir/out" "-DNATIVE_ROOT=$($CppRoot.Replace('\','/'))"
if ($LASTEXITCODE) { throw 'CMake configure failed' }
& cmake --build "$BuildDir/out" --config Debug --target battle_head_oracle -j 6
if ($LASTEXITCODE) { throw 'Native build failed' }
$exeDir = Join-Path $BuildDir 'out/Debug'
foreach ($dll in @('zlibd.dll', 'rdkafka.dll', 'rdkafka++.dll')) {
    Copy-Item (Join-Path $CppRoot "build/cpp/tests/$dll") $exeDir
}
$exe = Join-Path $exeDir 'battle_head_oracle.exe'
& $exe --gtest_filter='BattleScript*:-BattleScriptOracle.*' --gtest_color=no
if ($LASTEXITCODE) { throw 'Native tests failed' }
$output = & $exe --gtest_filter='BattleScriptOracle.DumpCombo' --gtest_color=no
if ($LASTEXITCODE) { throw 'Native oracle failed' }
foreach ($key in @('ORACLE_REQUEST', 'ORACLE_STATE', 'ORACLE_ROUND1', 'ORACLE_ROUND2')) {
    $line = @($output | Where-Object { $_.StartsWith("$key=") })
    if ($line.Count -ne 1 -or $line[0].Substring($key.Length + 1) -ne $fixture.$key) {
        throw "Native $key differs from recorded fixture"
    }
}
Write-Output 'Native fixture matches; run pytest tests/test_battle_script.py to compare Python.'
