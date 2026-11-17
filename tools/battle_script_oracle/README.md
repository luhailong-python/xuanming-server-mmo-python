# Battle-script native comparison

`tests/fixtures/battle_script_combo_head.json` records `ComboRequest` and the
first two `ResolveCurrentRound` results from C++ commit
`d74bbeb17db4e58515df775af250b825c1b719d5`. The Python regression test
`test_combo_matches_native_head_each_round` reconstructs the fixture's skill and
buff tables, supplies the native initial monster snapshot, and compares every
event and state field in both rounds. The Python engine fills `action_order`;
the C++ engine returns it separately for its node to add to the response. The
test checks that order separately before comparing engine messages.

Run the Python comparison from the repository root:

```powershell
.venv/Scripts/python.exe -m pytest -q tests/test_battle_script.py
```

To rebuild and validate the native fixture on Windows, use PowerShell 7,
Visual Studio C++ tools, CMake, and the existing C++ checkout with its Debug
third-party dependencies and libraries already built:

```powershell
pwsh -NoProfile -File tools/battle_script_oracle/run.ps1 -CppRoot D:/luyuan/xuanming-server-mmo-cpp
```

The script verifies the checkout's HEAD, exports tracked C++ sources with
`git archive` into the Python repository's ignored build directory, inserts a
small fixture-output test into that copy, and compiles the HEAD battle engine,
script, test fixture, and relevant protobuf/table sources. To prevent the
locally built libraries' changed protobuf object files from overriding the HEAD
objects, it creates filtered copies of `proto.lib` and `table.lib` under the
build directory. It runs the native script tests and checks that the emitted
request, starting state and both rounds exactly match the recorded fixture.
It does not write to the C++ checkout.

This is not a fully hermetic C++ build: it links the other prebuilt C++ and
third-party libraries from the checkout, whose provenance is not guaranteed.
The snapshot provides one scripted battle, not coverage of every battle input,
service transport, or production dependency. The independent C++ and Python
test suites exercise additional script rules but are not themselves a complete
end-to-end service interoperability test.
