import ctypes
from ctypes import wintypes
import os
import subprocess
import sys
import time

import pytest

import windows_job

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows job objects")


def _alive(pid):
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetExitCodeProcess.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    try:
        code = wintypes.DWORD()
        assert kernel.GetExitCodeProcess(handle, ctypes.byref(code))
        return code.value == 259  # STILL_ACTIVE
    finally:
        kernel.CloseHandle(handle)


def test_suspended_child_resumes_and_status_is_preserved(tmp_path):
    result = windows_job.run([sys.executable, "-c", "raise SystemExit(7)"],
                             cwd=tmp_path, env=os.environ.copy(), timeout=5)
    assert result == 7


def test_timeout_terminates_owned_descendants(tmp_path):
    pid_file = tmp_path / "child.pid"
    code = ("import pathlib,subprocess,sys,time; "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
            "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(30)")
    with pytest.raises(subprocess.TimeoutExpired):
        windows_job.run([sys.executable, "-c", code, str(pid_file)], cwd=tmp_path,
                        env=os.environ.copy(), timeout=2)
    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 5
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(.05)
    assert not _alive(pid), "owned child survived the job timeout"


def test_nonzero_parent_exit_terminates_descendants(tmp_path):
    pid_file = tmp_path / "child.pid"
    code = ("import pathlib,subprocess,sys; "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
            "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); raise SystemExit(7)")
    assert windows_job.run([sys.executable, "-c", code, str(pid_file)], cwd=tmp_path,
                           env=os.environ.copy(), timeout=5) == 7
    pid = int(pid_file.read_text())
    deadline = time.monotonic() + 5
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(.05)
    assert not _alive(pid), "owned child survived its driver's failure"
