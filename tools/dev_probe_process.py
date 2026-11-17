"""Bounded subprocess capture, including descendants of development launchers."""

import os
import signal
import subprocess
import sys


class ProbeFailure(Exception):
    pass


class WindowsJob:
    """Private kill-on-close job; targets only children created by this probe."""

    def __init__(self):
        import ctypes as ct
        from ctypes import wintypes as wt

        class BasicLimits(ct.Structure):
            _fields_ = [
                ("process_time", ct.c_int64),
                ("job_time", ct.c_int64),
                ("flags", wt.DWORD),
                ("min_working", ct.c_size_t),
                ("max_working", ct.c_size_t),
                ("active", wt.DWORD),
                ("affinity", ct.c_size_t),
                ("priority", wt.DWORD),
                ("scheduling", wt.DWORD),
            ]

        class ExtendedLimits(ct.Structure):
            _fields_ = [
                ("basic", BasicLimits),
                ("io", ct.c_uint64 * 6),
                ("process_memory", ct.c_size_t),
                ("job_memory", ct.c_size_t),
                ("peak_process", ct.c_size_t),
                ("peak_job", ct.c_size_t),
            ]

        self.api = ct.WinDLL("kernel32", use_last_error=True)
        signatures = (
            ("CreateJobObjectW", [wt.LPVOID, wt.LPCWSTR], wt.HANDLE),
            (
                "SetInformationJobObject",
                [wt.HANDLE, ct.c_int, wt.LPVOID, wt.DWORD],
                wt.BOOL,
            ),
            ("OpenProcess", [wt.DWORD, wt.BOOL, wt.DWORD], wt.HANDLE),
            ("AssignProcessToJobObject", [wt.HANDLE, wt.HANDLE], wt.BOOL),
            ("CloseHandle", [wt.HANDLE], wt.BOOL),
        )
        for name, arguments, result in signatures:
            function = getattr(self.api, name)
            function.argtypes, function.restype = arguments, result
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ProbeFailure("ACCESS_DENIED creating private probe job")
        limits = ExtendedLimits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(
            self.handle, 9, ct.byref(limits), ct.sizeof(limits)
        ):
            self.close()
            raise ProbeFailure("ACCESS_DENIED configuring private probe job")

    def assign(self, pid):
        handle = self.api.OpenProcess(0x0100 | 0x0001, False, pid)
        if not handle:
            raise ProbeFailure("ACCESS_DENIED opening private probe process")
        try:
            if not self.api.AssignProcessToJobObject(self.handle, handle):
                raise ProbeFailure("ACCESS_DENIED assigning private probe job")
        finally:
            self.api.CloseHandle(handle)

    def close(self):
        if self.handle:
            handle, self.handle = self.handle, None
            if not self.api.CloseHandle(handle):
                raise ProbeFailure("FAILED closing private probe job")


def capture(command, timeout):
    job = WindowsJob() if os.name == "nt" else None
    process = None
    try:
        if job:
            # Gate tool launch on stdin until the helper belongs to our job.
            command = [
                sys.executable,
                "-c",
                "import subprocess,sys; sys.exit(subprocess.call(sys.argv[1:]) if sys.stdin.read(1) == '1' else 2)",
                *command,
            ]
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE if job else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            start_new_session=os.name != "nt",
        )
        if job:
            job.assign(process.pid)
        try:
            stdout, stderr = process.communicate(
                input="1" if job else None, timeout=timeout
            )
        except subprocess.TimeoutExpired as error:
            try:
                if job:
                    job.close()
                else:
                    os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=1)
            except (OSError, ProbeFailure, subprocess.SubprocessError):
                raise ProbeFailure(
                    "TIMEOUT; probe process-tree cleanup failed"
                ) from error
            raise ProbeFailure("TIMEOUT; probe process tree terminated") from error
        return process.returncode, stdout, stderr
    finally:
        try:
            if job:
                job.close()
        finally:
            if process:
                if process.poll() is None:
                    process.kill()
                try:
                    process.communicate(timeout=1)
                except subprocess.TimeoutExpired:
                    # Never perform unbounded output drain after a cleanup failure.
                    pass
