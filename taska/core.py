import subprocess
import sys
import typing
from pathlib import Path


def start_job_file(job_path: typing.Union[Path, str]):
    job_path = Path(job_path).resolve()
    if job_path.is_dir():
        job_dir = job_path
        # job dir -> job meta file
        job_path = job_dir / "meta.json"
    elif job_path.is_file():
        job_dir = job_path.parent
    else:
        raise FileNotFoundError(job_path)
    workspace_dir = job_dir.parent.parent
    venv_dir = workspace_dir.parent.parent
    runner_path = venv_dir.parent.parent / "runner.py"
    assert runner_path.is_file()
    if sys.platform == "win32":
        executable = venv_dir / "Scripts" / "python.exe"
    else:
        executable = venv_dir / "bin" / "python"
    cmd = [executable.as_posix(), runner_path.as_posix()]
    if sys.platform == "win32":
        return subprocess.Popen(
            cmd,
            creationflags=subprocess.DETACHED_PROCESS
            | subprocess.CREATE_NEW_PROCESS_GROUP
            | subprocess.CREATE_NO_WINDOW,
            cwd=job_dir.as_posix(),
        )
    else:
        return subprocess.Popen(cmd, start_new_session=True, cwd=job_dir.as_posix())
