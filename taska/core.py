import json
import subprocess
import sys
from pathlib import Path

# job_meta:
# {
#   "cwd": "/demo_path/demo/default_python/venv1/workspaces/workspace1",
#   "python_path": "/demo_path/demo/default_python/venv1/Scripts/python.exe",
#   "entrypoint": "",
#   "params": {},
#   "enable": 0,
#   "crontab": "",
#   "timeout": 0,
#   "mem_limit": "",
#   "result_limit": "",
#   "stdout_limit": ""
# }


def start_proc(job_meta_path: Path):
    meta = json.loads(job_meta_path.read_text(encoding="utf-8"))
    cmd = [
        meta["python_path"],
        job_meta_path.parent.joinpath("runner.py").resolve().as_posix(),
    ]
    if sys.platform == "win32":
        proc = subprocess.Popen(
            cmd,
            creationflags=subprocess.DETACHED_PROCESS
            | subprocess.CREATE_NEW_PROCESS_GROUP
            | subprocess.CREATE_NO_WINDOW,
            cwd=meta["cwd"],
        )
    else:
        proc = subprocess.Popen(cmd, start_new_session=True, cwd=meta["cwd"])
    return proc
