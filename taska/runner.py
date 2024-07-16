import json
import logging
import os
import re
import sys
import time
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Timer


class LoggerStream:
    def __init__(self, logger):
        self.logger = logger
        self.linebuf = ""

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.info(line.rstrip())

    def flush(self):
        pass


def read_size(text: str):
    # 1g, 1GB, 1g => 1024**3
    m = re.match(r"(\d+)([gGmMkK])?", str(text))
    if not m:
        raise ValueError("Invalid size string: %s" % text)
    a, b = m.groups()
    size = int(a)
    if b:
        size = size * 1024 ** {"g": 3, "m": 2, "k": 1}[b.lower()]
    return size


def main():
    """job_meta:
    {
    "cwd": "/demo_path/demo/default_python/venv1/workspaces/workspace1",
    "python_path": "/demo_path/demo/default_python/venv1/Scripts/python.exe",
    "entrypoint": "",
    "params": {},
    "enable": 0,
    "crontab": "",
    "timeout": 0,
    "mem_limit": "",
    "result_limit": "",
    "stdout_limit": ""
    }"""
    job_dir = Path(__file__).parent
    meta = json.loads(job_dir.joinpath("meta.json").read_text(encoding="utf-8"))
    pid = str(os.getpid())
    pid_file = job_dir / "job.pid"
    pid_file.write_text(pid)
    cwd_path = Path(meta["cwd"])
    sys.path.insert(0, cwd_path.as_posix())
    os.chdir(cwd_path.resolve())
    if meta["mem_limit"]:
        mem_limit = read_size(meta["mem_limit"])
        if mem_limit and sys.platform != "win32":
            import resource

            resource.setrlimit(resource.RLIMIT_RSS, (mem_limit, mem_limit))

    default_log_size = 5 * 1024**2
    result_limit = read_size(meta["result_limit"] or default_log_size)
    stdout_limit = read_size(meta["stdout_limit"] or default_log_size)
    result_logger = logging.getLogger("result_logger")
    result_logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(
        job_dir.joinpath("result.jsonl").resolve().as_posix(),
        maxBytes=result_limit * 1.1,
        backupCount=1,
        encoding="utf-8",
        errors="replace",
    )
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    result_logger.addHandler(handler)

    stdout_logger = logging.getLogger("stdout_logger")
    stdout_logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(
        job_dir.joinpath("stdout.log").resolve().as_posix(),
        maxBytes=stdout_limit * 1.1,
        backupCount=1,
        encoding="utf-8",
        errors="replace",
    )
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    stdout_logger.addHandler(handler)
    sys.stdout = sys.stderr = LoggerStream(stdout_logger)
    start_at = time.strftime("%Y-%m-%d %H:%M:%S")
    start_ts = time.time()
    stdout_logger.debug(f"[INFO] Job start. pid: {pid}")
    timeout = meta.get("timeout")
    if timeout and isinstance(timeout, int):

        def force_shutdown():
            stdout_logger.error(
                f"[ERROR] Timeout Kill! start_at: {start_at}, timeout: {timeout}, pid: {pid}"
            )
            for h in stdout_logger.handlers:
                h.flush()
            os._exit(1)

        kill_timer = Timer(meta["timeout"], daemon=True, function=force_shutdown)
        kill_timer.start()
    else:
        kill_timer = None
    try:
        entrypoint = meta["entrypoint"]
        pattern = r"^\w+(\.\w+)?(:\w+)?$"
        if re.match(pattern, entrypoint):
            module, _, function = entrypoint.partition(":")
            if module:
                # main may be: 'module.py:main' or 'module.main' or 'package.module:main'
                # replace module.py to module
                module_path = cwd_path / module
                if module_path.is_file():
                    module = module_path.stem
                code = f"import {module}"
                if function:
                    locals()["RUNNER_PARAMS"] = meta["params"]
                    code += f"; globals().setdefault('RUNNER_GLOBAL_RESULT', {module}.{function}(**locals()['RUNNER_PARAMS']))"
                exec(code, globals(), locals())
                if "RUNNER_GLOBAL_RESULT" in globals():
                    result = {
                        "start_at": start_at,
                        "end_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "duration": round(time.time() - start_ts, 3),
                        "result": globals()["RUNNER_GLOBAL_RESULT"],
                    }
                    result_logger.info(
                        json.dumps(result, ensure_ascii=False, default=repr)
                    )
            else:
                raise ValueError("Invalid entrypoint: %s" % entrypoint)
        else:
            raise ValueError("Invalid entrypoint: %s" % entrypoint)
    except Exception:
        stdout_logger.error(
            f"[ERROR] Job fail. start_at: {start_at}, pid: {pid}, error: {traceback.format_exc()}"
        )
    finally:
        stdout_logger.debug(f"[INFO] Job end. start_at: {start_at}, pid: {pid}")
        if kill_timer:
            kill_timer.cancel()
        pid_file.write_text("")


if __name__ == "__main__":
    main()
