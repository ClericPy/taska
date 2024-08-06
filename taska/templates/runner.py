import json
import logging
import os
import re
import signal
import sys
import time
import traceback
import typing
from concurrent.futures import Future
from functools import partial
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Thread, Timer


class LoggerStream:
    def __init__(self, logger):
        self.logger = logger
        self.linebuf = ""
        self.newline = True

    @classmethod
    def setup(cls, std_type: str, dir_path: Path, stdout_limit):
        logger = logging.getLogger(f"{std_type}_log")
        logger.setLevel(logging.DEBUG)
        if not logger.hasHandlers():
            handler = RotatingFileHandler(
                dir_path.joinpath(f"{std_type}.log").resolve().as_posix(),
                maxBytes=stdout_limit * 1.1,
                backupCount=1,
                encoding="utf-8",
                errors="replace",
            )
            handler.terminator = ""
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        setattr(sys, std_type, LoggerStream(logger))

    def write(self, buf):
        if self.newline:
            buf = f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {buf}'
        if buf.endswith("\n"):
            self.newline = True
        else:
            self.newline = False
        self.logger.info(buf)

    def flush(self):
        pass


class SingletonError(RuntimeError):
    pass


class MaxWorkersError(RuntimeError):
    pass


class KillError(RuntimeError):
    pass


def handle_signal(sig, frame, future: typing.Optional[Future] = None):
    print(f"[ERROR] Got sig: {sig}, pid: {os.getpid}", flush=True, file=sys.stderr)
    if future:
        future.set_exception(KillError("killed-%s" % sig))


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


def is_running(pid: int):
    if sys.platform == "win32":
        with os.popen('tasklist /fo csv /fi "pid eq %s"' % pid) as f:
            f.readline()
            text = f.readline().strip()
            if text:
                return pid
    else:
        try:
            os.kill(pid, 0)
            return pid
        except OSError:
            pass
        except SystemError:
            return pid


def ensure_singleton(current_pid, pid_file: Path):
    if not pid_file.is_file():
        return
    old_pid = pid_file.read_text()
    if not old_pid:
        return
    if is_running(int(old_pid)):
        raise SingletonError(f"Job already running, running_pid: {old_pid}")


def log_result(result_limit, result_item: dict, start_ts):
    result_item["end_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    result_item["duration"] = round(time.time() - start_ts, 3)
    result_logger = logging.getLogger("result_logger")
    result_logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(
        Path(os.getcwd()).joinpath("result.jsonl").resolve().as_posix(),
        maxBytes=result_limit * 1.1,
        backupCount=1,
        encoding="utf-8",
        errors="replace",
    )
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    result_logger.addHandler(handler)
    result_logger.info(json.dumps(result_item, ensure_ascii=False, default=repr))
    handler.flush()
    result_logger.removeHandler(handler)


def setup_stdout_logger(cwd_path, stdout_limit):
    LoggerStream.setup("stdout", cwd_path, stdout_limit)
    LoggerStream.setup("stderr", cwd_path, stdout_limit)


def setup_mem_limit(mem_limit: str):
    if mem_limit:
        if sys.platform == "win32":
            raise ValueError(f"mem_limit={mem_limit} is not supported on windows")
        mem_limit = read_size(mem_limit)
        if mem_limit:
            import resource

            resource.setrlimit(resource.RLIMIT_RSS, (mem_limit, mem_limit))


def start_job(entrypoint, params, workspace_dir, EXEC_GLOBAL_FUTURE: Future):
    pattern = r"^\w+(\.\w+)?(:\w+)?$"
    if re.match(pattern, entrypoint):
        module, _, function = entrypoint.partition(":")
        if module:
            # main may be: 'module.py:main' or 'module.main' or 'package.module:main'
            # replace module.py to module
            sys.path.insert(0, workspace_dir.as_posix())
            module_path = workspace_dir / module
            if module_path.is_file():
                module = module_path.stem
            code = f"import {module}"
            if function:
                code += f"; EXEC_GLOBAL_FUTURE.set_result({module}.{function}(**RUNNER_PARAMS))"
            else:
                code += "; EXEC_GLOBAL_FUTURE.set_result('no result')"
            try:
                exec(
                    code,
                    {
                        "EXEC_GLOBAL_FUTURE": EXEC_GLOBAL_FUTURE,
                        "RUNNER_PARAMS": params,
                    },
                )
                if not EXEC_GLOBAL_FUTURE.done():
                    EXEC_GLOBAL_FUTURE.set_exception(
                        RuntimeError("Unknown Error, but no result(not None)")
                    )
            except Exception as e:
                if not EXEC_GLOBAL_FUTURE.done():
                    EXEC_GLOBAL_FUTURE.set_exception(e)

        else:
            EXEC_GLOBAL_FUTURE.set_exception(
                ValueError("Invalid entrypoint: %s" % entrypoint)
            )
    else:
        EXEC_GLOBAL_FUTURE.set_exception(
            ValueError("Invalid entrypoint: %s" % entrypoint)
        )


def ensure_max_workers(root_dir: Path):
    max_workers = int(root_dir.joinpath("max_workers").read_text())
    if max_workers > 0:
        runnings = 0
        for path in root_dir.joinpath("pids").iterdir():
            pid = int(path.name)
            if is_running(pid):
                runnings += 1
        if runnings >= max_workers:
            raise MaxWorkersError(f"Runnings: {runnings}/{max_workers}")


def main():
    """job_meta:
    {
        "entrypoint": "",
        "params": {},
        "enable": 0,
        "crontab": "",
        "timeout": 0,
        "mem_limit": "",
        "result_limit": "",
        "stdout_limit": ""
    }"""
    cwd_path = Path(os.getcwd()).resolve()
    workspace_dir = cwd_path.parent.parent
    root_dir = workspace_dir.parent.parent.parent.parent
    meta = json.loads(cwd_path.joinpath("meta.json").read_text(encoding="utf-8"))
    default_log_size = 5 * 1024**2
    result_limit = read_size(meta["result_limit"] or default_log_size)
    stdout_limit = read_size(meta["stdout_limit"] or default_log_size)
    start_at = time.strftime("%Y-%m-%d %H:%M:%S")
    start_ts = time.time()
    pid = os.getpid()
    result_item = {
        "start_at": start_at,
        "end_at": None,
        "duration": None,
        "pid": pid,
        "result": None,
        "error": None,
    }
    pid_str = str(pid)
    pid_file = cwd_path / "pid.txt"
    global_pid_file = root_dir / "pids" / pid_str
    global_pid_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        thread = None
        ensure_singleton(pid_str, pid_file)
        ensure_max_workers(root_dir)
        global_pid_file.touch()
        # start job
        pid_file.write_text(pid_str)
        cwd_path.joinpath("result.jsonl").touch()
        setup_stdout_logger(cwd_path, stdout_limit)
        EXEC_GLOBAL_FUTURE: Future = Future()
        print(f"[INFO] Job start. pid: {pid_str}", flush=True, file=sys.stderr)
        signal.signal(signal.SIGINT, partial(handle_signal, future=EXEC_GLOBAL_FUTURE))
        signal.signal(signal.SIGTERM, partial(handle_signal, future=EXEC_GLOBAL_FUTURE))
        setup_mem_limit(meta["mem_limit"])
        thread = Thread(
            target=start_job,
            args=(
                meta["entrypoint"],
                meta["params"],
                workspace_dir,
                EXEC_GLOBAL_FUTURE,
            ),
            daemon=True,
        )
        thread.start()

        try:
            timeout = meta.get("timeout")
            if not timeout and isinstance(timeout, int):
                timeout = None
            result_item["result"] = EXEC_GLOBAL_FUTURE.result(timeout=timeout)
        except TimeoutError:
            e = TimeoutError(f"timeout={timeout}")
            if not EXEC_GLOBAL_FUTURE.done():
                EXEC_GLOBAL_FUTURE.set_exception(e)
            raise e
    except Exception as e:
        print(
            f"[ERROR] Job fail. pid: {pid_str}, start_at: {start_at}, error: {traceback.format_exc()}",
            flush=True,
            file=sys.stderr,
        )
        result_item["error"] = repr(e)
    finally:
        log_result(result_limit, result_item, start_ts)
        print(
            f"[INFO] Job end. pid: {pid_str}, start_at: {start_at}",
            flush=True,
            file=sys.stderr,
        )
        if pid_file.is_file() and pid_file.read_text() == pid_str:
            pid_file.unlink(missing_ok=True)
        global_pid_file.unlink(missing_ok=True)
        if thread and thread.is_alive():
            timer = Timer(1, lambda: os._exit(1))
            timer.daemon = True
            timer.start()


if __name__ == "__main__":
    main()
